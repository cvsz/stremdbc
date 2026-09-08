package rtmp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

// Server represents an RTMP output server
type Server struct {
	config    *config.RTMPOutputConfig
	registry  *core.StreamRegistry
	logger    *zap.Logger
	listener  net.Listener
	wg        sync.WaitGroup
	mu        sync.Mutex
	running   bool
	sessions  map[string]*Session
}

// Session represents an RTMP output session
type Session struct {
	ID          string
	StreamID    string
	Conn        net.Conn
	State       string
	CreatedAt   time.Time
	BytesSent   int64
	RemoteAddr  string
}

// NewServer creates a new RTMP output server
func NewServer(cfg *config.RTMPOutputConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	return &Server{
		config:   cfg,
		registry: registry,
		logger:   logger.Named("rtmp-output"),
		sessions: make(map[string]*Session),
	}
}

// Start starts the RTMP output server
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener
	s.logger.Info("RTMP output server started", zap.String("address", addr))

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop(ctx)
	}()

	return nil
}

// Stop stops the RTMP output server
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	s.logger.Info("stopping RTMP output server")

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Error("failed to close listener", zap.Error(err))
		}
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout waiting for connections to close")
	}
}

func (s *Server) acceptLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				if s.isClosedError(err) {
					return
				}
				s.logger.Error("failed to accept connection", zap.Error(err))
				continue
			}

			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.handleConnection(ctx, conn)
			}()
		}
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	// RTMP handshake would go here
	// For now, simplified implementation
	
	sessionID := generateSessionID()
	session := &Session{
		ID:         sessionID,
		State:      "connected",
		Conn:       conn,
		CreatedAt:  time.Now(),
		RemoteAddr: conn.RemoteAddr().String(),
	}

	s.mu.Lock()
	s.sessions[sessionID] = session
	s.mu.Unlock()

	s.logger.Info("RTMP output client connected",
		zap.String("session_id", sessionID),
		zap.String("remote", conn.RemoteAddr().String()),
	)

	// Keep connection alive
	buf := make([]byte, 4096)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, err := conn.Read(buf)
			if err != nil {
				s.removeSession(sessionID)
				return
			}
			session.BytesSent += int64(n)
		}
	}
}

func (s *Server) removeSession(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
}

func (s *Server) isClosedError(err error) bool {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return false
	}
	return true
}

// GetSessionCount returns the number of active sessions
func (s *Server) GetSessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

// GetSession returns a session by ID
func (s *Server) GetSession(id string) (*Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, exists := s.sessions[id]
	return session, exists
}

func generateSessionID() string {
	return fmt.Sprintf("rtmp_%d", time.Now().UnixNano())
}
