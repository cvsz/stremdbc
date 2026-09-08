package rtsp

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

// Server represents an RTSP output server
type Server struct {
	config    *config.RTSPOutputConfig
	registry  *core.StreamRegistry
	logger    *zap.Logger
	listener  net.Listener
	wg        sync.WaitGroup
	mu        sync.Mutex
	running   bool
	sessions  map[string]*Session
}

// Session represents an RTSP output session
type Session struct {
	ID         string
	StreamID   string
	Conn       net.Conn
	State      string
	CreatedAt  time.Time
	RemoteAddr string
}

// NewServer creates a new RTSP output server
func NewServer(cfg *config.RTSPOutputConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	return &Server{
		config:   cfg,
		registry: registry,
		logger:   logger.Named("rtsp-output"),
		sessions: make(map[string]*Session),
	}
}

// Start starts the RTSP output server
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
	s.logger.Info("RTSP output server started", zap.String("address", addr))

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop(ctx)
	}()

	return nil
}

// Stop stops the RTSP output server
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	s.logger.Info("stopping RTSP output server")

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

	s.logger.Info("RTSP output client connected",
		zap.String("session_id", sessionID),
		zap.String("remote", conn.RemoteAddr().String()),
	)

	// Handle RTSP commands (DESCRIBE, SETUP, PLAY, TEARDOWN)
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
			s.handleRTSPCommand(conn, buf[:n], session)
		}
	}
}

func (s *Server) handleRTSPCommand(conn net.Conn, data []byte, session *Session) {
	// Simplified RTSP command handling
	// Full implementation would parse RTSP messages and stream RTP packets
	cmd := string(data)
	if len(cmd) > 50 {
		cmd = cmd[:50]
	}
	s.logger.Debug("RTSP command received", zap.String("cmd", cmd))
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
	return fmt.Sprintf("rtsp_%d", time.Now().UnixNano())
}
