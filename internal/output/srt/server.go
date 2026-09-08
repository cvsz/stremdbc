package srt

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

// Server represents an SRT output server
type Server struct {
	config    *config.SRTOutputConfig
	registry  *core.StreamRegistry
	logger    *zap.Logger
	listener  *net.UDPConn
	wg        sync.WaitGroup
	mu        sync.Mutex
	running   bool
	sessions  map[string]*Session
}

// Session represents an SRT output session
type Session struct {
	ID         string
	StreamID   string
	Addr       *net.UDPAddr
	State      string
	CreatedAt  time.Time
	BytesSent  int64
}

// NewServer creates a new SRT output server
func NewServer(cfg *config.SRTOutputConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	return &Server{
		config:   cfg,
		registry: registry,
		logger:   logger.Named("srt-output"),
		sessions: make(map[string]*Session),
	}
}

// Start starts the SRT output server
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address %s: %w", addr, err)
	}

	listener, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener
	s.logger.Info("SRT output server started", zap.String("address", addr))

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop(ctx)
	}()

	return nil
}

// Stop stops the SRT output server
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	s.logger.Info("stopping SRT output server")

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
	buf := make([]byte, 1472)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, addr, err := s.listener.ReadFromUDP(buf)
			if err != nil {
				if s.isClosedError(err) {
					return
				}
				s.logger.Error("failed to read from UDP", zap.Error(err))
				continue
			}

			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.handlePacket(ctx, buf[:n], addr)
			}()
		}
	}
}

func (s *Server) handlePacket(ctx context.Context, data []byte, addr *net.UDPAddr) {
	streamID := s.extractStreamID(data)
	if streamID == "" {
		streamID = fmt.Sprintf("stream_%d", time.Now().UnixNano())
	}

	s.mu.Lock()
	if _, exists := s.sessions[streamID]; !exists {
		s.sessions[streamID] = &Session{
			ID:        streamID,
			Addr:      addr,
			State:     "active",
			CreatedAt: time.Now(),
			BytesSent: int64(len(data)),
		}
		s.logger.Info("new SRT output stream",
			zap.String("stream_id", streamID),
			zap.String("remote", addr.String()),
		)
	} else {
		s.sessions[streamID].BytesSent += int64(len(data))
	}
	s.mu.Unlock()

	// Send acknowledgment
	ack := make([]byte, 16)
	copy(ack, data[:16])
	_, _ = s.listener.WriteToUDP(ack, addr)
}

func (s *Server) extractStreamID(data []byte) string {
	if len(data) > 32 {
		str := string(data)
		idx := strings.Index(str, "streamid=")
		if idx >= 0 {
			start := idx + 9
			end := strings.IndexAny(str[start:], "&\x00\r\n")
			if end > 0 {
				return str[start : start+end]
			}
			return str[start:]
		}
	}
	return ""
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

// RemoveSession removes a session
func (s *Server) RemoveSession(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
}
