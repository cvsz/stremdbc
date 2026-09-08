package rtsp

import (
	"bufio"
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

// Server represents an RTSP server
type Server struct {
	config   *config.RTSPConfig
	registry *core.StreamRegistry
	logger   *zap.Logger
	listener net.Listener
	wg       sync.WaitGroup
	mu       sync.Mutex
	running  bool
	sessions map[string]*Session
}

// Session represents an RTSP session
type Session struct {
	ID        string
	StreamID  string
	Conn      net.Conn
	Seq       int
	State     string
	CreatedAt time.Time
}

// NewServer creates a new RTSP server
func NewServer(cfg *config.RTSPConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	return &Server{
		config:   cfg,
		registry: registry,
		logger:   logger.Named("rtsp"),
		sessions: make(map[string]*Session),
	}
}

// Start starts the RTSP server
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
	s.logger.Info("RTSP server started", zap.String("address", addr))

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop(ctx)
	}()

	return nil
}

// Stop stops the RTSP server
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	s.logger.Info("stopping RTSP server")

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
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					continue
				}
				if strings.Contains(err.Error(), "use of closed network connection") {
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

	remoteAddr := conn.RemoteAddr().(*net.TCPAddr)
	s.logger.Debug("new RTSP connection", zap.String("remote", remoteAddr.String()))

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	sessionID := ""
	seq := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}

			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			seq++
			response := s.handleRequest(line, sessionID, seq)
			
			if strings.HasPrefix(line, "SETUP") {
				sessionID = fmt.Sprintf("%d", time.Now().UnixNano())
				s.sessions[sessionID] = &Session{
					ID:        sessionID,
					StreamID:  s.extractStreamID(line),
					Conn:      conn,
					Seq:       seq,
					State:     "active",
					CreatedAt: time.Now(),
				}
			}

			writer.WriteString(response + "\r\n\r\n")
			writer.Flush()

			if strings.HasPrefix(line, "TEARDOWN") {
				if sessionID != "" {
					delete(s.sessions, sessionID)
				}
				return
			}
		}
	}
}

func (s *Server) handleRequest(line, sessionID string, seq int) string {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 2 {
		return "RTSP/1.0 400 Bad Request"
	}

	method := parts[0]

	switch method {
	case "OPTIONS":
		return fmt.Sprintf("RTSP/1.0 200 OK\r\nCSeq: %d\r\nPublic: OPTIONS, DESCRIBE, SETUP, TEARDOWN, PLAY, PAUSE", seq)
	case "DESCRIBE":
		return fmt.Sprintf("RTSP/1.0 200 OK\r\nCSeq: %d\r\nContent-Type: application/sdp\r\nContent-Length: 0", seq)
	case "SETUP":
		return fmt.Sprintf("RTSP/1.0 200 OK\r\nCSeq: %d\r\nSession: %s\r\nTransport: RTP/AVP;unicast;client_port=3056-3057;server_port=3058-3059", seq, sessionID)
	case "PLAY":
		return fmt.Sprintf("RTSP/1.0 200 OK\r\nCSeq: %d\r\nSession: %s\r\nRange: npt=0.000-", seq, sessionID)
	case "PAUSE":
		return fmt.Sprintf("RTSP/1.0 200 OK\r\nCSeq: %d\r\nSession: %s", seq, sessionID)
	case "TEARDOWN":
		return fmt.Sprintf("RTSP/1.0 200 OK\r\nCSeq: %d\r\nSession: %s", seq, sessionID)
	default:
		return fmt.Sprintf("RTSP/1.0 501 Not Implemented\r\nCSeq: %d", seq)
	}
}

func (s *Server) extractStreamID(line string) string {
	parts := strings.Split(line, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

// GetSessionCount returns the number of active sessions
func (s *Server) GetSessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}
