package rtmp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

// Server represents an RTMP output control endpoint. It tracks connected
// consumers, but does not claim outbound media until a media graph is wired.
type Server struct {
	config   *config.RTMPOutputConfig
	registry *core.StreamRegistry
	logger   *zap.Logger

	mu          sync.Mutex
	lifecycle   sync.Mutex
	listener    net.Listener
	running     bool
	sessions    map[string]*Session
	connections map[net.Conn]struct{}
	acceptWG    sync.WaitGroup
	connWG      sync.WaitGroup
}

// Session represents an RTMP output session and is returned as a snapshot.
type Session struct {
	ID         string
	StreamID   string
	Conn       net.Conn
	State      string
	CreatedAt  time.Time
	BytesSent  int64
	RemoteAddr string
}

func NewServer(cfg *config.RTMPOutputConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	if cfg == nil {
		copyCfg := config.DefaultConfig().RTMPOutput
		cfg = &copyCfg
	}
	if registry == nil {
		registry = core.NewStreamRegistry(nil)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	copyCfg := *cfg
	return &Server{config: &copyCfg, registry: registry, logger: logger.Named("rtmp-output"), sessions: make(map[string]*Session), connections: make(map[net.Conn]struct{})}
}

func (s *Server) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.mu.Unlock()
	addr := net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.Port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	if err := ctx.Err(); err != nil {
		_ = listener.Close()
		return err
	}
	if tcp, ok := listener.(*net.TCPListener); ok {
		_ = tcp.SetDeadline(time.Now().Add(500 * time.Millisecond))
	}
	s.mu.Lock()
	s.listener = listener
	s.running = true
	s.mu.Unlock()
	s.logger.Info("RTMP output control endpoint started", zap.String("address", listener.Addr().String()))
	s.acceptWG.Add(1)
	go func() {
		defer s.acceptWG.Done()
		s.acceptLoop(ctx, listener)
	}()
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	s.mu.Lock()
	if !s.running && s.listener == nil && len(s.connections) == 0 {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	listener := s.listener
	s.listener = nil
	connections := make([]net.Conn, 0, len(s.connections))
	for conn := range s.connections {
		connections = append(connections, conn)
	}
	s.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	for _, conn := range connections {
		_ = conn.Close()
	}
	acceptDone := make(chan struct{})
	go func() { s.acceptWG.Wait(); close(acceptDone) }()
	select {
	case <-acceptDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	connDone := make(chan struct{})
	go func() { s.connWG.Wait(); close(connDone) }()
	select {
	case <-connDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) acceptLoop(ctx context.Context, listener net.Listener) {
	defer func() {
		_ = listener.Close()
		s.mu.Lock()
		if s.listener == listener {
			s.running = false
			s.listener = nil
		}
		s.mu.Unlock()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if tcp, ok := listener.(*net.TCPListener); ok {
					_ = tcp.SetDeadline(time.Now().Add(500 * time.Millisecond))
				}
				continue
			}
			s.logger.Error("failed to accept connection", zap.Error(err))
			return
		}
		s.mu.Lock()
		if !s.running {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.connections[conn] = struct{}{}
		s.connWG.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.connWG.Done()
			defer s.removeConnection(conn)
			connectionCtx, cancel := context.WithCancel(ctx)
			connectionDone := make(chan struct{})
			go func() {
				select {
				case <-connectionCtx.Done():
					_ = conn.Close()
				case <-connectionDone:
				}
			}()
			s.handleConnection(connectionCtx, conn)
			close(connectionDone)
			cancel()
		}()
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	remote := "unknown"
	if conn.RemoteAddr() != nil {
		remote = conn.RemoteAddr().String()
	}
	sessionID := nextSessionID()
	session := &Session{ID: sessionID, State: "connected", Conn: conn, CreatedAt: time.Now().UTC(), RemoteAddr: remote}
	s.mu.Lock()
	s.sessions[sessionID] = session
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.sessions, sessionID)
		s.mu.Unlock()
	}()
	s.logger.Info("RTMP output client connected", zap.String("session_id", sessionID), zap.String("remote", remote))
	buf := make([]byte, 32*1024)
	for {
		if ctx.Err() != nil {
			return
		}
		if s.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		}
		_, err := conn.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return
			}
			return
		}
		// Inbound bytes are control/client data and must not be reported as
		// BytesSent. Actual outbound accounting belongs to a media writer.
	}
}

func (s *Server) removeConnection(conn net.Conn) {
	s.mu.Lock()
	delete(s.connections, conn)
	s.mu.Unlock()
}

func (s *Server) GetSessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

func (s *Server) GetSession(id string) (*Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, exists := s.sessions[id]
	if !exists {
		return nil, false
	}
	copy := *session
	copy.Conn = nil
	return &copy, true
}

var sessionCounter uint64

func nextSessionID() string {
	return fmt.Sprintf("rtmp_%d", atomic.AddUint64(&sessionCounter, 1))
}
