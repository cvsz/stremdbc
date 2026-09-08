package srt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

const (
	maxOutputSessions = 4096
	outputSessionTTL  = 2 * time.Minute
)

// Server represents a bounded SRT output control endpoint. It records
// validated connection metadata but does not acknowledge inbound datagrams or
// report them as outbound media without a libsrt/media writer.
type Server struct {
	config   *config.SRTOutputConfig
	registry *core.StreamRegistry
	logger   *zap.Logger

	mu        sync.Mutex
	lifecycle sync.Mutex
	listener  *net.UDPConn
	running   bool
	stopping  bool
	sessions  map[string]*Session
	acceptWG  sync.WaitGroup
}

type Session struct {
	ID        string
	StreamID  string
	Addr      *net.UDPAddr
	State     string
	CreatedAt time.Time
	LastSeen  time.Time
	BytesSent int64
}

func NewServer(cfg *config.SRTOutputConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	if cfg == nil {
		copyCfg := config.DefaultConfig().SRTOutput
		cfg = &copyCfg
	}
	if registry == nil {
		registry = core.NewStreamRegistry(nil)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	copyCfg := *cfg
	return &Server{config: &copyCfg, registry: registry, logger: logger.Named("srt-output"), sessions: make(map[string]*Session)}
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
		return fmt.Errorf("SRT output server already running")
	}
	s.mu.Unlock()
	addr := net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.Port))
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address %s: %w", addr, err)
	}
	listener, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	if err := ctx.Err(); err != nil {
		_ = listener.Close()
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.running = true
	s.stopping = false
	s.mu.Unlock()
	s.logger.Info("SRT output control endpoint started", zap.String("address", listener.LocalAddr().String()))
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
	if !s.running && s.listener == nil {
		s.stopping = true
		s.sessions = make(map[string]*Session)
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.stopping = true
	listener := s.listener
	s.listener = nil
	s.sessions = make(map[string]*Session)
	s.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	done := make(chan struct{})
	go func() { s.acceptWG.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) acceptLoop(ctx context.Context, listener *net.UDPConn) {
	defer func() {
		_ = listener.Close()
		s.mu.Lock()
		if s.listener == listener {
			s.running = false
			s.listener = nil
			s.sessions = make(map[string]*Session)
		}
		s.mu.Unlock()
	}()
	buf := make([]byte, 64*1024)
	for {
		_ = listener.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, addr, err := listener.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			s.logger.Error("failed to read UDP packet", zap.Error(err))
			return
		}
		packet := append([]byte(nil), buf[:n]...)
		s.handlePacket(packet, addr)
	}
}

func (s *Server) handlePacket(data []byte, addr *net.UDPAddr) {
	if addr == nil {
		return
	}
	streamID := s.extractStreamID(data)
	if streamID == "" {
		return
	}
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	for id, candidate := range s.sessions {
		if !candidate.LastSeen.IsZero() && now.Sub(candidate.LastSeen) > outputSessionTTL {
			delete(s.sessions, id)
		}
	}
	session, exists := s.sessions[streamID]
	if !exists {
		if len(s.sessions) >= maxOutputSessions {
			s.mu.Unlock()
			return
		}
		session = &Session{ID: nextSessionID(), StreamID: streamID, Addr: cloneUDPAddr(addr), State: "metadata", CreatedAt: now, LastSeen: now}
		s.sessions[streamID] = session
	}
	session.Addr = cloneUDPAddr(addr)
	session.LastSeen = now
	// No bytes are counted as sent: this endpoint has no outbound media writer.
	s.mu.Unlock()
}

func (s *Server) extractStreamID(data []byte) string {
	value := string(data)
	if !strings.HasPrefix(value, "streamid=") {
		return ""
	}
	remainder := value[len("streamid="):]
	end := strings.IndexAny(remainder, "&\x00\r\n")
	if end < 0 {
		end = len(remainder)
	}
	if end == 0 {
		return ""
	}
	decoded, err := url.QueryUnescape(remainder[:end])
	if err != nil || core.ValidateStreamID(decoded) != nil {
		return ""
	}
	return decoded
}

func (s *Server) GetSessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

func (s *Server) GetSession(id string) (*Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, session, exists := s.findSessionLocked(id)
	if !exists {
		return nil, false
	}
	copy := *session
	copy.Addr = cloneUDPAddr(session.Addr)
	return &copy, true
}

func (s *Server) RemoveSession(id string) {
	s.mu.Lock()
	key, _, exists := s.findSessionLocked(id)
	if exists {
		delete(s.sessions, key)
	}
	s.mu.Unlock()
}

func (s *Server) findSessionLocked(identifier string) (string, *Session, bool) {
	if session, exists := s.sessions[identifier]; exists {
		return identifier, session, true
	}
	for key, session := range s.sessions {
		if session.ID == identifier {
			return key, session, true
		}
	}
	return "", nil, false
}

var sessionCounter uint64

func nextSessionID() string { return fmt.Sprintf("srt_%d", atomic.AddUint64(&sessionCounter, 1)) }

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	copy := *addr
	copy.IP = append(net.IP(nil), addr.IP...)
	return &copy
}
