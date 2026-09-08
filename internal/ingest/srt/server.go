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
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

const (
	maxMetadataStreams = 4096
	metadataTTL        = 2 * time.Minute
)

// Server represents a bounded SRT control/telemetry endpoint. The repository
// does not include a libsrt engine, so it does not acknowledge or forward
// arbitrary UDP payloads as if they were SRT media.
type Server struct {
	config   *config.SRTConfig
	registry *core.StreamRegistry
	logger   *zap.Logger

	mu        sync.Mutex
	lifecycle sync.Mutex
	listener  *net.UDPConn
	running   bool
	stopping  bool
	streams   map[string]*StreamConnection
	acceptWG  sync.WaitGroup
}

// StreamConnection represents validated stream metadata observed by the
// endpoint and is returned as a snapshot.
type StreamConnection struct {
	StreamID  string
	Conn      net.Conn
	State     string
	CreatedAt time.Time
	LastSeen  time.Time
	BytesRead int64
	Addr      *net.UDPAddr
}

func NewServer(cfg *config.SRTConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	if cfg == nil {
		copyCfg := config.DefaultConfig().SRT
		cfg = &copyCfg
	}
	if registry == nil {
		registry = core.NewStreamRegistry(nil)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	copyCfg := *cfg
	return &Server{config: &copyCfg, registry: registry, logger: logger.Named("srt"), streams: make(map[string]*StreamConnection)}
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
	s.logger.Info("SRT control endpoint started", zap.String("address", listener.LocalAddr().String()))
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
		s.streams = make(map[string]*StreamConnection)
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.stopping = true
	listener := s.listener
	s.listener = nil
	s.streams = make(map[string]*StreamConnection)
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
			s.streams = make(map[string]*StreamConnection)
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
		s.handlePacket(ctx, packet, addr)
	}
}

func (s *Server) handlePacket(ctx context.Context, data []byte, addr *net.UDPAddr) {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
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
	for id, candidate := range s.streams {
		if !candidate.LastSeen.IsZero() && now.Sub(candidate.LastSeen) > metadataTTL {
			delete(s.streams, id)
		}
	}
	stream, exists := s.streams[streamID]
	if !exists {
		if len(s.streams) >= maxMetadataStreams {
			s.mu.Unlock()
			return
		}
		stream = &StreamConnection{StreamID: streamID, State: "metadata", CreatedAt: now, LastSeen: now, Addr: cloneUDPAddr(addr)}
		s.streams[streamID] = stream
		s.logger.Info("validated SRT stream metadata", zap.String("stream_id", streamID), zap.String("remote", addr.String()))
	}
	stream.LastSeen = now
	stream.BytesRead += int64(len(data))
	stream.Addr = cloneUDPAddr(addr)
	s.mu.Unlock()
}

func (s *Server) extractStreamID(data []byte) string {
	value := string(data)
	if !strings.HasPrefix(value, "streamid=") {
		return ""
	}
	start := len("streamid=")
	remainder := value[start:]
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

func (s *Server) GetStreamCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *Server) GetStream(id string) (*StreamConnection, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, exists := s.streams[id]
	if !exists {
		return nil, false
	}
	copy := *stream
	copy.Conn = nil
	copy.Addr = cloneUDPAddr(stream.Addr)
	return &copy, true
}

func (s *Server) RemoveStream(id string) {
	s.mu.Lock()
	delete(s.streams, id)
	s.mu.Unlock()
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	copy := *addr
	copy.IP = append(net.IP(nil), addr.IP...)
	return &copy
}
