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

// Server represents an SRT server
type Server struct {
	config   *config.SRTConfig
	registry *core.StreamRegistry
	logger   *zap.Logger
	listener net.Listener
	wg       sync.WaitGroup
	mu       sync.Mutex
	running  bool
	streams  map[string]*StreamConnection
}

// StreamConnection represents an SRT stream connection
type StreamConnection struct {
	StreamID  string
	Conn      net.Conn
	State     string
	CreatedAt time.Time
	BytesRead int64
}

// NewServer creates a new SRT server
func NewServer(cfg *config.SRTConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	return &Server{
		config:   cfg,
		registry: registry,
		logger:   logger.Named("srt"),
		streams:  make(map[string]*StreamConnection),
	}
}

// Start starts the SRT server
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	listener, err := net.Listen("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener
	s.logger.Info("SRT server started", zap.String("address", addr))

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop(ctx)
	}()

	return nil
}

// Stop stops the SRT server
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	s.logger.Info("stopping SRT server")

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
	buf := make([]byte, 1472) // SRT typical MTU

	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, addr, err := s.listener.ReadFromUDP(buf)
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
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
	// Simplified SRT handshake handling
	// Full SRT protocol implementation would use libsrt
	
	if len(data) < 16 {
		return
	}

	// Extract stream ID from packet (simplified)
	streamID := s.extractStreamID(data)
	if streamID == "" {
		streamID = fmt.Sprintf("stream_%d", time.Now().UnixNano())
	}

	s.mu.Lock()
	if _, exists := s.streams[streamID]; !exists {
		s.streams[streamID] = &StreamConnection{
			StreamID:  streamID,
			State:     "active",
			CreatedAt: time.Now(),
			BytesRead: int64(len(data)),
		}
		
		s.logger.Info("new SRT stream",
			zap.String("stream_id", streamID),
			zap.String("remote", addr.String()),
		)
	} else {
		s.streams[streamID].BytesRead += int64(len(data))
	}
	s.mu.Unlock()

	// Send acknowledgment (simplified)
	ack := make([]byte, 16)
	copy(ack, data[:16])
	_, _ = s.listener.WriteToUDP(ack, addr)
}

func (s *Server) extractStreamID(data []byte) string {
	// Extract streamid from SRT handshake packet
	// This is a simplified implementation
	if len(data) > 32 {
		// Look for streamid= in the packet
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

// GetStreamCount returns the number of active streams
func (s *Server) GetStreamCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

// GetStream returns a stream by ID
func (s *Server) GetStream(id string) (*StreamConnection, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, exists := s.streams[id]
	return stream, exists
}

// RemoveStream removes a stream
func (s *Server) RemoveStream(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.streams, id)
}
