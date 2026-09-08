package rtmp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

// Server represents an RTMP server
type Server struct {
	config   *config.RTMPConfig
	registry *core.StreamRegistry
	logger   *zap.Logger
	listener net.Listener
	wg       sync.WaitGroup
	mu       sync.Mutex
	running  bool
}

// NewServer creates a new RTMP server
func NewServer(cfg *config.RTMPConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	return &Server{
		config:   cfg,
		registry: registry,
		logger:   logger.Named("rtmp"),
	}
}

// Start starts the RTMP server
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
	s.logger.Info("RTMP server started", zap.String("address", addr))

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop(ctx)
	}()

	return nil
}

// Stop stops the RTMP server
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	s.logger.Info("stopping RTMP server")

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Error("failed to close listener", zap.Error(err))
		}
	}

	// Wait for all connections to close with timeout
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
	s.logger.Debug("new connection", zap.String("remote", remoteAddr.String()))

	// Set deadlines
	conn.SetDeadline(time.Now().Add(s.config.ReadTimeout))

	// Create connection handler
	handler := newConnectionHandler(conn, remoteAddr, s.registry, s.logger)
	if err := handler.run(ctx); err != nil {
		s.logger.Error("connection error",
			zap.String("remote", remoteAddr.String()),
			zap.Error(err),
		)
	}
}

// connectionHandler handles a single RTMP connection
type connectionHandler struct {
	conn     net.Conn
	remote   *net.TCPAddr
	registry *core.StreamRegistry
	logger   *zap.Logger
	reader   *bufio.Reader
	writer   *bufio.Writer
}

func newConnectionHandler(conn net.Conn, remote *net.TCPAddr, registry *core.StreamRegistry, logger *zap.Logger) *connectionHandler {
	return &connectionHandler{
		conn:     conn,
		remote:   remote,
		registry: registry,
		logger:   logger.With(zap.String("remote", remote.String())),
		reader:   bufio.NewReader(conn),
		writer:   bufio.NewWriter(conn),
	}
}

func (h *connectionHandler) run(ctx context.Context) error {
	// RTMP handshake
	if err := h.handshake(); err != nil {
		return fmt.Errorf("handshake failed: %w", err)
	}

	h.logger.Debug("handshake completed")

	// Read RTMP messages
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := h.readMessage()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("read message: %w", err)
			}

			if err := h.processMessage(msg); err != nil {
				h.logger.Error("process message error", zap.Error(err))
			}
		}
	}
}

// handshake performs RTMP handshake (simplified for Phase 1)
func (h *connectionHandler) handshake() error {
	// Read client handshake (1537 bytes)
	buf := make([]byte, 1537)
	if _, err := io.ReadFull(h.reader, buf); err != nil {
		return err
	}

	// Send server handshake (same data for simple handshake)
	if _, err := h.writer.Write(buf); err != nil {
		return err
	}

	return h.writer.Flush()
}

// readMessage reads an RTMP message (simplified)
func (h *connectionHandler) readMessage() ([]byte, error) {
	// Basic header (3-12 bytes)
	header := make([]byte, 12)
	n, err := h.reader.Read(header)
	if err != nil {
		return nil, err
	}

	if n < 12 {
		return nil, io.ErrUnexpectedEOF
	}

	// Parse basic header to get message length
	// This is a simplified implementation
	timestamp := uint32(header[4])<<24 | uint32(header[5])<<16 | uint32(header[6])<<8 | uint32(header[7])
	msgLength := uint32(header[8])<<16 | uint32(header[9])<<8 | uint32(header[10])
	msgType := header[11]

	h.logger.Debug("received message",
		zap.Uint32("timestamp", timestamp),
		zap.Uint32("length", msgLength),
		zap.Uint8("type", msgType),
	)

	// Read message payload
	payload := make([]byte, msgLength)
	if msgLength > 0 {
		if _, err := io.ReadFull(h.reader, payload); err != nil {
			return nil, err
		}
	}

	// Read 4-byte footer (timestamp)
	footer := make([]byte, 4)
	if _, err := io.ReadFull(h.reader, footer); err != nil {
		return nil, err
	}

	return append(header, append(payload, footer...)...), nil
}

// processMessage processes an RTMP message
func (h *connectionHandler) processMessage(msg []byte) error {
	if len(msg) < 12 {
		return fmt.Errorf("message too short")
	}

	// For Phase 1, we'll log the message and acknowledge
	// Full RTMP protocol implementation will be added later
	h.logger.Debug("processing message", zap.Int("size", len(msg)))

	return nil
}

// extractStreamKey extracts stream key from RTMP publish command
// This will be fully implemented in Phase 2
func (h *connectionHandler) extractStreamKey(msg []byte) (string, error) {
	// Simplified extraction - full parsing in Phase 2
	return "default", nil
}
