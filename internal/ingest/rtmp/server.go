package rtmp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
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

const (
	rtmpVersion       = 3
	rtmpHandshakeSize = 1536
	defaultChunkSize  = 128
	maxMessageSize    = 8 << 20
)

var (
	ErrStreamKeyMissing = errors.New("RTMP publish stream key is missing")
	ErrUnsupportedMedia = errors.New("RTMP media forwarding is not configured")
	ErrMessageTooLarge  = errors.New("RTMP message exceeds configured limit")
)

// Server represents an RTMP ingest server. It provides a standards-correct
// handshake and bounded control-message parser; media forwarding requires a
// codec/RTMP media engine and is rejected explicitly.
type Server struct {
	config   *config.RTMPConfig
	registry *core.StreamRegistry
	logger   *zap.Logger

	mu          sync.Mutex
	lifecycle   sync.Mutex
	listener    net.Listener
	running     bool
	connections map[net.Conn]struct{}
	acceptWG    sync.WaitGroup
	connWG      sync.WaitGroup
}

// NewServer creates a new RTMP server.
func NewServer(cfg *config.RTMPConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	if cfg == nil {
		copyCfg := config.DefaultConfig().RTMP
		cfg = &copyCfg
	}
	if registry == nil {
		registry = core.NewStreamRegistry(nil)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	copyCfg := *cfg
	return &Server{config: &copyCfg, registry: registry, logger: logger.Named("rtmp"), connections: make(map[net.Conn]struct{})}
}

// Start starts the RTMP server after its listener has successfully bound.
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

	addr := net.JoinHostPort(s.config.Host, fmt.Sprintf("%d", s.config.Port))
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
	s.logger.Info("RTMP server started", zap.String("address", listener.Addr().String()))

	s.acceptWG.Add(1)
	go func() {
		defer s.acceptWG.Done()
		s.acceptLoop(ctx, listener)
	}()
	return nil
}

// Stop stops the listener and all active connections.
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

	s.logger.Info("stopping RTMP server")
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
			if ctx.Err() != nil {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if tcp, ok := listener.(*net.TCPListener); ok {
					_ = tcp.SetDeadline(time.Now().Add(500 * time.Millisecond))
				}
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return
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
			defer func() {
				s.mu.Lock()
				delete(s.connections, conn)
				s.mu.Unlock()
			}()
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
	defer conn.Close()
	remote, _ := conn.RemoteAddr().(*net.TCPAddr)
	remoteText := "unknown"
	if conn.RemoteAddr() != nil {
		remoteText = conn.RemoteAddr().String()
	}
	s.logger.Debug("new connection", zap.String("remote", remoteText))
	if s.config.ReadTimeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(s.config.ReadTimeout))
	}
	if s.config.WriteTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	}
	handler := newConnectionHandler(conn, remote, s.registry, s.logger)
	handler.readTimeout = s.config.ReadTimeout
	if err := handler.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error("connection error", zap.String("remote", remoteText), zap.Error(err))
	}
}

type rtmpMessage struct {
	Timestamp   uint32
	Length      uint32
	MessageType byte
	StreamID    uint32
	Payload     []byte
}

type connectionHandler struct {
	conn        net.Conn
	remote      *net.TCPAddr
	registry    *core.StreamRegistry
	logger      *zap.Logger
	reader      *bufio.Reader
	writer      *bufio.Writer
	chunkSize   uint32
	readTimeout time.Duration
}

func newConnectionHandler(conn net.Conn, remote *net.TCPAddr, registry *core.StreamRegistry, logger *zap.Logger) *connectionHandler {
	if logger == nil {
		logger = zap.NewNop()
	}
	if registry == nil {
		registry = core.NewStreamRegistry(nil)
	}
	fields := make([]zap.Field, 0, 1)
	if remote != nil {
		fields = append(fields, zap.String("remote", remote.String()))
	}
	return &connectionHandler{
		conn:      conn,
		remote:    remote,
		registry:  registry,
		logger:    logger.With(fields...),
		reader:    bufio.NewReader(conn),
		writer:    bufio.NewWriter(conn),
		chunkSize: defaultChunkSize,
	}
}

func (h *connectionHandler) run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := h.handshake(); err != nil {
		return fmt.Errorf("handshake failed: %w", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if h.readTimeout > 0 {
			_ = h.conn.SetReadDeadline(time.Now().Add(h.readTimeout))
		}
		message, err := h.readMessage()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read message: %w", err)
		}
		if err := h.processMessage(message); err != nil {
			return err
		}
	}
}

// handshake implements RTMP C0/C1/S0/S1/S2/C2 exchange.
func (h *connectionHandler) handshake() error {
	var version [1]byte
	if _, err := io.ReadFull(h.reader, version[:]); err != nil {
		return err
	}
	if version[0] != rtmpVersion {
		return fmt.Errorf("unsupported RTMP version %d", version[0])
	}
	c1 := make([]byte, rtmpHandshakeSize)
	if _, err := io.ReadFull(h.reader, c1); err != nil {
		return err
	}
	s1 := make([]byte, rtmpHandshakeSize)
	binary.BigEndian.PutUint32(s1[:4], rtmpTimestamp())
	if _, err := rand.Read(s1[8:]); err != nil {
		return fmt.Errorf("generate handshake challenge: %w", err)
	}
	if err := h.writer.WriteByte(rtmpVersion); err != nil {
		return err
	}
	if _, err := h.writer.Write(s1); err != nil {
		return err
	}
	if _, err := h.writer.Write(c1); err != nil {
		return err
	}
	if err := h.writer.Flush(); err != nil {
		return err
	}
	c2 := make([]byte, rtmpHandshakeSize)
	if _, err := io.ReadFull(h.reader, c2); err != nil {
		return err
	}
	if !bytes.Equal(c2, s1) {
		return fmt.Errorf("RTMP C2 does not echo S1")
	}
	return nil
}

func rtmpTimestamp() uint32 {
	seconds := time.Now().Unix()
	if seconds < 0 {
		return 0
	}
	const modulus = int64(1 << 32)
	seconds %= modulus
	// #nosec G115 -- the explicit modulo above bounds the RTMP uint32 timestamp.
	return uint32(seconds)
}

func (h *connectionHandler) readMessage() (*rtmpMessage, error) {
	basic, err := h.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	format := basic >> 6
	chunkStreamID := uint32(basic & 0x3f)
	switch chunkStreamID {
	case 0:
		value, err := h.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		chunkStreamID = uint32(value) + 64
	case 1:
		var extended [2]byte
		if _, err := io.ReadFull(h.reader, extended[:]); err != nil {
			return nil, err
		}
		chunkStreamID = uint32(extended[0]) + uint32(extended[1])*256 + 64
	}
	if format != 0 {
		return nil, fmt.Errorf("unsupported RTMP chunk header format %d", format)
	}
	var header [11]byte
	if _, err := io.ReadFull(h.reader, header[:]); err != nil {
		return nil, err
	}
	timestamp := uint32(header[0])<<16 | uint32(header[1])<<8 | uint32(header[2])
	length := uint32(header[3])<<16 | uint32(header[4])<<8 | uint32(header[5])
	if length > maxMessageSize {
		return nil, ErrMessageTooLarge
	}
	messageType := header[6]
	streamID := binary.LittleEndian.Uint32(header[7:])
	if timestamp == 0xffffff {
		var extended [4]byte
		if _, err := io.ReadFull(h.reader, extended[:]); err != nil {
			return nil, err
		}
		timestamp = binary.BigEndian.Uint32(extended[:])
	}
	payload := make([]byte, length)
	remaining := length
	chunkSize := h.chunkSize
	if chunkSize == 0 {
		chunkSize = defaultChunkSize
	}
	for remaining > 0 {
		part := remaining
		if part > chunkSize {
			part = chunkSize
		}
		start := length - remaining
		if _, err := io.ReadFull(h.reader, payload[start:start+part]); err != nil {
			return nil, err
		}
		remaining -= part
		if remaining == 0 {
			break
		}
		continuation, err := h.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if continuation>>6 != 3 || uint32(continuation&0x3f) != chunkStreamID {
			return nil, fmt.Errorf("invalid RTMP continuation chunk")
		}
	}
	return &rtmpMessage{Timestamp: timestamp, Length: length, MessageType: messageType, StreamID: streamID, Payload: payload}, nil
}

func (h *connectionHandler) processMessage(message *rtmpMessage) error {
	if message == nil {
		return fmt.Errorf("nil RTMP message")
	}
	switch message.MessageType {
	case 1: // Set Chunk Size
		if len(message.Payload) != 4 {
			return fmt.Errorf("invalid RTMP chunk-size message")
		}
		chunkSize := binary.BigEndian.Uint32(message.Payload)
		if chunkSize == 0 || chunkSize > maxMessageSize {
			return fmt.Errorf("invalid RTMP chunk size %d", chunkSize)
		}
		h.chunkSize = chunkSize
		return nil
	case 20, 17: // AMF0/AMF3 command messages
		command := firstAMFString(message.Payload)
		if strings.EqualFold(command, "publish") {
			streamKey, err := h.extractStreamKey(message.Payload)
			if err != nil {
				return err
			}
			if _, exists := h.registry.Get(streamKey); !exists {
				if _, err := h.registry.Register(streamKey, streamKey); err != nil && !errors.Is(err, core.ErrStreamExists) {
					return fmt.Errorf("register RTMP stream: %w", err)
				}
			}
		}
		return nil
	case 8, 9: // audio/video payloads
		return ErrUnsupportedMedia
	default:
		return nil
	}
}

// extractStreamKey extracts and validates the publish stream key from an AMF
// command payload. It never invents a fallback stream name.
func (h *connectionHandler) extractStreamKey(message []byte) (string, error) {
	values := amfStringValues(message)
	for i, value := range values {
		if strings.EqualFold(value, "publish") && i+1 < len(values) {
			if err := core.ValidateStreamID(values[i+1]); err != nil {
				return "", fmt.Errorf("invalid RTMP stream key: %w", err)
			}
			return values[i+1], nil
		}
	}
	return "", ErrStreamKeyMissing
}

func firstAMFString(data []byte) string {
	values := amfStringValues(data)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func amfStringValues(data []byte) []string {
	values := make([]string, 0, 2)
	for i := 0; i+3 <= len(data); i++ {
		if data[i] != 2 {
			continue
		}
		length := int(data[i+1])<<8 | int(data[i+2])
		start := i + 3
		end := start + length
		if end > len(data) {
			continue
		}
		values = append(values, string(data[start:end]))
		i = end - 1
	}
	return values
}
