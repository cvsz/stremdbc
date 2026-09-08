package rtsp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

const (
	maxRequestLine = 8 << 10
	maxHeaderBytes = 64 << 10
	maxBodyBytes   = 1 << 20
)

// Server represents an RTSP output control endpoint. It tracks consumers and
// returns standards-shaped control responses; outbound RTP requires a media
// source that is not part of this package.
type Server struct {
	config   *config.RTSPOutputConfig
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

// Session represents an RTSP output session and is returned as a snapshot.
type Session struct {
	ID         string
	StreamID   string
	Conn       net.Conn
	State      string
	CreatedAt  time.Time
	RemoteAddr string
}

type rtspRequest struct {
	Method  string
	URI     string
	CSeq    string
	Headers map[string]string
	Body    []byte
}

func NewServer(cfg *config.RTSPOutputConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	if cfg == nil {
		copyCfg := config.DefaultConfig().RTSPOutput
		cfg = &copyCfg
	}
	if registry == nil {
		registry = core.NewStreamRegistry(nil)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	copyCfg := *cfg
	return &Server{config: &copyCfg, registry: registry, logger: logger.Named("rtsp-output"), sessions: make(map[string]*Session), connections: make(map[net.Conn]struct{})}
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
	s.logger.Info("RTSP output control endpoint started", zap.String("address", listener.Addr().String()))
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
	reader := bufio.NewReaderSize(conn, maxHeaderBytes)
	writer := bufio.NewWriter(conn)
	for {
		if ctx.Err() != nil {
			return
		}
		if s.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		}
		request, err := readRequest(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			_, _ = writer.WriteString(responseForRTSP("", ""))
			_ = writer.Flush()
			return
		}
		if streamID := extractStreamID(request.URI); streamID != "" {
			s.mu.Lock()
			if current, exists := s.sessions[sessionID]; exists {
				current.StreamID = streamID
			}
			s.mu.Unlock()
		}
		if _, err := writer.WriteString(responseForRTSP(request.Method, request.CSeq)); err != nil {
			return
		}
		if err := writer.Flush(); err != nil {
			return
		}
		if request.Method == "TEARDOWN" {
			return
		}
	}
}

func readRequest(reader *bufio.Reader) (*rtspRequest, error) {
	line, err := readBoundedLine(reader, maxRequestLine)
	if err != nil {
		return nil, err
	}
	if strings.IndexFunc(line, unicode.IsControl) >= 0 {
		return nil, fmt.Errorf("invalid RTSP request line")
	}
	parts := strings.SplitN(strings.TrimSpace(line), " ", 3)
	if len(parts) != 3 || parts[2] != "RTSP/1.0" || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("invalid RTSP request line")
	}
	request := &rtspRequest{Method: strings.ToUpper(parts[0]), URI: parts[1], Headers: make(map[string]string)}
	headerBytes := len(line)
	for {
		line, err := readBoundedLine(reader, maxRequestLine)
		if err != nil {
			return nil, err
		}
		if strings.IndexFunc(line, unicode.IsControl) >= 0 {
			return nil, fmt.Errorf("invalid RTSP header")
		}
		headerBytes += len(line)
		if headerBytes > maxHeaderBytes {
			return nil, fmt.Errorf("RTSP headers too large")
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			break
		}
		name, value, ok := strings.Cut(trimmed, ":")
		if !ok || strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("invalid RTSP header")
		}
		name = strings.ToLower(strings.TrimSpace(name))
		if _, exists := request.Headers[name]; exists {
			return nil, fmt.Errorf("duplicate RTSP header %q", name)
		}
		request.Headers[name] = strings.TrimSpace(value)
	}
	request.CSeq = request.Headers["cseq"]
	if request.CSeq == "" {
		return nil, fmt.Errorf("RTSP CSeq header is required")
	}
	if _, err := strconv.ParseUint(request.CSeq, 10, 31); err != nil {
		return nil, fmt.Errorf("invalid RTSP CSeq")
	}
	if value := request.Headers["content-length"]; value != "" {
		length, err := strconv.Atoi(value)
		if err != nil || length < 0 || length > maxBodyBytes {
			return nil, fmt.Errorf("invalid RTSP content length")
		}
		request.Body = make([]byte, length)
		if _, err := io.ReadFull(reader, request.Body); err != nil {
			return nil, err
		}
	}
	return request, nil
}

func readBoundedLine(reader *bufio.Reader, limit int) (string, error) {
	if reader == nil || limit <= 0 {
		return "", fmt.Errorf("invalid RTSP line limit")
	}
	line := make([]byte, 0, minInt(limit, 4096))
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(line)+len(fragment) > limit {
			return "", fmt.Errorf("RTSP line too long")
		}
		line = append(line, fragment...)
		if err == nil {
			value := strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r")
			return value, nil
		}
		if len(line) >= limit || !errors.Is(err, bufio.ErrBufferFull) {
			return "", err
		}
	}
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func responseForRTSP(method, cseq string) string {
	status := "501 Not Implemented"
	if method == "OPTIONS" {
		status = "200 OK"
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "RTSP/1.0 %s\r\n", status)
	if cseq != "" {
		fmt.Fprintf(&builder, "CSeq: %s\r\n", cseq)
	}
	if method == "OPTIONS" {
		builder.WriteString("Public: OPTIONS, DESCRIBE, SETUP, TEARDOWN, PLAY, PAUSE\r\n")
	}
	builder.WriteString("Content-Length: 0\r\n\r\n")
	return builder.String()
}

func extractStreamID(value string) string {
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	path, err := url.PathUnescape(parsed.EscapedPath())
	if err != nil {
		return ""
	}
	path = strings.Trim(path, "/")
	if path == "" {
		return ""
	}
	segments := strings.Split(path, "/")
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return ""
		}
	}
	id := segments[len(segments)-1]
	if strings.HasPrefix(strings.ToLower(id), "trackid=") {
		if len(segments) < 2 {
			return ""
		}
		id = segments[len(segments)-2]
	}
	if core.ValidateStreamID(id) != nil {
		return ""
	}
	return id
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

func nextSessionID() string { return fmt.Sprintf("rtsp_%d", atomic.AddUint64(&sessionCounter, 1)) }
