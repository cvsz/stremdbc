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

// Server represents an RTSP ingest server. It implements bounded RTSP
// control/session handling; RTP media forwarding is not enabled by this
// dependency-free adapter.
type Server struct {
	config   *config.RTSPConfig
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
	sessionSeq  uint64
}

// Session represents an RTSP session and is returned as a snapshot.
type Session struct {
	ID        string
	StreamID  string
	Conn      net.Conn
	Seq       int
	State     string
	CreatedAt time.Time
}

type rtspRequest struct {
	Method  string
	URI     string
	Version string
	CSeq    string
	Headers map[string]string
	Body    []byte
}

func NewServer(cfg *config.RTSPConfig, registry *core.StreamRegistry, logger *zap.Logger) *Server {
	if cfg == nil {
		copyCfg := config.DefaultConfig().RTSP
		cfg = &copyCfg
	}
	if registry == nil {
		registry = core.NewStreamRegistry(nil)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	copyCfg := *cfg
	return &Server{config: &copyCfg, registry: registry, logger: logger.Named("rtsp"), sessions: make(map[string]*Session), connections: make(map[net.Conn]struct{})}
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
	s.logger.Info("RTSP server started", zap.String("address", listener.Addr().String()))
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

	s.logger.Info("stopping RTSP server")
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
			defer func() {
				s.mu.Lock()
				delete(s.connections, conn)
				s.removeSessionsForConnLocked(conn)
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
	reader := bufio.NewReaderSize(conn, maxHeaderBytes)
	writer := bufio.NewWriter(conn)
	sessionID := ""
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if s.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		}
		request, err := readRequest(reader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			s.logger.Debug("RTSP request rejected", zap.Error(err))
			return
		}
		response, newSessionID, closeConnection := s.handleRequest(request, sessionID, conn)
		if _, err := writer.WriteString(response); err != nil {
			return
		}
		if err := writer.Flush(); err != nil {
			return
		}
		if newSessionID != "" {
			sessionID = newSessionID
		}
		if closeConnection {
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
	request := &rtspRequest{Method: strings.ToUpper(parts[0]), URI: parts[1], Version: parts[2], Headers: make(map[string]string)}
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
	contentLength := 0
	if value := request.Headers["content-length"]; value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 || parsed > maxBodyBytes {
			return nil, fmt.Errorf("invalid RTSP content length")
		}
		contentLength = parsed
	}
	if contentLength > 0 {
		request.Body = make([]byte, contentLength)
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

func (s *Server) handleRequest(request *rtspRequest, sessionID string, conn net.Conn) (string, string, bool) {
	if request == nil {
		return rtspResponse("400 Bad Request", "", nil, ""), sessionID, true
	}
	cseq := request.CSeq
	requestedSession, err := requestSessionID(request, sessionID)
	if err != nil {
		return rtspResponse("400 Bad Request", cseq, nil, ""), sessionID, false
	}
	sessionID = requestedSession
	switch request.Method {
	case "OPTIONS":
		return rtspResponse("200 OK", cseq, map[string]string{"Public": "OPTIONS, DESCRIBE, SETUP, TEARDOWN, PLAY, PAUSE"}, ""), sessionID, false
	case "DESCRIBE":
		streamID := s.extractStreamID(request.URI)
		if streamID == "" {
			return rtspResponse("400 Bad Request", cseq, nil, ""), sessionID, false
		}
		if _, exists := s.registry.Get(streamID); !exists {
			return rtspResponse("404 Not Found", cseq, nil, ""), sessionID, false
		}
		return rtspResponse("501 Not Implemented", cseq, map[string]string{"Content-Type": "application/sdp"}, ""), sessionID, false
	case "SETUP":
		if request.Headers["transport"] == "" {
			return rtspResponse("400 Bad Request", cseq, nil, ""), sessionID, false
		}
		streamID := s.extractStreamID(request.URI)
		if streamID == "" {
			return rtspResponse("400 Bad Request", cseq, nil, ""), sessionID, false
		}
		if sessionID != "" {
			if status := s.sessionStreamStatus(sessionID, streamID); status != "" {
				return rtspResponse(status, cseq, nil, ""), sessionID, false
			}
		}
		if _, exists := s.registry.Get(streamID); !exists {
			if _, err := s.registry.Register(streamID, streamID); err != nil && !errors.Is(err, core.ErrStreamExists) {
				return rtspResponse("500 Internal Server Error", cseq, nil, ""), sessionID, false
			}
		}
		if sessionID == "" {
			sessionID = fmt.Sprintf("rtsp_%d", atomic.AddUint64(&s.sessionSeq, 1))
			seq, _ := strconv.Atoi(cseq)
			s.mu.Lock()
			s.sessions[sessionID] = &Session{ID: sessionID, StreamID: streamID, Conn: conn, Seq: seq, State: "ready", CreatedAt: time.Now().UTC()}
			s.mu.Unlock()
		}
		return rtspResponse("200 OK", cseq, map[string]string{"Session": sessionID, "Transport": request.Headers["transport"]}, ""), sessionID, false
	case "PLAY":
		streamID := s.extractStreamID(request.URI)
		if streamID == "" {
			return rtspResponse("400 Bad Request", cseq, nil, ""), sessionID, false
		}
		if status := s.sessionStreamStatus(sessionID, streamID); status != "" {
			return rtspResponse(status, cseq, nil, ""), sessionID, false
		}
		s.setSessionState(sessionID, "playing")
		return rtspResponse("200 OK", cseq, map[string]string{"Session": sessionID, "Range": "npt=0.000-"}, ""), sessionID, false
	case "PAUSE":
		streamID := s.extractStreamID(request.URI)
		if streamID == "" {
			return rtspResponse("400 Bad Request", cseq, nil, ""), sessionID, false
		}
		if status := s.sessionStreamStatus(sessionID, streamID); status != "" {
			return rtspResponse(status, cseq, nil, ""), sessionID, false
		}
		s.setSessionState(sessionID, "paused")
		return rtspResponse("200 OK", cseq, map[string]string{"Session": sessionID}, ""), sessionID, false
	case "TEARDOWN":
		streamID := s.extractStreamID(request.URI)
		if streamID == "" {
			return rtspResponse("400 Bad Request", cseq, nil, ""), sessionID, true
		}
		if status := s.sessionStreamStatus(sessionID, streamID); status != "" {
			return rtspResponse(status, cseq, nil, ""), sessionID, true
		}
		s.mu.Lock()
		delete(s.sessions, sessionID)
		s.mu.Unlock()
		return rtspResponse("200 OK", cseq, map[string]string{"Session": sessionID}, ""), sessionID, true
	default:
		return rtspResponse("501 Not Implemented", cseq, nil, ""), sessionID, false
	}
}

func rtspResponse(status, cseq string, headers map[string]string, body string) string {
	var builder strings.Builder
	builder.WriteString("RTSP/1.0 ")
	builder.WriteString(status)
	builder.WriteString("\r\n")
	if cseq != "" {
		builder.WriteString("CSeq: ")
		builder.WriteString(cseq)
		builder.WriteString("\r\n")
	}
	for name, value := range headers {
		builder.WriteString(name)
		builder.WriteString(": ")
		builder.WriteString(value)
		builder.WriteString("\r\n")
	}
	builder.WriteString("Content-Length: ")
	builder.WriteString(strconv.Itoa(len(body)))
	builder.WriteString("\r\n\r\n")
	builder.WriteString(body)
	return builder.String()
}

func (s *Server) sessionStreamStatus(id, streamID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, exists := s.sessions[id]
	if !exists {
		return "454 Session Not Found"
	}
	if session.StreamID != streamID {
		return "459 Aggregate Operation Not Allowed"
	}
	return ""
}

func requestSessionID(request *rtspRequest, fallback string) (string, error) {
	if request == nil {
		return fallback, nil
	}
	value := strings.TrimSpace(request.Headers["session"])
	if value == "" {
		return fallback, nil
	}
	if separator := strings.IndexByte(value, ';'); separator >= 0 {
		value = strings.TrimSpace(value[:separator])
	}
	if value == "" || len(value) > 256 || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("invalid RTSP session")
	}
	return value, nil
}

func (s *Server) setSessionState(id, state string) {
	s.mu.Lock()
	if session, exists := s.sessions[id]; exists {
		session.State = state
	}
	s.mu.Unlock()
}

func (s *Server) extractStreamID(value string) string {
	parts := strings.Fields(value)
	if len(parts) > 1 {
		value = parts[1]
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	path, err := url.PathUnescape(parsed.EscapedPath())
	if err != nil || strings.IndexFunc(path, func(r rune) bool { return r == '\x00' || r == '\r' || r == '\n' }) >= 0 {
		return ""
	}
	path = strings.Trim(path, "/")
	if path == "" {
		return ""
	}
	segments := strings.Split(path, "/")
	for _, segment := range segments {
		if segment == "." || segment == ".." || segment == "" {
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
	if err := core.ValidateStreamID(id); err != nil {
		return ""
	}
	return id
}

func (s *Server) removeSessionsForConnLocked(conn net.Conn) {
	for id, session := range s.sessions {
		if session.Conn == conn {
			delete(s.sessions, id)
		}
	}
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
