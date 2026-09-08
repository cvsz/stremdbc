package rtsp

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

func TestRTSPParserReadsBoundedHeadersAndBody(t *testing.T) {
	input := "ANNOUNCE rtsp://example/live/demo RTSP/1.0\r\nCSeq: 9\r\nContent-Length: 4\r\nX-Test: value\r\n\r\nbody"
	request, err := readRequest(bufio.NewReader(strings.NewReader(input)))
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	if request.Method != "ANNOUNCE" || request.CSeq != "9" || string(request.Body) != "body" || request.Headers["x-test"] != "value" {
		t.Fatalf("request = %+v", request)
	}
}

func TestRTSPParserRejectsControlCharactersInHeaders(t *testing.T) {
	input := "OPTIONS rtsp://example/live/demo RTSP/1.0\r\nCSeq: 1\r\nTransport: RTP/AVP\rattack\r\n\r\n"
	if _, err := readRequest(bufio.NewReader(strings.NewReader(input))); err == nil {
		t.Fatal("RTSP parser accepted a control character in a header")
	}
}

type countingReader struct {
	reader *strings.Reader
	bytes  int
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes += n
	return n, err
}

func TestRTSPParserStopsReadingAnUnterminatedOversizedLine(t *testing.T) {
	source := &countingReader{reader: strings.NewReader(strings.Repeat("x", maxRequestLine*4))}
	_, err := readBoundedLine(bufio.NewReaderSize(source, 16), maxRequestLine)
	if err == nil {
		t.Fatal("unterminated oversized RTSP line was accepted")
	}
	if source.bytes > maxRequestLine+32 {
		t.Fatalf("parser consumed %d bytes before rejecting a %d-byte line", source.bytes, maxRequestLine)
	}
}

func TestRTSPSetupAllocatesSessionBeforeResponseAndEchoesCSeq(t *testing.T) {
	registry := core.NewStreamRegistry(config.DefaultConfig())
	server := NewServer(&config.RTSPConfig{Host: "127.0.0.1", Port: 8554}, registry, zap.NewNop())
	request := &rtspRequest{Method: "SETUP", URI: "rtsp://example/live/demo", CSeq: "42", Headers: map[string]string{"transport": "RTP/AVP/TCP;unicast"}}
	response, sessionID, closeConnection := server.handleRequest(request, "", nil)
	if closeConnection || sessionID == "" {
		t.Fatalf("setup result: session=%q close=%v", sessionID, closeConnection)
	}
	if !strings.Contains(response, "CSeq: 42") || !strings.Contains(response, "Session: "+sessionID) {
		t.Fatalf("response = %q", response)
	}
	if server.GetSessionCount() != 1 {
		t.Fatalf("session count = %d", server.GetSessionCount())
	}
	if got, ok := registry.Get("demo"); !ok || got.State != core.StreamStateIdle {
		t.Fatalf("stream = %+v", got)
	}
}

func TestRTSPUsesBaseStreamForTrackURLsAndBindsSessionToStream(t *testing.T) {
	registry := core.NewStreamRegistry(config.DefaultConfig())
	server := NewServer(&config.RTSPConfig{Host: "127.0.0.1", Port: 8554}, registry, zap.NewNop())
	setup := &rtspRequest{Method: "SETUP", URI: "rtsp://example/live/demo/trackID=0", CSeq: "1", Headers: map[string]string{"transport": "RTP/AVP/TCP;unicast"}}
	response, sessionID, closeConnection := server.handleRequest(setup, "", nil)
	if closeConnection || !strings.Contains(response, "200 OK") || sessionID == "" {
		t.Fatalf("track setup result: response=%q session=%q close=%v", response, sessionID, closeConnection)
	}
	if session, ok := server.GetSession(sessionID); !ok || session.StreamID != "demo" {
		t.Fatalf("session should be bound to demo: %+v", session)
	}

	wrongStream := &rtspRequest{Method: "PLAY", URI: "rtsp://example/live/other", CSeq: "2", Headers: map[string]string{"session": sessionID}}
	response, nextSessionID, _ := server.handleRequest(wrongStream, sessionID, nil)
	if !strings.Contains(response, "459 Aggregate Operation Not Allowed") || nextSessionID != sessionID {
		t.Fatalf("wrong-stream PLAY response=%q session=%q", response, nextSessionID)
	}

	track := &rtspRequest{Method: "SETUP", URI: "rtsp://example/live/demo/trackID=1", CSeq: "3", Headers: map[string]string{"transport": "RTP/AVP/TCP;unicast", "session": sessionID}}
	response, nextSessionID, _ = server.handleRequest(track, sessionID, nil)
	if !strings.Contains(response, "200 OK") || nextSessionID != sessionID {
		t.Fatalf("second track SETUP response=%q session=%q", response, nextSessionID)
	}
}

func TestRTSPExtractsStreamIDBeforeTrackSuffix(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	for _, uri := range []string{"rtsp://example/live/demo", "rtsp://example/live/demo/trackID=0", "rtsp://example/live/demo/trackID=1?tcp=1"} {
		if got := server.extractStreamID(uri); got != "demo" {
			t.Fatalf("extractStreamID(%q) = %q, want demo", uri, got)
		}
	}
}

func TestRTSPRejectsInvalidSessionAndStreamIDs(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	request := &rtspRequest{Method: "SETUP", URI: "rtsp://example/live/../escape", CSeq: "1", Headers: map[string]string{"transport": "RTP/AVP/TCP"}}
	response, _, _ := server.handleRequest(request, "", nil)
	if !strings.Contains(response, "400 Bad Request") {
		t.Fatalf("unsafe stream response = %q", response)
	}
	request = &rtspRequest{Method: "PLAY", URI: "rtsp://example/live/demo", CSeq: "2", Headers: map[string]string{}}
	response, _, _ = server.handleRequest(request, "missing", nil)
	if !strings.Contains(response, "454 Session Not Found") {
		t.Fatalf("missing session response = %q", response)
	}
}

func TestRTSPStartFailureDoesNotLeaveServerRunning(t *testing.T) {
	server := NewServer(&config.RTSPConfig{Host: "127.0.0.1", Port: -1}, nil, zap.NewNop())
	if err := server.Start(context.Background()); err == nil {
		t.Fatal("invalid RTSP address unexpectedly started")
	}
	if server.running {
		t.Fatal("failed RTSP start left running state set")
	}
}

func TestRTSPSessionGetterDoesNotExposeConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	server := NewServer(nil, nil, zap.NewNop())
	server.sessions["one"] = &Session{ID: "one", Conn: serverConn}
	snapshot, ok := server.GetSession("one")
	if !ok || snapshot.Conn != nil {
		t.Fatalf("session snapshot exposed connection: %+v", snapshot)
	}
}
