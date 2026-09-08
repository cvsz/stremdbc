package rtsp

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"

	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

func TestRTSPOutputResponsesPreserveCSeqAndAreExplicit(t *testing.T) {
	response := responseForRTSP("OPTIONS", "17")
	if !strings.Contains(response, "RTSP/1.0 200 OK") || !strings.Contains(response, "CSeq: 17") {
		t.Fatalf("OPTIONS response = %q", response)
	}
	response = responseForRTSP("DESCRIBE", "18")
	if !strings.Contains(response, "501 Not Implemented") || !strings.Contains(response, "CSeq: 18") {
		t.Fatalf("DESCRIBE response = %q", response)
	}
}

func TestRTSPOutputExtractsBaseStreamFromTrackURLs(t *testing.T) {
	for _, uri := range []string{"rtsp://example/live/demo", "rtsp://example/live/demo/trackID=0"} {
		if got := extractStreamID(uri); got != "demo" {
			t.Fatalf("extractStreamID(%q) = %q, want demo", uri, got)
		}
	}
}

func TestRTSPOutputParserRejectsControlCharactersInHeaders(t *testing.T) {
	input := "OPTIONS rtsp://example/live/demo RTSP/1.0\r\nCSeq: 1\r\nTransport: RTP/AVP\rattack\r\n\r\n"
	if _, err := readRequest(bufio.NewReader(strings.NewReader(input))); err == nil {
		t.Fatal("RTSP output parser accepted a control character in a header")
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

func TestRTSPOutputParserStopsReadingAnUnterminatedOversizedLine(t *testing.T) {
	source := &countingReader{reader: strings.NewReader(strings.Repeat("x", maxRequestLine*4))}
	_, err := readBoundedLine(bufio.NewReaderSize(source, 16), maxRequestLine)
	if err == nil {
		t.Fatal("unterminated oversized RTSP line was accepted")
	}
	if source.bytes > maxRequestLine+32 {
		t.Fatalf("parser consumed %d bytes before rejecting a %d-byte line", source.bytes, maxRequestLine)
	}
}

func TestRTSPOutputStartFailureDoesNotLeaveServerRunning(t *testing.T) {
	server := NewServer(&config.RTSPOutputConfig{Host: "127.0.0.1", Port: -1}, nil, zap.NewNop())
	if err := server.Start(context.Background()); err == nil {
		t.Fatal("invalid RTSP output address unexpectedly started")
	}
	if server.running {
		t.Fatal("failed RTSP output start left running state set")
	}
}

func TestRTSPOutputSessionGetterReturnsSnapshot(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	server.sessions["one"] = &Session{ID: "one", State: "connected", Conn: serverConn}
	snapshot, ok := server.GetSession("one")
	if !ok {
		t.Fatal("session missing")
	}
	if snapshot.Conn != nil {
		t.Fatal("session snapshot exposed connection")
	}
	snapshot.State = "corrupted"
	if got, _ := server.GetSession("one"); got.State != "connected" {
		t.Fatalf("session snapshot leaked: %+v", got)
	}
}
