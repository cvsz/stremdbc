package srt

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

func TestSRTStreamIDExtractionRejectsMissingAndUnsafeValues(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	if got := server.extractStreamID([]byte("streamid=demo\x00")); got != "demo" {
		t.Fatalf("stream ID = %q", got)
	}
	if got := server.extractStreamID([]byte("streamid=../escape\x00")); got != "" {
		t.Fatalf("unsafe stream ID = %q", got)
	}
	if got := server.extractStreamID([]byte("unrelated packet")); got != "" {
		t.Fatalf("missing stream ID = %q", got)
	}
	if got := server.extractStreamID([]byte("prefix streamid=demo")); got != "" {
		t.Fatalf("embedded stream ID was accepted: %q", got)
	}
}

func TestSRTStartFailureDoesNotLeaveServerRunning(t *testing.T) {
	server := NewServer(&config.SRTConfig{Host: "127.0.0.1", Port: -1}, nil, zap.NewNop())
	if err := server.Start(context.Background()); err == nil {
		t.Fatal("invalid SRT address unexpectedly started")
	}
	if server.running {
		t.Fatal("failed SRT start left running state set")
	}
}

func TestSRTRejectsArbitraryDatagramsWithoutStreamMetadata(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	server.handlePacket(context.Background(), []byte(strings.Repeat("x", 32)), nil)
	if got := server.GetStreamCount(); got != 0 {
		t.Fatalf("arbitrary datagram created %d streams", got)
	}
}

func TestSRTStreamGetterDoesNotExposeConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	server := NewServer(nil, nil, zap.NewNop())
	server.streams["demo"] = &StreamConnection{StreamID: "demo", Conn: serverConn}
	snapshot, ok := server.GetStream("demo")
	if !ok || snapshot.Conn != nil {
		t.Fatalf("stream snapshot exposed connection: %+v", snapshot)
	}
}

func TestSRTStopClearsMetadataSessions(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	server.streams["demo"] = &StreamConnection{StreamID: "demo", State: "metadata"}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("stop SRT server: %v", err)
	}
	if got := server.GetStreamCount(); got != 0 {
		t.Fatalf("metadata sessions survived shutdown: %d", got)
	}
}

func TestSRTContextCancellationClearsMetadataSessions(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen UDP: %v", err)
	}
	defer listener.Close()
	server.listener = listener
	server.running = true
	server.streams["demo"] = &StreamConnection{StreamID: "demo", State: "metadata"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.acceptLoop(ctx, listener)
	if got := server.GetStreamCount(); got != 0 {
		t.Fatalf("metadata sessions survived context cancellation: %d", got)
	}
}
