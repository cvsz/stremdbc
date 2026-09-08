package srt

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

func TestSRTOutputStreamIDExtractionRejectsUnsafeValues(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	if got := server.extractStreamID([]byte("streamid=demo\x00")); got != "demo" {
		t.Fatalf("stream ID = %q", got)
	}
	if got := server.extractStreamID([]byte("streamid=../escape\x00")); got != "" {
		t.Fatalf("unsafe stream ID = %q", got)
	}
	if got := server.extractStreamID([]byte(strings.Repeat("x", 20))); got != "" {
		t.Fatalf("missing stream ID = %q", got)
	}
	if got := server.extractStreamID([]byte("prefix streamid=demo")); got != "" {
		t.Fatalf("embedded stream ID was accepted: %q", got)
	}
}

func TestSRTOutputStartFailureDoesNotLeaveServerRunning(t *testing.T) {
	server := NewServer(&config.SRTOutputConfig{Host: "127.0.0.1", Port: -1}, nil, zap.NewNop())
	if err := server.Start(context.Background()); err == nil {
		t.Fatal("invalid SRT output address unexpectedly started")
	}
	if server.running {
		t.Fatal("failed SRT output start left running state set")
	}
}

func TestSRTOutputSessionGetterReturnsSnapshot(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	server.sessions["one"] = &Session{ID: "one", State: "connected"}
	snapshot, ok := server.GetSession("one")
	if !ok {
		t.Fatal("session missing")
	}
	snapshot.State = "corrupted"
	if got, _ := server.GetSession("one"); got.State != "connected" {
		t.Fatalf("session snapshot leaked: %+v", got)
	}
}

func TestSRTOutputSessionCanBeLookedUpAndRemovedBySessionID(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	server.handlePacket([]byte("streamid=demo"), &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9000})
	created, ok := server.sessions["demo"]
	if !ok {
		t.Fatal("metadata packet did not create a session")
	}
	if _, ok := server.GetSession(created.ID); !ok {
		t.Fatalf("session ID %q was not found", created.ID)
	}
	server.RemoveSession(created.ID)
	if server.GetSessionCount() != 0 {
		t.Fatal("session was not removed by session ID")
	}
}

func TestSRTOutputStopClearsMetadataSessions(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	server.sessions["demo"] = &Session{ID: "srt_1", StreamID: "demo", State: "metadata"}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("stop SRT output server: %v", err)
	}
	if got := server.GetSessionCount(); got != 0 {
		t.Fatalf("metadata sessions survived shutdown: %d", got)
	}
}

func TestSRTOutputContextCancellationClearsMetadataSessions(t *testing.T) {
	server := NewServer(nil, nil, zap.NewNop())
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen UDP: %v", err)
	}
	defer listener.Close()
	server.listener = listener
	server.running = true
	server.sessions["demo"] = &Session{ID: "srt_1", StreamID: "demo", State: "metadata"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.acceptLoop(ctx, listener)
	if got := server.GetSessionCount(); got != 0 {
		t.Fatalf("metadata sessions survived context cancellation: %d", got)
	}
}
