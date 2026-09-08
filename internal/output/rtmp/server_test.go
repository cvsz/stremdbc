package rtmp

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

func TestRTMPOutputStartFailureDoesNotLeaveServerRunning(t *testing.T) {
	server := NewServer(&config.RTMPOutputConfig{Host: "127.0.0.1", Port: -1}, nil, zap.NewNop())
	if err := server.Start(context.Background()); err == nil {
		t.Fatal("invalid RTMP output address unexpectedly started")
	}
	if server.running {
		t.Fatal("failed RTMP output start left running state set")
	}
}

func TestRTMPOutputSessionGetterReturnsSnapshot(t *testing.T) {
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

func TestRTMPOutputClosesIdleClientOnReadTimeout(t *testing.T) {
	server := NewServer(&config.RTMPOutputConfig{ReadTimeout: 10 * time.Millisecond}, nil, zap.NewNop())
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	done := make(chan struct{})
	go func() {
		server.handleConnection(context.Background(), serverConn)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("idle RTMP output client was not closed")
	}
	if got := server.GetSessionCount(); got != 0 {
		t.Fatalf("session count after timeout = %d", got)
	}
}
