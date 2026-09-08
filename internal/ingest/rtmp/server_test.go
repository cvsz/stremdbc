package rtmp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

func TestRTMPHandshakeFollowsWireFormat(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	handler := newConnectionHandler(serverConn, nil, nil, zap.NewNop())
	c1 := bytes.Repeat([]byte{0x11}, 1536)
	done := make(chan error, 1)
	go func() { done <- handler.handshake() }()

	if _, err := clientConn.Write(append([]byte{3}, c1...)); err != nil {
		t.Fatalf("write client handshake: %v", err)
	}
	serverHandshake := make([]byte, 3073)
	if _, err := io.ReadFull(clientConn, serverHandshake); err != nil {
		t.Fatalf("read server handshake: %v", err)
	}
	if serverHandshake[0] != 3 || !bytes.Equal(serverHandshake[1537:], c1) {
		t.Fatal("server handshake did not echo C1 as S2")
	}
	if _, err := clientConn.Write(serverHandshake[1:1537]); err != nil {
		t.Fatalf("write C2: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("handshake: %v", err)
	}
}

func TestRTMPMessageParserReadsChunkedFormatZeroMessage(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	handler := newConnectionHandler(serverConn, nil, nil, zap.NewNop())
	handler.chunkSize = 4
	payload := []byte("abcdefghij")
	message := make([]byte, 0, len(payload)+14)
	message = append(message, 0x03, 0, 0, 1, byte(len(payload)>>16), byte(len(payload)>>8), byte(len(payload)), 20, 1, 0, 0, 0)
	message = append(message, payload[:4]...)
	message = append(message, 0xc3)
	message = append(message, payload[4:8]...)
	message = append(message, 0xc3)
	message = append(message, payload[8:]...)
	go func() { _, _ = clientConn.Write(message) }()

	got, err := handler.readMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	if got.MessageType != 20 || got.StreamID != 1 || !bytes.Equal(got.Payload, payload) {
		t.Fatalf("message = %+v", got)
	}
}

func TestRTMPStreamKeyNeverDefaultsAndValidates(t *testing.T) {
	handler := newConnectionHandler(nil, nil, nil, zap.NewNop())
	if _, err := handler.extractStreamKey([]byte("publish")); !errors.Is(err, ErrStreamKeyMissing) {
		t.Fatalf("missing key error = %v", err)
	}
	command := append([]byte{2, 0, 7}, []byte("publish")...)
	command = append(command, 2, 0, 4, 'd', 'e', 'm', 'o')
	key, err := handler.extractStreamKey(command)
	if err != nil || key != "demo" {
		t.Fatalf("stream key = %q, error = %v", key, err)
	}
}

func TestRTMPStartFailureDoesNotLeaveServerRunning(t *testing.T) {
	server := NewServer(&config.RTMPConfig{Host: "127.0.0.1", Port: -1}, nil, zap.NewNop())
	if err := server.Start(context.Background()); err == nil {
		t.Fatal("invalid RTMP address unexpectedly started")
	}
	if server.running {
		t.Fatal("failed RTMP start left running state set")
	}
}

func TestRTMPConnectionHandlerHonorsContext(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	handler := newConnectionHandler(serverConn, nil, nil, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := handler.run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled handler error = %v", err)
	}
}
