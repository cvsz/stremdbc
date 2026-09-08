package webrtc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pion/webrtc/v3"
	"github.com/policedbc/stremdbc/internal/auth"
	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

func TestWebRTCParsesStandardsSDPOffers(t *testing.T) {
	request := httptest.NewRequest("POST", "/whip/demo", strings.NewReader("v=0\r\n"))
	request.Header.Set("Content-Type", "application/sdp")
	offer, standard, err := parseSDPOffer(request)
	if err != nil || !standard || offer.Type != webrtc.SDPTypeOffer || offer.SDP != "v=0\r\n" {
		t.Fatalf("offer=%+v standard=%v err=%v", offer, standard, err)
	}
}

func TestWebRTCRejectsEmptyOrOversizedOffers(t *testing.T) {
	request := httptest.NewRequest("POST", "/whip/demo", strings.NewReader(""))
	request.Header.Set("Content-Type", "application/sdp")
	if _, _, err := parseSDPOffer(request); err == nil {
		t.Fatal("empty SDP accepted")
	}
	request = httptest.NewRequest("POST", "/whip/demo", io.LimitReader(strings.NewReader(strings.Repeat("x", maxSDPSize+1)), maxSDPSize+1))
	request.Header.Set("Content-Type", "application/sdp")
	if _, _, err := parseSDPOffer(request); err == nil {
		t.Fatal("oversized SDP accepted")
	}
}

func TestWebRTCRejectsUnsupportedSDPContentTypes(t *testing.T) {
	request := httptest.NewRequest("POST", "/whip/demo", strings.NewReader(`{"type":"offer","sdp":"v=0\r\n"}`))
	request.Header.Set("Content-Type", "application/octet-stream")
	if _, _, err := parseSDPOffer(request); err == nil {
		t.Fatal("unsupported SDP content type was accepted")
	}
}

func TestWebRTCStartFailureDoesNotLeaveServerRunning(t *testing.T) {
	server, err := NewServer(&config.WebRTCConfig{Host: "127.0.0.1", Port: -1}, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("new WebRTC server: %v", err)
	}
	if err := server.Start(context.Background()); err == nil {
		t.Fatal("invalid WebRTC address unexpectedly started")
	}
	if server.httpServer != nil {
		t.Fatal("failed WebRTC start left HTTP server configured")
	}
}

func TestWebRTCAnonymousPlaybackDoesNotAuthorizePublishing(t *testing.T) {
	server, err := NewServer(&config.WebRTCConfig{}, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("new WebRTC server: %v", err)
	}
	manager, err := auth.NewManager("0123456789abcdef0123456789abcdef0123456789abcdef", "15m", []string{"secret-key-123456"}, true)
	if err != nil {
		t.Fatalf("new auth manager: %v", err)
	}
	server.SetAuthManager(manager)
	request := httptest.NewRequest("POST", "/whip/demo", strings.NewReader("v=0\r\n"))
	request.Header.Set("Content-Type", "application/sdp")
	request.Header.Set("Origin", "https://player.example")
	response := httptest.NewRecorder()
	server.api.ServeHTTP(response, request)
	if response.Code != 401 {
		t.Fatalf("anonymous WHIP request returned %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("unauthorized WHIP response missing CORS header: %q", response.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestWebRTCProvidesDeleteSessionCORSPreflight(t *testing.T) {
	server, err := NewServer(&config.WebRTCConfig{}, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("new WebRTC server: %v", err)
	}
	request := httptest.NewRequest(http.MethodOptions, "/whep/demo/550e8400-e29b-41d4-a716-446655440000", nil)
	request.Header.Set("Origin", "https://player.example")
	response := httptest.NewRecorder()
	server.api.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete preflight returned %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("delete preflight CORS header = %q", response.Header().Get("Access-Control-Allow-Origin"))
	}
	if got := response.Header().Values("Access-Control-Allow-Origin"); len(got) != 1 {
		t.Fatalf("delete preflight should emit one CORS origin header, got %v", got)
	}
}

func TestWebRTCServerCopiesICEServerURLs(t *testing.T) {
	cfg := &config.WebRTCConfig{ICEServer: webrtc.ICEServer{URLs: []string{"stun:stun.example.com:3478"}}}
	server, err := NewServer(cfg, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("new WebRTC server: %v", err)
	}
	cfg.ICEServer.URLs[0] = "stun:changed.example.com:3478"
	if got := server.config.ICEServer.URLs[0]; got != "stun:stun.example.com:3478" {
		t.Fatalf("WebRTC server retained mutable ICE configuration: %q", got)
	}
}
