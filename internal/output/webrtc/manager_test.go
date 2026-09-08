package webrtc

import (
	"context"
	"errors"
	"testing"

	pion "github.com/pion/webrtc/v3"
	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

func TestWebRTCOutputLifecycleIsExplicitAndIdempotent(t *testing.T) {
	manager, err := NewOutputManager(nil, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("new WebRTC output manager: %v", err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("idempotent start: %v", err)
	}
	if err := manager.Stop(); err != nil {
		t.Fatalf("stop manager: %v", err)
	}
	if err := manager.Stop(); err != nil {
		t.Fatalf("idempotent stop: %v", err)
	}
	if _, _, err := manager.CreateViewer("demo", pion.SessionDescription{Type: pion.SDPTypeOffer, SDP: "v=0"}, ""); !errors.Is(err, ErrOutputManagerStopped) {
		t.Fatalf("post-stop error = %v", err)
	}
}

func TestWebRTCOutputRejectsInvalidOffersBeforePeerCreation(t *testing.T) {
	manager, err := NewOutputManager(&config.WebRTCConfig{}, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("new WebRTC output manager: %v", err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	if _, _, err := manager.CreateViewer("demo", pion.SessionDescription{Type: pion.SDPTypeAnswer, SDP: "v=0"}, ""); !errors.Is(err, ErrInvalidOffer) {
		t.Fatalf("invalid offer error = %v", err)
	}
}

func TestWebRTCOutputCopiesICEServerURLs(t *testing.T) {
	cfg := &config.WebRTCConfig{ICEServer: pion.ICEServer{URLs: []string{"stun:stun.example.com:3478"}}}
	manager, err := NewOutputManager(cfg, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("new WebRTC output manager: %v", err)
	}
	cfg.ICEServer.URLs[0] = "stun:changed.example.com:3478"
	if got := manager.config.ICEServer.URLs[0]; got != "stun:stun.example.com:3478" {
		t.Fatalf("WebRTC output retained mutable ICE configuration: %q", got)
	}
}
