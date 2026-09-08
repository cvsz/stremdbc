package core

import (
	"testing"

	"github.com/policedbc/stremdbc/internal/config"
)

func TestStreamRegistryLifecycle(t *testing.T) {
	registry := NewStreamRegistry(config.DefaultConfig())
	stream, err := registry.Register("alpha", "Alpha")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if stream.State != StreamStateIdle {
		t.Fatalf("expected idle state, got %s", stream.State)
	}
	if _, err := registry.Register("alpha", "duplicate"); err != ErrStreamExists {
		t.Fatalf("expected duplicate error, got %v", err)
	}
	if err := registry.SetState("alpha", StreamStateLive); err != nil {
		t.Fatalf("set live: %v", err)
	}
	if err := registry.IncrementViewers("alpha"); err != nil {
		t.Fatalf("increment viewers: %v", err)
	}
	got, ok := registry.Get("alpha")
	if !ok || got.State != StreamStateLive || got.Viewers != 1 {
		t.Fatalf("unexpected stream state: %+v", got)
	}
	if registry.LiveCount() != 1 {
		t.Fatalf("expected one live stream, got %d", registry.LiveCount())
	}
	if err := registry.Delete("alpha"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := registry.Get("alpha"); ok {
		t.Fatal("stream should be deleted")
	}
}
