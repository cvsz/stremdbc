package core

import (
	"context"
	"strings"
	"testing"
	"time"

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
	if err := registry.Delete("alpha"); err != ErrStreamActive {
		t.Fatalf("active delete error = %v", err)
	}
	if err := registry.DecrementViewers("alpha"); err != nil {
		t.Fatalf("decrement viewers: %v", err)
	}
	if err := registry.SetState("alpha", StreamStateIdle); err != nil {
		t.Fatalf("set idle: %v", err)
	}
	if err := registry.Delete("alpha"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := registry.Get("alpha"); ok {
		t.Fatal("stream should be deleted")
	}
}

func TestStreamRegistryRejectsUnsafeIDs(t *testing.T) {
	registry := NewStreamRegistry(config.DefaultConfig())
	for _, id := range []string{"", "../secret", "space key", strings.Repeat("x", 129)} {
		if _, err := registry.Register(id, "name"); err == nil {
			t.Fatalf("expected %q to be rejected", id)
		}
	}
}

func TestStreamRegistrySnapshotsDoNotShareMetadata(t *testing.T) {
	registry := NewStreamRegistry(config.DefaultConfig())
	created, err := registry.Register("alpha", "Alpha")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := registry.Update("alpha", func(info *StreamInfo) {
		info.Metadata["source"] = "publisher"
	}); err != nil {
		t.Fatalf("update metadata: %v", err)
	}
	if created.Metadata["source"] != "" {
		t.Fatal("register must return a snapshot, not a mutable registry entry")
	}

	got, ok := registry.Get("alpha")
	if !ok {
		t.Fatal("stream should exist")
	}
	got.Metadata["source"] = "mutated"
	gotAgain, ok := registry.Get("alpha")
	if !ok || gotAgain.Metadata["source"] != "publisher" {
		t.Fatalf("metadata escaped registry snapshot: %+v", gotAgain)
	}
}

func TestStreamRegistryListIsDeterministic(t *testing.T) {
	registry := NewStreamRegistry(config.DefaultConfig())
	for _, id := range []string{"charlie", "alpha", "bravo"} {
		if _, err := registry.Register(id, id); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
	streams := registry.List()
	for i, want := range []string{"alpha", "bravo", "charlie"} {
		if streams[i].ID != want {
			t.Fatalf("stream %d: expected %q, got %q", i, want, streams[i].ID)
		}
	}
}

func TestStreamRegistryRejectsNilUpdaterAndInvalidState(t *testing.T) {
	registry := NewStreamRegistry(config.DefaultConfig())
	if _, err := registry.Register("alpha", "Alpha"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := registry.Update("alpha", nil); err == nil {
		t.Fatal("expected nil updater to fail")
	}
	if err := registry.SetState("alpha", StreamState("invalid")); err == nil {
		t.Fatal("expected invalid state to fail")
	}
}

func TestStreamRegistryRejectsInvalidCounterAndMetadataUpdates(t *testing.T) {
	registry := NewStreamRegistry(config.DefaultConfig())
	if _, err := registry.Register("alpha", "Alpha"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := registry.Update("alpha", func(info *StreamInfo) { info.Viewers = -1 }); err == nil {
		t.Fatal("negative viewer count should be rejected")
	}
	if err := registry.Update("alpha", func(info *StreamInfo) { info.Bitrate = -1 }); err == nil {
		t.Fatal("negative bitrate should be rejected")
	}
	if err := registry.Update("alpha", func(info *StreamInfo) { info.Metadata["bad\nkey"] = "value" }); err == nil {
		t.Fatal("metadata control characters should be rejected")
	}
	if err := registry.Update("alpha", func(info *StreamInfo) { info.Name = "  Renamed  " }); err != nil {
		t.Fatalf("trimmed name update: %v", err)
	}
	stream, _ := registry.Get("alpha")
	if stream.Name != "Renamed" {
		t.Fatalf("name was not normalized: %q", stream.Name)
	}
}

func TestStreamRegistryStateTimestampsAndCleanup(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Server.StreamTTL = time.Hour
	registry := NewStreamRegistry(cfg)
	if _, err := registry.Register("idle", "Idle"); err != nil {
		t.Fatalf("register idle: %v", err)
	}
	if _, err := registry.Register("live", "Live"); err != nil {
		t.Fatalf("register live: %v", err)
	}
	if err := registry.SetState("live", StreamStateLive); err != nil {
		t.Fatalf("set live: %v", err)
	}
	first, _ := registry.Get("live")
	if first.StartedAt == nil {
		t.Fatal("live stream should have a start timestamp")
	}
	if err := registry.SetState("live", StreamStateLive); err != nil {
		t.Fatalf("set live again: %v", err)
	}
	second, _ := registry.Get("live")
	if !second.StartedAt.Equal(*first.StartedAt) {
		t.Fatal("reasserting live state must not reset start time")
	}

	old := time.Now().Add(-2 * time.Hour)
	if err := registry.Update("idle", func(info *StreamInfo) { info.LastActivityAt = old }); err != nil {
		t.Fatalf("age idle stream: %v", err)
	}
	registry.cleanupStaleStreams()
	if _, ok := registry.Get("idle"); ok {
		t.Fatal("old idle stream should be cleaned up")
	}
	if _, ok := registry.Get("live"); !ok {
		t.Fatal("live stream must not be cleaned up")
	}

	if err := registry.SetState("live", StreamStateIdle); err != nil {
		t.Fatalf("set idle: %v", err)
	}
	cleared, _ := registry.Get("live")
	if cleared.StartedAt != nil {
		t.Fatal("non-live stream should not retain a start timestamp")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	registry.Cleanup(ctx, 0)
}
