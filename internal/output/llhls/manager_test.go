package llhls

import (
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

func TestLLHLSDurationMultiplicationSaturates(t *testing.T) {
	if got := multiplyDuration(time.Second, 2); got != 2*time.Second {
		t.Fatalf("duration multiplication = %s", got)
	}
	if got := multiplyDuration(time.Duration(math.MaxInt64), 3); got != time.Duration(math.MaxInt64) {
		t.Fatalf("overflowing duration multiplication = %s", got)
	}
}

func TestLLHLSUsesConfiguredPathAndReturnsSnapshots(t *testing.T) {
	cfg := &config.LLHLSConfig{Enable: true, Path: t.TempDir(), SegmentDuration: time.Second, PartDuration: 200 * time.Millisecond, PlaylistSize: 3}
	manager, err := NewManager(cfg, core.NewStreamRegistry(config.DefaultConfig()), zap.NewNop())
	if err != nil {
		t.Fatalf("new LL-HLS manager: %v", err)
	}
	if manager.GetOutputPath() != cfg.Path {
		t.Fatalf("expected configured output path %q, got %q", cfg.Path, manager.GetOutputPath())
	}
	if err := manager.CreateStream("demo"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	stream, ok := manager.GetStream("demo")
	if !ok {
		t.Fatal("stream should exist")
	}
	stream.State = "mutated"
	again, _ := manager.GetStream("demo")
	if again.State != "active" {
		t.Fatal("GetStream must return a snapshot")
	}
}

func TestLLHLSConcurrentSegmentsAreCountedWithoutDataLoss(t *testing.T) {
	cfg := &config.LLHLSConfig{Enable: true, Path: t.TempDir(), SegmentDuration: time.Second, PartDuration: 200 * time.Millisecond, PlaylistSize: 3}
	manager, err := NewManager(cfg, core.NewStreamRegistry(config.DefaultConfig()), zap.NewNop())
	if err != nil {
		t.Fatalf("new LL-HLS manager: %v", err)
	}
	if err := manager.CreateStream("demo"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := manager.AddSegment("demo", []byte{1, 2, 3}); err != nil {
				t.Errorf("add segment: %v", err)
			}
		}()
	}
	wg.Wait()
	stream, _ := manager.GetStream("demo")
	if stream.SegmentCount != 20 {
		t.Fatalf("expected 20 segments, got %d", stream.SegmentCount)
	}
	entries, err := os.ReadDir(filepath.Join(cfg.Path, "demo"))
	if err != nil {
		t.Fatalf("read stream directory: %v", err)
	}
	segmentFiles := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".ts" {
			segmentFiles++
		}
	}
	if segmentFiles != 20 {
		t.Fatalf("expected 20 segment files, got %d", segmentFiles)
	}
}

func TestLLHLSRejectsUnsafeIDsAndMissingStreams(t *testing.T) {
	cfg := &config.LLHLSConfig{Enable: true, Path: t.TempDir(), SegmentDuration: time.Second, PartDuration: 200 * time.Millisecond, PlaylistSize: 3}
	manager, err := NewManager(cfg, core.NewStreamRegistry(config.DefaultConfig()), zap.NewNop())
	if err != nil {
		t.Fatalf("new LL-HLS manager: %v", err)
	}
	if err := manager.CreateStream("../escape"); err == nil {
		t.Fatal("unsafe stream ID should be rejected")
	}
	if err := manager.AddSegment("missing", []byte{1}); err == nil {
		t.Fatal("missing stream should be rejected")
	}
}

func TestLLHLSRejectsSymlinkedStreamDirectory(t *testing.T) {
	path := t.TempDir()
	manager, err := NewManager(&config.LLHLSConfig{Enable: true, Path: path, SegmentDuration: time.Second, PartDuration: 200 * time.Millisecond, PlaylistSize: 3}, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("new LL-HLS manager: %v", err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(path, "escape")); err != nil {
		t.Fatalf("create stream symlink: %v", err)
	}
	if err := manager.CreateStream("escape"); err == nil {
		t.Fatal("LL-HLS accepted a symlinked stream directory")
	}
}

func TestLLHLSRejectsUnboundedPlaylistSize(t *testing.T) {
	cfg := &config.LLHLSConfig{Enable: true, Path: t.TempDir(), SegmentDuration: time.Second, PartDuration: 200 * time.Millisecond, PlaylistSize: config.MaxPlaylistSize + 1}
	if _, err := NewManager(cfg, nil, zap.NewNop()); err == nil {
		t.Fatal("LL-HLS accepted an unbounded playlist size")
	}
}

func TestLLHLSStopRemovesStaleOutput(t *testing.T) {
	path := t.TempDir()
	manager, err := NewManager(&config.LLHLSConfig{Enable: true, Path: path, SegmentDuration: time.Second, PartDuration: 200 * time.Millisecond, PlaylistSize: 3}, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("new LL-HLS manager: %v", err)
	}
	if err := manager.CreateStream("demo"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := manager.AddSegment("demo", []byte("segment")); err != nil {
		t.Fatalf("add segment: %v", err)
	}
	if err := manager.Stop(); err != nil {
		t.Fatalf("stop manager: %v", err)
	}
	if _, err := os.Stat(filepath.Join(path, "demo")); !os.IsNotExist(err) {
		t.Fatalf("stale LL-HLS output still exists, err=%v", err)
	}
}
