package hls

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

func newTestManager(t *testing.T) (*OutputManager, *config.HLSConfig) {
	t.Helper()
	cfg := &config.HLSConfig{Enable: true, Path: t.TempDir(), SegmentDuration: 1200 * time.Millisecond, PlaylistSize: 2}
	manager, err := NewOutputManager(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("new HLS manager: %v", err)
	}
	return manager, cfg
}

func TestHLSPlaylistIsLiveAndUsesConfiguredWindow(t *testing.T) {
	manager, _ := newTestManager(t)
	if _, err := manager.Start("demo"); err != nil {
		t.Fatalf("start stream: %v", err)
	}
	for _, sequence := range []int{4, 5} {
		if err := manager.WriteSegment("demo", sequence, []byte{0, 1, 2}); err != nil {
			t.Fatalf("write segment %d: %v", sequence, err)
		}
	}
	if err := manager.WritePlaylist("demo", []int{4, 5}); err != nil {
		t.Fatalf("write playlist: %v", err)
	}
	path, ok := manager.GetPath("demo")
	if !ok {
		t.Fatal("HLS output path should be registered")
	}
	data, err := os.ReadFile(filepath.Join(path, "index.m3u8"))
	if err != nil {
		t.Fatalf("read playlist: %v", err)
	}
	playlist := string(data)
	for _, want := range []string{"#EXT-X-TARGETDURATION:2", "#EXT-X-MEDIA-SEQUENCE:4", "#EXTINF:1.200,"} {
		if !strings.Contains(playlist, want) {
			t.Fatalf("playlist missing %q:\n%s", want, playlist)
		}
	}
	if strings.Contains(playlist, "#EXT-X-ENDLIST") {
		t.Fatal("live playlist must not contain ENDLIST")
	}
}

func TestHLSCleanupRetainsLatestSegmentsAndIgnoresOtherFiles(t *testing.T) {
	manager, _ := newTestManager(t)
	if _, err := manager.Start("demo"); err != nil {
		t.Fatalf("start stream: %v", err)
	}
	path, _ := manager.GetPath("demo")
	for _, sequence := range []int{1, 2, 3, 4} {
		if err := os.WriteFile(filepath.Join(path, "segment_"+itoa(sequence)+".ts"), []byte{1}, 0o600); err != nil {
			t.Fatalf("write segment: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(path, "keep.txt"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write unrelated file: %v", err)
	}
	manager.cleanupOldSegments()
	for _, sequence := range []int{1, 2} {
		if _, err := os.Stat(filepath.Join(path, "segment_"+itoa(sequence)+".ts")); !os.IsNotExist(err) {
			t.Fatalf("old segment %d should be removed, err=%v", sequence, err)
		}
	}
	for _, sequence := range []int{3, 4} {
		if _, err := os.Stat(filepath.Join(path, "segment_"+itoa(sequence)+".ts")); err != nil {
			t.Fatalf("latest segment %d should remain: %v", sequence, err)
		}
	}
	if _, err := os.Stat(filepath.Join(path, "keep.txt")); err != nil {
		t.Fatalf("unrelated file should remain: %v", err)
	}
}

func TestHLSRejectsUnsafeStreamIDsAndInvalidCleanupInterval(t *testing.T) {
	manager, _ := newTestManager(t)
	if _, err := manager.Start("../escape"); err == nil {
		t.Fatal("unsafe stream ID should be rejected")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	manager.Cleanup(ctx, 0)
}

func TestHLSRejectsSymlinkedStreamDirectory(t *testing.T) {
	manager, cfg := newTestManager(t)
	if err := os.Symlink(t.TempDir(), filepath.Join(cfg.Path, "escape")); err != nil {
		t.Fatalf("create stream symlink: %v", err)
	}
	if _, err := manager.Start("escape"); err == nil {
		t.Fatal("HLS accepted a symlinked stream directory")
	}
}

func TestHLSRejectsUnboundedPlaylistSize(t *testing.T) {
	cfg := &config.HLSConfig{Enable: true, Path: t.TempDir(), SegmentDuration: time.Second, PlaylistSize: config.MaxPlaylistSize + 1}
	if _, err := NewOutputManager(cfg, zap.NewNop()); err == nil {
		t.Fatal("HLS accepted an unbounded playlist size")
	}
}

func TestHLSStopRemovesStaleOutput(t *testing.T) {
	manager, _ := newTestManager(t)
	path, err := manager.Start("demo")
	if err != nil {
		t.Fatalf("start stream: %v", err)
	}
	if err := manager.WriteSegment("demo", 1, []byte("segment")); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	if err := manager.Stop("demo"); err != nil {
		t.Fatalf("stop stream: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("stale HLS output still exists, err=%v", err)
	}
}

func itoa(value int) string {
	return strconv.Itoa(value)
}
