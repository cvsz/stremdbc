package dvr

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

func testDVR(t *testing.T) (*Manager, *core.StreamRegistry) {
	t.Helper()
	registry := core.NewStreamRegistry(config.DefaultConfig())
	if _, err := registry.Register("demo", "Demo"); err != nil {
		t.Fatalf("register stream: %v", err)
	}
	manager, err := NewManager(&config.DVRConfig{
		Enable:      true,
		Path:        t.TempDir(),
		MaxDuration: time.Hour,
		Format:      "mpegts",
	}, registry, zap.NewNop())
	if err != nil {
		t.Fatalf("new DVR: %v", err)
	}
	return manager, registry
}

func TestDVRWritesDataAndAccountsForPause(t *testing.T) {
	manager, registry := testDVR(t)
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("start DVR: %v", err)
	}
	if err := manager.StartRecording("../escape"); !errors.Is(err, ErrInvalidStreamID) {
		t.Fatalf("unsafe ID error = %v", err)
	}
	if err := manager.StartRecording("demo"); err != nil {
		t.Fatalf("start recording: %v", err)
	}
	if err := manager.Write("demo", []byte("one")); err != nil {
		t.Fatalf("write first packet: %v", err)
	}
	if err := manager.PauseRecording("demo"); err != nil {
		t.Fatalf("pause recording: %v", err)
	}
	if err := manager.Write("demo", []byte("paused")); !errors.Is(err, ErrSessionNotRecording) {
		t.Fatalf("paused write error = %v", err)
	}
	if err := manager.ResumeRecording("demo"); err != nil {
		t.Fatalf("resume recording: %v", err)
	}
	if err := manager.Write("demo", []byte("two")); err != nil {
		t.Fatalf("write second packet: %v", err)
	}

	snapshot, ok := manager.GetSession("demo")
	if !ok {
		t.Fatal("DVR session missing")
	}
	snapshot.State = "corrupted"
	if got, _ := manager.GetSession("demo"); got.State != stateRecording {
		t.Fatalf("mutable session leaked: %+v", got)
	}
	if err := manager.StopRecording("demo"); err != nil {
		t.Fatalf("stop recording: %v", err)
	}
	stopped, _ := manager.GetSession("demo")
	if stopped.Duration <= 0 || stopped.FileSize != 6 {
		t.Fatalf("stopped session = %+v", stopped)
	}
	if _, err := os.Stat(stopped.OutputPath); err != nil {
		t.Fatalf("recording file missing: %v", err)
	}
	stream, _ := registry.Get("demo")
	if stream.State != core.StreamStateIdle {
		t.Fatalf("stream state after stop = %s", stream.State)
	}
	if err := manager.Stop(); err != nil {
		t.Fatalf("stop DVR: %v", err)
	}
}

func TestDVRRestoresPreexistingLiveState(t *testing.T) {
	manager, registry := testDVR(t)
	if err := registry.SetState("demo", core.StreamStateLive); err != nil {
		t.Fatalf("set stream live: %v", err)
	}
	if err := manager.StartRecording("demo"); err != nil {
		t.Fatalf("start recording: %v", err)
	}
	if err := manager.StopRecording("demo"); err != nil {
		t.Fatalf("stop recording: %v", err)
	}
	stream, _ := registry.Get("demo")
	if stream.State != core.StreamStateLive {
		t.Fatalf("stream state after stop = %s", stream.State)
	}
}

func TestDVRCanStartAnotherRecordingAfterStop(t *testing.T) {
	manager, _ := testDVR(t)
	if err := manager.StartRecording("demo"); err != nil {
		t.Fatalf("start first recording: %v", err)
	}
	if err := manager.StopRecording("demo"); err != nil {
		t.Fatalf("stop first recording: %v", err)
	}
	if err := manager.StartRecording("demo"); err != nil {
		t.Fatalf("start second recording: %v", err)
	}
	if err := manager.StopRecording("demo"); err != nil {
		t.Fatalf("stop second recording: %v", err)
	}
}

func TestDVROutputRemainsUnderConfiguredDirectory(t *testing.T) {
	manager, _ := testDVR(t)
	if err := manager.StartRecording("demo"); err != nil {
		t.Fatalf("start recording: %v", err)
	}
	session, _ := manager.GetSession("demo")
	root, _ := filepath.Abs(manager.outputPath)
	output, _ := filepath.Abs(session.OutputPath)
	rel, err := filepath.Rel(root, output)
	if err != nil || rel == ".." || len(rel) > 3 && rel[:3] == ".."+string(filepath.Separator) {
		t.Fatalf("DVR output escaped root: %s", session.OutputPath)
	}
	_ = manager.Stop()
}

func TestDVRRejectsSymlinkedStreamDirectory(t *testing.T) {
	manager, _ := testDVR(t)
	if err := os.Symlink(t.TempDir(), filepath.Join(manager.outputPath, "escape")); err != nil {
		t.Fatalf("create stream symlink: %v", err)
	}
	if err := manager.StartRecording("escape"); err == nil {
		t.Fatal("DVR accepted a symlinked stream directory")
	}
}
