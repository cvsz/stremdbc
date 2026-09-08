package transcoder

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
)

func testTranscoder(t *testing.T) *Manager {
	t.Helper()
	ffmpeg := filepath.Join(t.TempDir(), "fake-ffmpeg")
	if err := os.WriteFile(ffmpeg, []byte("#!/bin/sh\ncase \"$*\" in *-encoders*) exit 0;; esac\nlast=\"\"\nfor arg in \"$@\"; do last=\"$arg\"; done\nprintf '#EXTM3U\\n' > \"$last\"\n"), 0o700); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	manager, err := NewManager(&config.TranscoderConfig{
		Enable:       true,
		WorkerCount:  1,
		FFmpegPath:   ffmpeg,
		OutputFormat: "hls",
		ABRLadder: []config.ABRProfile{{
			Name: "360p", Width: 640, Height: 360, Bitrate: 800000, FrameRate: 30, AudioBitrate: 64000,
		}},
	}, nil)
	if err != nil {
		t.Fatalf("new transcoder: %v", err)
	}
	return manager
}

func TestTranscoderFailsWhenFFmpegProducesNoOutput(t *testing.T) {
	manager, err := NewManager(&config.TranscoderConfig{
		Enable:       true,
		WorkerCount:  1,
		FFmpegPath:   "/bin/true",
		OutputFormat: "hls",
		ABRLadder: []config.ABRProfile{{
			Name: "360p", Width: 640, Height: 360, Bitrate: 800000, FrameRate: 30, AudioBitrate: 64000,
		}},
	}, nil)
	if err != nil {
		t.Fatalf("new transcoder: %v", err)
	}
	if err := manager.Start(); err != nil {
		t.Fatalf("start transcoder: %v", err)
	}
	done := make(chan error, 1)
	if err := manager.Submit(&TranscodeJob{ID: "empty-output", StreamID: "empty-output", InputPath: "/tmp/input.ts", OutputDir: t.TempDir(), OnComplete: func(err error) { done <- err }}); err != nil {
		t.Fatalf("submit job: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("transcoder reported success without an output playlist")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("transcoder completion callback did not run")
	}
	if err := manager.Stop(); err != nil {
		t.Fatalf("stop transcoder: %v", err)
	}
}

func TestBuildFFmpegArgsKeepsCodecOptionsWellFormed(t *testing.T) {
	worker := &Worker{encoder: "libx264"}
	job := &TranscodeJob{StreamID: "demo", InputPath: "input.mp4", OutputDir: "/tmp/out"}
	profile := config.ABRProfile{Name: "720p", Width: 1280, Height: 720, Bitrate: 3000000, FrameRate: 30, AudioBitrate: 128000}
	args := worker.buildFFmpegArgs(job, profile)

	mustContainSequence(t, args, []string{"-c:v", "libx264", "-preset", "fast", "-b:v", "3000000"})
	mustContainSequence(t, args, []string{"-c:a", "aac", "-b:a", "128000"})
	mustContainSequence(t, args, []string{"-f", "hls"})
}

func mustContainSequence(t *testing.T, args, expected []string) {
	t.Helper()
	for i := 0; i+len(expected) <= len(args); i++ {
		if reflect.DeepEqual(args[i:i+len(expected)], expected) {
			return
		}
	}
	t.Fatalf("argument sequence %v not found in %v", expected, args)
}

func TestTranscoderRequiresStartedManagerAndSafeJobs(t *testing.T) {
	manager := testTranscoder(t)
	job := &TranscodeJob{ID: "job-1", StreamID: "demo", InputPath: "/tmp/input.ts", OutputDir: t.TempDir()}
	if err := manager.Submit(job); !errors.Is(err, ErrManagerNotStarted) {
		t.Fatalf("submit before start error = %v", err)
	}
	if err := manager.Start(); err != nil {
		t.Fatalf("start transcoder: %v", err)
	}
	if err := manager.Start(); err != nil {
		t.Fatalf("idempotent start: %v", err)
	}
	if err := manager.Submit(&TranscodeJob{ID: "bad", StreamID: "../escape", InputPath: "/tmp/input.ts", OutputDir: t.TempDir()}); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("unsafe stream error = %v", err)
	}
	if err := manager.Submit(&TranscodeJob{ID: "bad-profile", StreamID: "demo", InputPath: "/tmp/input.ts", OutputDir: t.TempDir(), Profiles: []config.ABRProfile{{Name: "../escape", Width: 1, Height: 1, Bitrate: 1, FrameRate: 1, AudioBitrate: 1}}}); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("unsafe profile error = %v", err)
	}
	if err := manager.Stop(); err != nil {
		t.Fatalf("stop transcoder: %v", err)
	}
	if err := manager.Stop(); err != nil {
		t.Fatalf("idempotent stop: %v", err)
	}
}

func TestTranscoderRunsConfiguredBinaryAndRejectsDuplicateJobs(t *testing.T) {
	manager := testTranscoder(t)
	if err := manager.Start(); err != nil {
		t.Fatalf("start transcoder: %v", err)
	}
	done := make(chan error, 1)
	outputDir, err := os.MkdirTemp("", "stremdbc-transcoder-")
	if err != nil {
		t.Fatalf("create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)
	job := &TranscodeJob{ID: "job-1", StreamID: "demo", InputPath: "/tmp/input.ts", OutputDir: outputDir, OnComplete: func(err error) { done <- err }}
	if err := manager.Submit(job); err != nil {
		t.Fatalf("submit job: %v", err)
	}
	if err := manager.Submit(&TranscodeJob{ID: "job-1", StreamID: "demo", InputPath: "/tmp/input.ts", OutputDir: outputDir}); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("duplicate job error = %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("transcode callback error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("transcode job did not finish")
	}
	if err := manager.Stop(); err != nil {
		t.Fatalf("stop transcoder: %v", err)
	}
	if err := manager.Submit(job); !errors.Is(err, ErrManagerStopped) {
		t.Fatalf("submit after stop error = %v", err)
	}
	_ = context.Background()
}

func TestTranscoderCancelsQueuedJobsOnStop(t *testing.T) {
	manager := testTranscoder(t)
	if err := manager.Start(); err != nil {
		t.Fatalf("start transcoder: %v", err)
	}
	callbacks := make(chan error, 2)
	for _, jobID := range []string{"job-a", "job-b"} {
		job := &TranscodeJob{
			ID:         jobID,
			StreamID:   jobID,
			InputPath:  "/tmp/input.ts",
			OutputDir:  t.TempDir(),
			OnComplete: func(err error) { callbacks <- err },
		}
		if err := manager.Submit(job); err != nil {
			t.Fatalf("submit %s: %v", jobID, err)
		}
	}
	if err := manager.Stop(); err != nil {
		t.Fatalf("stop transcoder: %v", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-callbacks:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected callback error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("queued job callback did not run")
		}
	}
	manager.mu.RLock()
	remainingJobs, remainingStreams := len(manager.jobIDs), len(manager.streamIDs)
	manager.mu.RUnlock()
	if remainingJobs != 0 || remainingStreams != 0 {
		t.Fatalf("stale job bookkeeping after stop: jobs=%d streams=%d", remainingJobs, remainingStreams)
	}
}

func TestTranscoderRejectsUnboundedResources(t *testing.T) {
	base := config.TranscoderConfig{
		Enable:       true,
		WorkerCount:  1,
		FFmpegPath:   "/bin/true",
		OutputFormat: "hls",
		ABRLadder: []config.ABRProfile{{
			Name: "360p", Width: 640, Height: 360, Bitrate: 800000, FrameRate: 30, AudioBitrate: 64000,
		}},
	}
	for _, test := range []struct {
		name   string
		change func(*config.TranscoderConfig)
	}{
		{name: "workers", change: func(cfg *config.TranscoderConfig) { cfg.WorkerCount = 257 }},
		{name: "width", change: func(cfg *config.TranscoderConfig) { cfg.ABRLadder[0].Width = 16385 }},
		{name: "height", change: func(cfg *config.TranscoderConfig) { cfg.ABRLadder[0].Height = 16385 }},
		{name: "video bitrate", change: func(cfg *config.TranscoderConfig) { cfg.ABRLadder[0].Bitrate = 100000001 }},
		{name: "frame rate", change: func(cfg *config.TranscoderConfig) { cfg.ABRLadder[0].FrameRate = 241 }},
		{name: "audio bitrate", change: func(cfg *config.TranscoderConfig) { cfg.ABRLadder[0].AudioBitrate = 10000001 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := base
			cfg.ABRLadder = append([]config.ABRProfile(nil), base.ABRLadder...)
			test.change(&cfg)
			if _, err := NewManager(&cfg, nil); err == nil {
				t.Fatal("expected unbounded transcoder resource to be rejected")
			}
		})
	}
}
