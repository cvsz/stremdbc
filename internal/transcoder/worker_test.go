package transcoder

import (
	"reflect"
	"testing"

	"github.com/policedbc/stremdbc/internal/config"
)

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
