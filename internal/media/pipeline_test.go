package media

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

func newMediaManager(t *testing.T) (*Manager, *core.StreamRegistry) {
	t.Helper()
	registry := core.NewStreamRegistry(config.DefaultConfig())
	if _, err := registry.Register("demo", "Demo"); err != nil {
		t.Fatalf("register stream: %v", err)
	}
	return NewManager(registry, zap.NewNop()), registry
}

func TestPipelineRequiresRegisteredSafeStreamAndSnapshotsState(t *testing.T) {
	manager, _ := newMediaManager(t)

	if _, err := manager.CreatePipeline("../escape"); !errors.Is(err, core.ErrInvalidStreamID) {
		t.Fatalf("unsafe stream ID error = %v", err)
	}
	if _, err := manager.CreatePipeline("missing"); !errors.Is(err, core.ErrStreamNotFound) {
		t.Fatalf("unknown stream error = %v", err)
	}

	pipeline, err := manager.CreatePipeline("demo")
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	pipeline.State = "corrupted"
	pipeline.BytesIn = 99
	if pipeline.Input != nil || pipeline.Output != nil {
		t.Fatal("new pipeline should not expose IO handles")
	}

	snapshot, ok := manager.GetPipeline("demo")
	if !ok {
		t.Fatal("pipeline snapshot missing")
	}
	if snapshot.State != pipelineStateCreated || snapshot.BytesIn != 0 {
		t.Fatalf("internal pipeline was exposed: %+v", snapshot)
	}
	if _, err := manager.CreatePipeline("demo"); !errors.Is(err, ErrPipelineExists) {
		t.Fatalf("duplicate pipeline error = %v", err)
	}
}

func TestPipelineHonorsCancellationAndShortWrites(t *testing.T) {
	manager, registry := newMediaManager(t)
	if _, err := manager.CreatePipeline("demo"); err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	if err := manager.SetOutput("demo", shortWriter{limit: 1}); err != nil {
		t.Fatalf("set output: %v", err)
	}
	if err := manager.StartPipeline(context.Background(), "demo"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	if err := manager.Process(context.Background(), "demo", []byte("data")); !errors.Is(err, ErrShortWrite) {
		t.Fatalf("short write error = %v", err)
	}
	if got := manager.GetPipelineCount(); got != 1 {
		t.Fatalf("pipeline count = %d", got)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := manager.Process(canceled, "demo", []byte("data")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled process error = %v", err)
	}
	if err := manager.StopPipeline("demo"); err != nil {
		t.Fatalf("stop pipeline: %v", err)
	}
	stream, ok := registry.Get("demo")
	if !ok || stream.State != core.StreamStateIdle {
		t.Fatalf("stream state after stop = %+v", stream)
	}
}

func TestPipelineRejectsDataWithoutAnOutput(t *testing.T) {
	manager, _ := newMediaManager(t)
	if _, err := manager.CreatePipeline("demo"); err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	if err := manager.StartPipeline(context.Background(), "demo"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}
	if err := manager.Process(context.Background(), "demo", []byte("discard me")); err == nil {
		t.Fatal("pipeline reported success while no output was configured")
	}
	snapshot, _ := manager.GetPipeline("demo")
	if snapshot.BytesIn != 0 || snapshot.BytesOut != 0 {
		t.Fatalf("discarded data was counted as delivered: %+v", snapshot)
	}
}

func TestPipelineProcessCountsSuccessfulWritesAndRejectsCanceledStart(t *testing.T) {
	manager, _ := newMediaManager(t)
	if _, err := manager.CreatePipeline("demo"); err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	var output bytes.Buffer
	if err := manager.SetOutput("demo", &output); err != nil {
		t.Fatalf("set output: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := manager.StartPipeline(ctx, "demo"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled start error = %v", err)
	}
	if err := manager.StartPipeline(context.Background(), "demo"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}
	if err := manager.Process(context.Background(), "demo", []byte("hello")); err != nil {
		t.Fatalf("process pipeline: %v", err)
	}
	if output.String() != "hello" {
		t.Fatalf("output = %q", output.String())
	}
	snapshot, _ := manager.GetPipeline("demo")
	if snapshot.BytesIn != 5 || snapshot.BytesOut != 5 {
		t.Fatalf("stats = %+v", snapshot)
	}
	if err := manager.RemovePipeline("demo"); err != nil {
		t.Fatalf("remove pipeline: %v", err)
	}
	if err := manager.RemovePipeline("demo"); !errors.Is(err, ErrPipelineNotFound) {
		t.Fatalf("missing remove error = %v", err)
	}
}

type shortWriter struct {
	limit int
}

func (w shortWriter) Write(data []byte) (int, error) {
	if len(data) < w.limit {
		return len(data), nil
	}
	return w.limit, nil
}
