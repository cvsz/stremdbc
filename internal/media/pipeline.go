package media

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

const (
	pipelineStateCreated = "created"
	pipelineStateRunning = "running"
	pipelineStateStopped = "stopped"
)

var (
	ErrPipelineExists     = errors.New("pipeline already exists")
	ErrPipelineNotFound   = errors.New("pipeline not found")
	ErrPipelineNotRunning = errors.New("pipeline is not running")
	ErrPipelineNoOutput   = errors.New("pipeline output is not configured")
	ErrShortWrite         = errors.New("pipeline output short write")
)

// Pipeline represents a media processing pipeline. Values returned by the
// manager are snapshots; use the manager methods to change a pipeline.
type Pipeline struct {
	ID        string
	StreamID  string
	Input     io.Reader
	Output    io.Writer
	State     string
	CreatedAt time.Time
	BytesIn   int64
	BytesOut  int64
	mu        sync.Mutex
}

// Manager manages media pipelines.
type Manager struct {
	logger      *zap.Logger
	registry    *core.StreamRegistry
	mu          sync.RWMutex
	lifecycleMu sync.Mutex
	pipelines   map[string]*Pipeline
}

// NewManager creates a new media pipeline manager.
func NewManager(registry *core.StreamRegistry, logger *zap.Logger) *Manager {
	if registry == nil {
		registry = core.NewStreamRegistry(nil)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Manager{
		logger:    logger.Named("media"),
		registry:  registry,
		pipelines: make(map[string]*Pipeline),
	}
}

// CreatePipeline creates a new pipeline for a registered stream.
func (m *Manager) CreatePipeline(streamID string) (*Pipeline, error) {
	if err := core.ValidateStreamID(streamID); err != nil {
		return nil, err
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if _, exists := m.registry.Get(streamID); !exists {
		return nil, core.ErrStreamNotFound
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.pipelines[streamID]; exists {
		return nil, ErrPipelineExists
	}

	pipeline := &Pipeline{
		ID:        "pipeline_" + streamID,
		StreamID:  streamID,
		State:     pipelineStateCreated,
		CreatedAt: time.Now().UTC(),
	}
	m.pipelines[streamID] = pipeline
	m.logger.Info("media pipeline created", zap.String("stream_id", streamID))
	return pipelineSnapshot(pipeline), nil
}

// RemovePipeline removes a pipeline. A running pipeline is stopped before it
// is removed, and its registry state is returned to idle.
func (m *Manager) RemovePipeline(streamID string) error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	pipeline, err := m.pipeline(streamID)
	if err != nil {
		return err
	}

	pipeline.mu.Lock()
	wasRunning := pipeline.State == pipelineStateRunning
	pipeline.State = pipelineStateStopped
	pipeline.mu.Unlock()

	m.mu.Lock()
	delete(m.pipelines, streamID)
	m.mu.Unlock()
	if wasRunning {
		if err := m.registry.SetState(streamID, core.StreamStateIdle); err != nil && !errors.Is(err, core.ErrStreamNotFound) {
			return err
		}
	}
	m.logger.Info("media pipeline removed", zap.String("stream_id", streamID))
	return nil
}

// GetPipeline returns a snapshot of a pipeline by stream ID.
func (m *Manager) GetPipeline(streamID string) (*Pipeline, bool) {
	m.mu.RLock()
	pipeline, exists := m.pipelines[streamID]
	m.mu.RUnlock()
	if !exists {
		return nil, false
	}
	return pipelineSnapshot(pipeline), true
}

// SetInput sets the input reader for a pipeline.
func (m *Manager) SetInput(streamID string, input io.Reader) error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	pipeline, err := m.pipeline(streamID)
	if err != nil {
		return err
	}
	pipeline.mu.Lock()
	pipeline.Input = input
	pipeline.mu.Unlock()
	return nil
}

// SetOutput sets the output writer for a pipeline.
func (m *Manager) SetOutput(streamID string, output io.Writer) error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	pipeline, err := m.pipeline(streamID)
	if err != nil {
		return err
	}
	pipeline.mu.Lock()
	pipeline.Output = output
	pipeline.mu.Unlock()
	return nil
}

// StartPipeline starts a media pipeline.
func (m *Manager) StartPipeline(ctx context.Context, streamID string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	pipeline, err := m.pipeline(streamID)
	if err != nil {
		return err
	}

	pipeline.mu.Lock()
	if pipeline.State == pipelineStateRunning {
		pipeline.mu.Unlock()
		return nil
	}
	pipeline.State = pipelineStateRunning
	pipeline.mu.Unlock()

	if err := m.registry.SetState(streamID, core.StreamStateLive); err != nil {
		pipeline.mu.Lock()
		pipeline.State = pipelineStateStopped
		pipeline.mu.Unlock()
		return fmt.Errorf("set stream live: %w", err)
	}

	m.logger.Info("media pipeline started", zap.String("stream_id", streamID))
	return nil
}

// StopPipeline stops a media pipeline.
func (m *Manager) StopPipeline(streamID string) error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	pipeline, err := m.pipeline(streamID)
	if err != nil {
		return err
	}

	pipeline.mu.Lock()
	wasRunning := pipeline.State == pipelineStateRunning
	pipeline.State = pipelineStateStopped
	pipeline.mu.Unlock()
	if wasRunning {
		if err := m.registry.SetState(streamID, core.StreamStateIdle); err != nil && !errors.Is(err, core.ErrStreamNotFound) {
			return fmt.Errorf("set stream idle: %w", err)
		}
	}
	m.logger.Info("media pipeline stopped", zap.String("stream_id", streamID))
	return nil
}

// Process processes media data through the pipeline.
func (m *Manager) Process(ctx context.Context, streamID string, data []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	pipeline, err := m.pipeline(streamID)
	if err != nil {
		return err
	}

	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()
	if pipeline.State != pipelineStateRunning {
		return ErrPipelineNotRunning
	}
	if err := contextError(ctx); err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}
	if pipeline.Output == nil {
		return ErrPipelineNoOutput
	}
	pipeline.BytesIn += int64(len(data))
	n, writeErr := pipeline.Output.Write(data)
	if n < 0 || n > len(data) {
		return fmt.Errorf("invalid output write count %d", n)
	}
	pipeline.BytesOut += int64(n)
	if writeErr != nil {
		return fmt.Errorf("write pipeline output: %w", writeErr)
	}
	if n != len(data) {
		return ErrShortWrite
	}
	return nil
}

// GetStats returns aggregate pipeline statistics.
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.RLock()
	pipelines := make([]*Pipeline, 0, len(m.pipelines))
	for _, pipeline := range m.pipelines {
		pipelines = append(pipelines, pipeline)
	}
	m.mu.RUnlock()

	var totalBytesIn, totalBytesOut int64
	runningCount := 0
	for _, pipeline := range pipelines {
		pipeline.mu.Lock()
		totalBytesIn += pipeline.BytesIn
		totalBytesOut += pipeline.BytesOut
		if pipeline.State == pipelineStateRunning {
			runningCount++
		}
		pipeline.mu.Unlock()
	}

	return map[string]interface{}{
		"pipeline_count": len(pipelines),
		"running_count":  runningCount,
		"bytes_in":       totalBytesIn,
		"bytes_out":      totalBytesOut,
	}
}

// GetPipelineCount returns the number of pipelines.
func (m *Manager) GetPipelineCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.pipelines)
}

func (m *Manager) pipeline(streamID string) (*Pipeline, error) {
	if err := core.ValidateStreamID(streamID); err != nil {
		return nil, err
	}
	m.mu.RLock()
	pipeline, exists := m.pipelines[streamID]
	m.mu.RUnlock()
	if !exists {
		return nil, ErrPipelineNotFound
	}
	return pipeline, nil
}

func pipelineSnapshot(pipeline *Pipeline) *Pipeline {
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()
	return &Pipeline{
		ID:        pipeline.ID,
		StreamID:  pipeline.StreamID,
		State:     pipeline.State,
		CreatedAt: pipeline.CreatedAt,
		BytesIn:   pipeline.BytesIn,
		BytesOut:  pipeline.BytesOut,
	}
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
