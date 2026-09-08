package media

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

// Pipeline represents a media processing pipeline
type Pipeline struct {
	ID          string
	StreamID    string
	Input       io.Reader
	Output      io.Writer
	State       string
	CreatedAt   time.Time
	BytesIn     int64
	BytesOut    int64
	mu          sync.Mutex
}

// Manager manages media pipelines
type Manager struct {
	logger    *zap.Logger
	registry  *core.StreamRegistry
	mu        sync.Mutex
	pipelines map[string]*Pipeline
}

// NewManager creates a new media pipeline manager
func NewManager(registry *core.StreamRegistry, logger *zap.Logger) *Manager {
	return &Manager{
		logger:    logger.Named("media"),
		registry:  registry,
		pipelines: make(map[string]*Pipeline),
	}
}

// CreatePipeline creates a new media pipeline for a stream
func (m *Manager) CreatePipeline(streamID string) (*Pipeline, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.pipelines[streamID]; exists {
		return nil, fmt.Errorf("pipeline already exists for stream")
	}

	pipeline := &Pipeline{
		ID:        fmt.Sprintf("pipeline_%s", streamID),
		StreamID:  streamID,
		State:     "created",
		CreatedAt: time.Now(),
	}

	m.pipelines[streamID] = pipeline
	m.logger.Info("media pipeline created", zap.String("stream_id", streamID))

	return pipeline, nil
}

// RemovePipeline removes a media pipeline
func (m *Manager) RemovePipeline(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.pipelines, streamID)
	m.logger.Info("media pipeline removed", zap.String("stream_id", streamID))
	return nil
}

// GetPipeline returns a pipeline by stream ID
func (m *Manager) GetPipeline(streamID string) (*Pipeline, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pipeline, exists := m.pipelines[streamID]
	return pipeline, exists
}

// StartPipeline starts a media pipeline
func (m *Manager) StartPipeline(ctx context.Context, streamID string) error {
	m.mu.Lock()
	pipeline, exists := m.pipelines[streamID]
	m.mu.Unlock()

	if !exists {
		return fmt.Errorf("pipeline not found")
	}

	pipeline.mu.Lock()
	pipeline.State = "running"
	pipeline.mu.Unlock()

	m.logger.Info("media pipeline started", zap.String("stream_id", streamID))

	// Update stream state in registry
	_ = m.registry.SetState(streamID, core.StreamStateLive)

	return nil
}

// StopPipeline stops a media pipeline
func (m *Manager) StopPipeline(streamID string) error {
	m.mu.Lock()
	pipeline, exists := m.pipelines[streamID]
	m.mu.Unlock()

	if !exists {
		return fmt.Errorf("pipeline not found")
	}

	pipeline.mu.Lock()
	pipeline.State = "stopped"
	pipeline.mu.Unlock()

	m.logger.Info("media pipeline stopped", zap.String("stream_id", streamID))
	return nil
}

// Process processes media data through the pipeline
func (m *Manager) Process(ctx context.Context, streamID string, data []byte) error {
	m.mu.Lock()
	pipeline, exists := m.pipelines[streamID]
	m.mu.Unlock()

	if !exists {
		return fmt.Errorf("pipeline not found")
	}

	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()

	if pipeline.State != "running" {
		return fmt.Errorf("pipeline is not running")
	}

	// Process data (simplified - actual implementation would use GStreamer/FFmpeg)
	pipeline.BytesIn += int64(len(data))

	// Forward to output if available
	if pipeline.Output != nil {
		n, err := pipeline.Output.Write(data)
		if err != nil {
			return err
		}
		pipeline.BytesOut += int64(n)
	}

	return nil
}

// GetStats returns pipeline statistics
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	totalBytesIn := int64(0)
	totalBytesOut := int64(0)
	runningCount := 0

	for _, pipeline := range m.pipelines {
		pipeline.mu.Lock()
		totalBytesIn += pipeline.BytesIn
		totalBytesOut += pipeline.BytesOut
		if pipeline.State == "running" {
			runningCount++
		}
		pipeline.mu.Unlock()
	}

	return map[string]interface{}{
		"pipeline_count": len(m.pipelines),
		"running_count":  runningCount,
		"bytes_in":       totalBytesIn,
		"bytes_out":      totalBytesOut,
	}
}

// GetPipelineCount returns the number of pipelines
func (m *Manager) GetPipelineCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pipelines)
}
