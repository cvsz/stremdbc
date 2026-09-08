package llhls

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

// Manager manages Low-Latency HLS output
type Manager struct {
	config        *config.LLHLSConfig
	registry      *core.StreamRegistry
	logger        *zap.Logger
	outputPath    string
	mu            sync.Mutex
	streams       map[string]*LLHLSStream
	partDuration  time.Duration
	segmentDur    time.Duration
}

// LLHLSStream represents a low-latency HLS stream
type LLHLSStream struct {
	ID           string
	CreatedAt    time.Time
	LastSegment  time.Time
	SegmentCount int
	PartCount    int
	State        string
}

// NewManager creates a new LL-HLS manager
func NewManager(cfg *config.LLHLSConfig, registry *core.StreamRegistry, logger *zap.Logger) (*Manager, error) {
	if !cfg.Enable {
		return &Manager{
			config:   cfg,
			registry: registry,
			logger:   logger.Named("llhls"),
			streams:  make(map[string]*LLHLSStream),
		}, nil
	}

	outputPath := "/tmp/llhls"
	if cfg.SegmentDuration > 0 {
		outputPath = filepath.Join(outputPath, fmt.Sprintf("%d", int(cfg.SegmentDuration.Seconds())))
	}

	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create LL-HLS output directory: %w", err)
	}

	return &Manager{
		config:       cfg,
		registry:     registry,
		logger:       logger.Named("llhls"),
		outputPath:   outputPath,
		streams:      make(map[string]*LLHLSStream),
		partDuration: cfg.PartDuration,
		segmentDur:   cfg.SegmentDuration,
	}, nil
}

// Start starts the LL-HLS manager
func (m *Manager) Start(ctx context.Context) error {
	m.logger.Info("LL-HLS manager started",
		zap.String("output_path", m.outputPath),
		zap.Duration("segment_duration", m.segmentDur),
		zap.Duration("part_duration", m.partDuration),
	)
	return nil
}

// Stop stops the LL-HLS manager
func (m *Manager) Stop() error {
	m.logger.Info("stopping LL-HLS manager")
	return nil
}

// CreateStream creates a new LL-HLS stream
func (m *Manager) CreateStream(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.streams[streamID]; exists {
		return fmt.Errorf("stream already exists")
	}

	streamPath := filepath.Join(m.outputPath, streamID)
	if err := os.MkdirAll(streamPath, 0755); err != nil {
		return fmt.Errorf("failed to create stream directory: %w", err)
	}

	m.streams[streamID] = &LLHLSStream{
		ID:          streamID,
		CreatedAt:   time.Now(),
		State:       "active",
		SegmentCount: 0,
		PartCount:   0,
	}

	m.logger.Info("LL-HLS stream created",
		zap.String("stream_id", streamID),
		zap.String("path", streamPath),
	)

	return nil
}

// RemoveStream removes an LL-HLS stream
func (m *Manager) RemoveStream(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.streams, streamID)

	streamPath := filepath.Join(m.outputPath, streamID)
	if err := os.RemoveAll(streamPath); err != nil {
		m.logger.Error("failed to remove stream directory",
			zap.String("stream_id", streamID),
			zap.Error(err),
		)
	}

	m.logger.Info("LL-HLS stream removed", zap.String("stream_id", streamID))
	return nil
}

// AddSegment adds a segment to an LL-HLS stream
func (m *Manager) AddSegment(streamID string, data []byte) error {
	m.mu.Lock()
	stream, exists := m.streams[streamID]
	m.mu.Unlock()

	if !exists {
		return fmt.Errorf("stream not found")
	}

	stream.LastSegment = time.Now()
	stream.SegmentCount++

	// Write segment file
	segmentPath := filepath.Join(m.outputPath, streamID, fmt.Sprintf("segment_%d.ts", stream.SegmentCount))
	if err := os.WriteFile(segmentPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write segment: %w", err)
	}

	return nil
}

// GetStream returns an LL-HLS stream by ID
func (m *Manager) GetStream(streamID string) (*LLHLSStream, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stream, exists := m.streams[streamID]
	return stream, exists
}

// GetStreamCount returns the number of active LL-HLS streams
func (m *Manager) GetStreamCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.streams)
}

// GetOutputPath returns the LL-HLS output path
func (m *Manager) GetOutputPath() string {
	return m.outputPath
}

// GetStats returns LL-HLS statistics
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	totalSegments := 0
	totalParts := 0
	for _, stream := range m.streams {
		totalSegments += stream.SegmentCount
		totalParts += stream.PartCount
	}

	return map[string]interface{}{
		"stream_count":   len(m.streams),
		"total_segments": totalSegments,
		"total_parts":    totalParts,
		"output_path":    m.outputPath,
	}
}
