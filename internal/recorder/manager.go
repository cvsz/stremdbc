package recorder

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

// Manager handles stream recording
type Manager struct {
	config     *config.RecorderConfig
	logger     *zap.Logger
	recordings map[string]*Recording
	mu         sync.Mutex
}

// Recording represents an active recording
type Recording struct {
	ID          string
	StreamID    string
	OutputPath  string
	StartTime   time.Time
	Duration    time.Duration
	Size        int64
	State       string // "recording", "stopped", "failed"
	cmd         *exec.Cmd
	onComplete  func(string, error)
}

// NewManager creates a new recorder manager
func NewManager(cfg *config.RecorderConfig, logger *zap.Logger) (*Manager, error) {
	if err := os.MkdirAll(cfg.Path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create recordings directory: %w", err)
	}

	return &Manager{
		config:     cfg,
		logger:     logger.Named("recorder"),
		recordings: make(map[string]*Recording),
	}, nil
}

// StartRecording starts recording a stream
func (m *Manager) StartRecording(streamID, inputURL string, onComplete func(string, error)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.recordings[streamID]; exists {
		return fmt.Errorf("recording already in progress for stream %s", streamID)
	}

	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.mp4", streamID, timestamp)
	outputPath := filepath.Join(m.config.Path, filename)

	rec := &Recording{
		ID:         fmt.Sprintf("%s_%d", streamID, time.Now().Unix()),
		StreamID:   streamID,
		OutputPath: outputPath,
		StartTime:  time.Now(),
		State:      "starting",
		onComplete: onComplete,
	}

	// Build FFmpeg command
	args := []string{
		"-i", inputURL,
		"-c:v", "copy",
		"-c:a", "copy",
		"-f", "mp4",
		outputPath,
	}

	cmd := exec.CommandContext(context.Background(), "ffmpeg", args...)
	rec.cmd = cmd

	go func() {
		m.logger.Info("starting recording",
			zap.String("stream_id", streamID),
			zap.String("output", outputPath),
		)

		output, err := cmd.CombinedOutput()
		
		m.mu.Lock()
		rec.State = "stopped"
		rec.Duration = time.Since(rec.StartTime)
		
		// Get file size
		if info, statErr := os.Stat(outputPath); statErr == nil {
			rec.Size = info.Size()
		}
		m.mu.Unlock()

		if err != nil {
			m.logger.Error("recording failed",
				zap.String("stream_id", streamID),
				zap.Error(err),
				zap.String("output", string(output)),
			)
			rec.State = "failed"
			
			if rec.onComplete != nil {
				rec.onComplete("", err)
			}
			return
		}

		m.logger.Info("recording completed",
			zap.String("stream_id", streamID),
			zap.String("output", outputPath),
			zap.Duration("duration", rec.Duration),
			zap.Int64("size", rec.Size),
		)

		if rec.onComplete != nil {
			rec.onComplete(outputPath, nil)
		}
	}()

	m.recordings[streamID] = rec
	return nil
}

// StopRecording stops recording a stream
func (m *Manager) StopRecording(streamID string) error {
	m.mu.Lock()
	rec, exists := m.recordings[streamID]
	m.mu.Unlock()

	if !exists {
		return fmt.Errorf("no recording found for stream %s", streamID)
	}

	m.logger.Info("stopping recording", zap.String("stream_id", streamID))

	// Send SIGINT to gracefully stop FFmpeg
	if rec.cmd != nil && rec.cmd.Process != nil {
		rec.cmd.Process.Signal(os.Interrupt)
	}

	return nil
}

// GetRecording returns a recording by stream ID
func (m *Manager) GetRecording(streamID string) (*Recording, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, exists := m.recordings[streamID]
	return rec, exists
}

// ListRecordings returns all recordings
func (m *Manager) ListRecordings() []*Recording {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*Recording, 0, len(m.recordings))
	for _, rec := range m.recordings {
		result = append(result, rec)
	}
	return result
}

// GetStats returns recorder statistics
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	activeCount := 0
	totalDuration := time.Duration(0)
	totalSize := int64(0)

	for _, rec := range m.recordings {
		if rec.State == "recording" || rec.State == "starting" {
			activeCount++
		}
		totalDuration += rec.Duration
		totalSize += rec.Size
	}

	return map[string]interface{}{
		"active_recordings": activeCount,
		"total_recordings":  len(m.recordings),
		"total_duration":    totalDuration.String(),
		"total_size_bytes":  totalSize,
		"output_path":       m.config.Path,
	}
}

// Cleanup removes old recordings based on retention policy
func (m *Manager) Cleanup(ctx context.Context, retentionDays int) {
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.cleanupOldRecordings(retentionDays)
			}
		}
	}()
}

func (m *Manager) cleanupOldRecordings(retentionDays int) {
	cutoff := time.Now().AddDate(0, 0, -retentionDays)

	entries, err := os.ReadDir(m.config.Path)
	if err != nil {
		m.logger.Error("failed to read recordings directory", zap.Error(err))
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			path := filepath.Join(m.config.Path, entry.Name())
			if err := os.Remove(path); err == nil {
				m.logger.Info("removed old recording",
					zap.String("path", path),
					zap.Time("modified", info.ModTime()),
				)
			}
		}
	}
}
