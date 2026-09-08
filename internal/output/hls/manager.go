package hls

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

// OutputManager manages HLS output for streams
type OutputManager struct {
	config      *config.HLSConfig
	logger      *zap.Logger
	outputPaths map[string]string // stream ID -> output path
	mu          sync.RWMutex
}

// NewOutputManager creates a new HLS output manager
func NewOutputManager(cfg *config.HLSConfig, logger *zap.Logger) (*OutputManager, error) {
	// Ensure output directory exists
	if err := os.MkdirAll(cfg.Path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create HLS directory: %w", err)
	}

	return &OutputManager{
		config:      cfg,
		logger:      logger.Named("hls"),
		outputPaths: make(map[string]string),
	}, nil
}

// Start starts HLS output for a stream
func (m *OutputManager) Start(streamID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.outputPaths[streamID]; exists {
		return "", fmt.Errorf("HLS output already started for stream %s", streamID)
	}

	streamPath := filepath.Join(m.config.Path, streamID)
	if err := os.MkdirAll(streamPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create stream directory: %w", err)
	}

	m.outputPaths[streamID] = streamPath
	m.logger.Info("HLS output started",
		zap.String("stream_id", streamID),
		zap.String("path", streamPath),
	)

	return streamPath, nil
}

// Stop stops HLS output for a stream
func (m *OutputManager) Stop(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	path, exists := m.outputPaths[streamID]
	if !exists {
		return fmt.Errorf("HLS output not found for stream %s", streamID)
	}

	// Clean up HLS files (optional - could keep for VOD)
	// For now, we'll leave them for potential VOD functionality

	delete(m.outputPaths, streamID)
	m.logger.Info("HLS output stopped",
		zap.String("stream_id", streamID),
		zap.String("path", path),
	)

	return nil
}

// GetPath returns the HLS output path for a stream
func (m *OutputManager) GetPath(streamID string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	path, exists := m.outputPaths[streamID]
	return path, exists
}

// WriteSegment writes a media segment to disk
func (m *OutputManager) WriteSegment(streamID string, sequence int, data []byte) error {
	m.mu.RLock()
	path, exists := m.outputPaths[streamID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("HLS output not found for stream %s", streamID)
	}

	filename := filepath.Join(path, fmt.Sprintf("segment_%d.ts", sequence))
	return os.WriteFile(filename, data, 0644)
}

// WritePlaylist writes an HLS playlist file
func (m *OutputManager) WritePlaylist(streamID string, segments []int) error {
	m.mu.RLock()
	path, exists := m.outputPaths[streamID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("HLS output not found for stream %s", streamID)
	}

	playlist := "#EXTM3U\n"
	playlist += "#EXT-X-VERSION:3\n"
	playlist += "#EXT-X-TARGETDURATION:4\n"
	playlist += "#EXT-X-MEDIA-SEQUENCE:0\n"
	playlist += "#EXT-X-PLAYLIST-TYPE:EVENT\n"
	playlist += "\n"

	for _, seq := range segments {
		playlist += fmt.Sprintf("#EXTINF:%.3f,\n", m.config.SegmentDuration.Seconds())
		playlist += fmt.Sprintf("segment_%d.ts\n", seq)
	}

	playlist += "#EXT-X-ENDLIST\n"

	playlistPath := filepath.Join(path, "index.m3u8")
	return os.WriteFile(playlistPath, []byte(playlist), 0644)
}

// Cleanup removes old segments based on playlist size
func (m *OutputManager) Cleanup(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.cleanupOldSegments()
			}
		}
	}()
}

func (m *OutputManager) cleanupOldSegments() {
	// Implement cleanup logic for old segments
	// Keep only the last N segments based on config.PlaylistSize
	m.logger.Debug("cleanup old segments")
}

// GetStats returns statistics about HLS output
func (m *OutputManager) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return map[string]interface{}{
		"active_streams": len(m.outputPaths),
		"output_path":    m.config.Path,
		"segment_duration": m.config.SegmentDuration.Seconds(),
		"playlist_size":  m.config.PlaylistSize,
	}
}
