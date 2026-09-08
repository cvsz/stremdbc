package hls

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
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
	if cfg == nil {
		return nil, fmt.Errorf("HLS configuration is required")
	}
	if strings.TrimSpace(cfg.Path) == "" {
		return nil, fmt.Errorf("HLS path cannot be empty")
	}
	if cfg.SegmentDuration <= 0 || cfg.PlaylistSize <= 0 {
		return nil, fmt.Errorf("HLS segment duration and playlist size must be positive")
	}
	if cfg.PlaylistSize > config.MaxPlaylistSize {
		return nil, fmt.Errorf("HLS playlist size must not exceed %d", config.MaxPlaylistSize)
	}
	copyCfg := *cfg
	root, err := filepath.Abs(copyCfg.Path)
	if err != nil {
		return nil, fmt.Errorf("resolve HLS path: %w", err)
	}
	copyCfg.Path = root
	if logger == nil {
		logger = zap.NewNop()
	}
	// Ensure output directory exists
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create HLS directory: %w", err)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("HLS path must be a regular directory")
	}

	return &OutputManager{
		config:      &copyCfg,
		logger:      logger.Named("hls"),
		outputPaths: make(map[string]string),
	}, nil
}

// Start starts HLS output for a stream
func (m *OutputManager) Start(streamID string) (string, error) {
	if err := core.ValidateStreamID(streamID); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.config.Enable {
		return "", fmt.Errorf("HLS output is disabled")
	}

	if _, exists := m.outputPaths[streamID]; exists {
		return "", fmt.Errorf("HLS output already started for stream %s", streamID)
	}

	streamPath, err := safeStreamDirectory(m.config.Path, streamID)
	if err != nil {
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

	path, exists := m.outputPaths[streamID]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("HLS output not found for stream %s", streamID)
	}

	delete(m.outputPaths, streamID)
	m.mu.Unlock()
	if err := removeStreamOutput(m.config.Path, path); err != nil {
		return fmt.Errorf("remove HLS output for stream %s: %w", streamID, err)
	}
	m.logger.Info("HLS output stopped",
		zap.String("stream_id", streamID),
		zap.String("path", path),
	)

	return nil
}

func removeStreamOutput(root, path string) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return fmt.Errorf("output path escapes root")
	}
	return os.RemoveAll(pathAbs)
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
	if err := core.ValidateStreamID(streamID); err != nil {
		return err
	}
	if sequence < 0 {
		return fmt.Errorf("segment sequence must not be negative")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	path, exists := m.outputPaths[streamID]
	if !exists {
		return fmt.Errorf("HLS output not found for stream %s", streamID)
	}

	filename := filepath.Join(path, fmt.Sprintf("segment_%d.ts", sequence))
	return atomicWriteFile(filename, data, 0o640)
}

// WritePlaylist writes an HLS playlist file
func (m *OutputManager) WritePlaylist(streamID string, segments []int) error {
	return m.writePlaylist(streamID, segments, false)
}

// WriteFinalPlaylist writes a completed HLS playlist with an ENDLIST marker.
func (m *OutputManager) WriteFinalPlaylist(streamID string, segments []int) error {
	return m.writePlaylist(streamID, segments, true)
}

func (m *OutputManager) writePlaylist(streamID string, segments []int, final bool) error {
	if err := core.ValidateStreamID(streamID); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	path, exists := m.outputPaths[streamID]
	if !exists {
		return fmt.Errorf("HLS output not found for stream %s", streamID)
	}
	for _, seq := range segments {
		if seq < 0 {
			return fmt.Errorf("segment sequence must not be negative")
		}
	}
	normalized := normalizeSegments(segments, m.config.PlaylistSize)

	targetDuration := int(math.Ceil(m.config.SegmentDuration.Seconds()))
	if targetDuration < 1 {
		targetDuration = 1
	}
	mediaSequence := 0
	if len(normalized) > 0 {
		mediaSequence = normalized[0]
	}
	var playlist strings.Builder
	playlist.WriteString("#EXTM3U\n")
	playlist.WriteString("#EXT-X-VERSION:3\n")
	playlist.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", targetDuration))
	playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n\n", mediaSequence))

	for _, seq := range normalized {
		playlist.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", m.config.SegmentDuration.Seconds()))
		playlist.WriteString(fmt.Sprintf("segment_%d.ts\n", seq))
	}

	if final {
		playlist.WriteString("#EXT-X-ENDLIST\n")
	}

	playlistPath := filepath.Join(path, "index.m3u8")
	return atomicWriteFile(playlistPath, []byte(playlist.String()), 0o640)
}

// Cleanup removes old segments based on playlist size
func (m *OutputManager) Cleanup(ctx context.Context, interval time.Duration) {
	if ctx == nil || interval <= 0 {
		return
	}
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

func normalizeSegments(segments []int, limit int) []int {
	seen := make(map[int]struct{}, len(segments))
	normalized := make([]int, 0, len(segments))
	for _, sequence := range segments {
		if _, exists := seen[sequence]; exists {
			continue
		}
		seen[sequence] = struct{}{}
		normalized = append(normalized, sequence)
	}
	sort.Ints(normalized)
	if limit > 0 && len(normalized) > limit {
		normalized = normalized[len(normalized)-limit:]
	}
	return normalized
}

func (m *OutputManager) cleanupOldSegments() {
	m.mu.RLock()
	paths := make([]string, 0, len(m.outputPaths))
	for _, path := range m.outputPaths {
		paths = append(paths, path)
	}
	m.mu.RUnlock()

	for _, path := range paths {
		entries, err := os.ReadDir(path)
		if err != nil {
			m.logger.Warn("failed to read HLS stream directory", zap.String("path", path), zap.Error(err))
			continue
		}
		sequences := make([]int, 0, len(entries))
		files := make(map[int]string, len(entries))
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if !strings.HasPrefix(name, "segment_") || !strings.HasSuffix(name, ".ts") {
				continue
			}
			sequence, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "segment_"), ".ts"))
			if err != nil || sequence < 0 {
				continue
			}
			sequences = append(sequences, sequence)
			files[sequence] = filepath.Join(path, name)
		}
		sort.Ints(sequences)
		keepFrom := len(sequences) - m.config.PlaylistSize
		if keepFrom < 0 {
			keepFrom = 0
		}
		for _, sequence := range sequences[:keepFrom] {
			if err := os.Remove(files[sequence]); err != nil && !os.IsNotExist(err) {
				m.logger.Warn("failed to remove old HLS segment", zap.String("path", files[sequence]), zap.Error(err))
			}
		}
	}
}

// GetStats returns statistics about HLS output
func (m *OutputManager) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return map[string]interface{}{
		"active_streams":   len(m.outputPaths),
		"output_path":      m.config.Path,
		"segment_duration": m.config.SegmentDuration.Seconds(),
		"playlist_size":    m.config.PlaylistSize,
	}
}

func (m *OutputManager) GetOutputPath(streamID string) (string, bool) {
	return m.GetPath(streamID)
}

func atomicWriteFile(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".stremdbc-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	n, err := tmp.Write(data)
	if err != nil {
		_ = tmp.Close()
		return err
	}
	if n != len(data) {
		_ = tmp.Close()
		return io.ErrShortWrite
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func safeStreamDirectory(root, streamID string) (string, error) {
	streamPath := filepath.Join(root, streamID)
	if err := os.MkdirAll(streamPath, 0o750); err != nil {
		return "", err
	}
	info, err := os.Lstat(streamPath)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("stream directory is a symlink")
	}
	rootResolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	streamResolved, err := filepath.EvalSymlinks(streamPath)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(rootResolved, streamResolved)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("stream directory escapes output root")
	}
	return streamPath, nil
}
