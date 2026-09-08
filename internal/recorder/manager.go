package recorder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

const (
	stateStarting  = "starting"
	stateRecording = "recording"
	stateStopped   = "stopped"
	stateFailed    = "failed"
	stateCancelled = "cancelled"
	maxFFmpegLog   = 64 * 1024
)

var (
	ErrInvalidStreamID = errors.New("invalid stream ID")
	ErrInvalidInputURL = errors.New("invalid recording input URL")
	ErrRecorderStopped = errors.New("recorder is stopped")
)

// Manager handles stream recording.
type Manager struct {
	config     *config.RecorderConfig
	ffmpegPath string
	logger     *zap.Logger
	recordings map[string]*Recording
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	stopped    bool
}

// Recording represents a recording and is returned as a snapshot.
type Recording struct {
	ID          string
	StreamID    string
	OutputPath  string
	StartTime   time.Time
	Duration    time.Duration
	Size        int64
	State       string // starting, recording, stopped, failed, cancelled
	cmd         *exec.Cmd
	process     *os.Process
	onComplete  func(string, error)
	stopRequest bool
}

// NewManager creates a new recorder manager.
func NewManager(cfg *config.RecorderConfig, logger *zap.Logger) (*Manager, error) {
	if cfg == nil {
		return nil, fmt.Errorf("recorder configuration is required")
	}
	copyCfg := *cfg
	if strings.TrimSpace(copyCfg.Path) == "" {
		return nil, fmt.Errorf("recordings path is required")
	}
	if strings.TrimSpace(copyCfg.FFmpegPath) == "" {
		copyCfg.FFmpegPath = "ffmpeg"
	}
	ffmpegPath, err := exec.LookPath(copyCfg.FFmpegPath)
	if err != nil {
		return nil, fmt.Errorf("ffmpeg not found at %q: %w", copyCfg.FFmpegPath, err)
	}
	root, err := filepath.Abs(copyCfg.Path)
	if err != nil {
		return nil, fmt.Errorf("resolve recordings path: %w", err)
	}
	copyCfg.Path = root
	if err := os.MkdirAll(copyCfg.Path, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create recordings directory: %w", err)
	}
	rootInfo, err := os.Lstat(copyCfg.Path)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("recordings path must be a regular directory")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		config:     &copyCfg,
		ffmpegPath: ffmpegPath,
		logger:     logger.Named("recorder"),
		recordings: make(map[string]*Recording),
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// StartRecording starts recording a stream.
func (m *Manager) StartRecording(streamID, inputURL string, onComplete func(string, error)) error {
	if err := core.ValidateStreamID(streamID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidStreamID, err)
	}
	if err := validateInputURL(inputURL); err != nil {
		return err
	}
	m.mu.Lock()
	if m.stopped || m.ctx == nil || m.ctx.Err() != nil {
		m.mu.Unlock()
		return ErrRecorderStopped
	}
	if existing, exists := m.recordings[streamID]; exists && isActive(existing.State) {
		m.mu.Unlock()
		return fmt.Errorf("recording already in progress for stream %s", streamID)
	}
	now := time.Now().UTC()
	filename := fmt.Sprintf("%s_%s.mp4", streamID, now.Format("20060102_150405.000000000"))
	outputPath := filepath.Join(m.config.Path, filename)
	rec := &Recording{
		ID:         fmt.Sprintf("%s_%d", streamID, now.UnixNano()),
		StreamID:   streamID,
		OutputPath: outputPath,
		StartTime:  now,
		State:      stateStarting,
		onComplete: onComplete,
	}
	// #nosec G204 -- direct exec does not invoke a shell; inputURL is restricted
	// to an explicit protocol allowlist and outputPath is manager-owned.
	cmd := exec.CommandContext(m.ctx, m.ffmpegPath,
		"-hide_banner", "-loglevel", "error", "-nostdin",
		"-i", inputURL,
		"-c:v", "copy", "-c:a", "copy", "-f", "mp4", outputPath,
	)
	cmd.Stdout = io.Discard
	stderr := &limitedBuffer{limit: maxFFmpegLog}
	cmd.Stderr = stderr
	rec.cmd = cmd
	m.recordings[streamID] = rec
	m.wg.Add(1)
	m.mu.Unlock()

	go m.runRecording(rec, stderr)
	return nil
}

func (m *Manager) runRecording(rec *Recording, stderr *limitedBuffer) {
	defer m.wg.Done()
	m.logger.Info("starting recording", zap.String("stream_id", rec.StreamID), zap.String("output", rec.OutputPath))

	if err := rec.cmd.Start(); err != nil {
		m.finishRecording(rec, err, stderr.String())
		return
	}
	m.mu.Lock()
	if current, exists := m.recordings[rec.StreamID]; exists && current == rec {
		rec.State = stateRecording
		rec.process = rec.cmd.Process
	}
	stopRequested := rec.stopRequest
	process := rec.process
	m.mu.Unlock()
	if stopRequested && process != nil {
		_ = process.Signal(os.Interrupt)
	}

	err := rec.cmd.Wait()
	m.finishRecording(rec, err, stderr.String())
}

func (m *Manager) finishRecording(rec *Recording, processErr error, ffmpegLog string) {
	if processErr == nil {
		if err := validateRecordingOutput(rec.OutputPath); err != nil {
			processErr = err
		}
	}
	m.mu.Lock()
	current, exists := m.recordings[rec.StreamID]
	if !exists || current != rec {
		m.mu.Unlock()
		return
	}
	rec.Duration = time.Since(rec.StartTime)
	if info, err := os.Stat(rec.OutputPath); err == nil && info.Mode().IsRegular() {
		rec.Size = info.Size()
	}
	stopRequested := rec.stopRequest
	callback := rec.onComplete
	rec.onComplete = nil
	rec.process = nil
	if processErr == nil || stopRequested {
		rec.State = stateStopped
	} else if errors.Is(processErr, context.Canceled) || errors.Is(m.ctx.Err(), context.Canceled) {
		rec.State = stateCancelled
	} else {
		rec.State = stateFailed
	}
	state := rec.State
	streamID := rec.StreamID
	outputPath := rec.OutputPath
	duration := rec.Duration
	size := rec.Size
	m.mu.Unlock()

	if processErr != nil {
		m.logger.Error("recording failed",
			zap.String("stream_id", streamID),
			zap.String("state", state),
			zap.Error(processErr),
			zap.String("ffmpeg_output", ffmpegLog),
		)
	} else {
		m.logger.Info("recording completed",
			zap.String("stream_id", streamID),
			zap.String("output", outputPath),
			zap.Duration("duration", duration),
			zap.Int64("size", size),
		)
	}
	if callback != nil {
		if processErr != nil {
			m.callCompletion(callback, "", processErr, streamID)
		} else {
			m.callCompletion(callback, outputPath, nil, streamID)
		}
	}
}

func validateRecordingOutput(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("recording output was not created: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("recording output is not a regular file")
	}
	if info.Size() == 0 {
		return fmt.Errorf("recording output is empty")
	}
	return nil
}

func (m *Manager) callCompletion(callback func(string, error), outputPath string, err error, streamID string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			m.logger.Error("recording completion callback panicked", zap.Any("panic", recovered), zap.String("stream_id", streamID))
		}
	}()
	callback(outputPath, err)
}

// StopRecording requests graceful termination of a stream recording.
func (m *Manager) StopRecording(streamID string) error {
	m.mu.Lock()
	rec, exists := m.recordings[streamID]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("no recording found for stream %s", streamID)
	}
	if !isActive(rec.State) {
		m.mu.Unlock()
		return nil
	}
	rec.stopRequest = true
	process := rec.process
	m.mu.Unlock()

	m.logger.Info("stopping recording", zap.String("stream_id", streamID))
	if process != nil {
		if err := process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return fmt.Errorf("interrupt recording: %w", err)
		}
	}
	return nil
}

// Stop requests all recordings to stop and waits for their processes.
func (m *Manager) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	if !m.stopped {
		m.stopped = true
	}
	active := make([]*os.Process, 0)
	for _, rec := range m.recordings {
		if isActive(rec.State) {
			rec.stopRequest = true
			if rec.process != nil {
				active = append(active, rec.process)
			}
		}
	}
	cancel := m.cancel
	m.mu.Unlock()
	for _, process := range active {
		_ = process.Signal(os.Interrupt)
	}
	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetRecording returns a snapshot by stream ID.
func (m *Manager) GetRecording(streamID string) (*Recording, bool) {
	m.mu.RLock()
	rec, exists := m.recordings[streamID]
	if !exists {
		m.mu.RUnlock()
		return nil, false
	}
	snapshot := recordingSnapshot(rec)
	m.mu.RUnlock()
	return snapshot, true
}

// GetRecordingCount returns the number of tracked stream recordings.
func (m *Manager) GetRecordingCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.recordings)
}

// ListRecordings returns deterministic snapshots of all tracked recordings.
func (m *Manager) ListRecordings() []*Recording {
	m.mu.RLock()
	result := make([]*Recording, 0, len(m.recordings))
	for _, rec := range m.recordings {
		result = append(result, recordingSnapshot(rec))
	}
	m.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// GetStats returns recorder statistics.
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.RLock()
	activeCount := 0
	var totalDuration time.Duration
	var totalSize int64
	for _, rec := range m.recordings {
		if isActive(rec.State) {
			activeCount++
		}
		totalDuration += rec.Duration
		if isActive(rec.State) {
			totalDuration += time.Since(rec.StartTime)
		}
		totalSize += rec.Size
	}
	outputPath := m.config.Path
	totalRecordings := len(m.recordings)
	m.mu.RUnlock()
	return map[string]interface{}{
		"active_recordings": activeCount,
		"total_recordings":  totalRecordings,
		"total_duration":    totalDuration.String(),
		"total_size_bytes":  totalSize,
		"output_path":       outputPath,
	}
}

// Cleanup removes old inactive recordings based on retention policy.
func (m *Manager) Cleanup(ctx context.Context, retentionDays int) {
	if ctx == nil || retentionDays <= 0 {
		return
	}
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
	if retentionDays <= 0 {
		return
	}
	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	m.mu.RLock()
	activePaths := make(map[string]struct{})
	for _, rec := range m.recordings {
		if isActive(rec.State) {
			activePaths[rec.OutputPath] = struct{}{}
		}
	}
	root := m.config.Path
	m.mu.RUnlock()

	entries, err := os.ReadDir(root)
	if err != nil {
		m.logger.Error("failed to read recordings directory", zap.Error(err))
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || !info.ModTime().Before(cutoff) {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if _, active := activePaths[path]; active {
			continue
		}
		if err := os.Remove(path); err == nil {
			m.logger.Info("removed old recording", zap.String("path", path), zap.Time("modified", info.ModTime()))
		}
	}
}

func isActive(state string) bool {
	return state == stateStarting || state == stateRecording
}

func recordingSnapshot(rec *Recording) *Recording {
	m := *rec
	if isActive(rec.State) {
		m.Duration = time.Since(rec.StartTime)
	}
	m.cmd = nil
	m.process = nil
	m.onComplete = nil
	return &m
}

func validateInputURL(input string) error {
	if strings.TrimSpace(input) != input || input == "" || strings.IndexFunc(input, func(r rune) bool {
		return r == '\x00' || r == '\r' || r == '\n' || r == '\t'
	}) >= 0 {
		return ErrInvalidInputURL
	}
	parsed, err := url.Parse(input)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ErrInvalidInputURL
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https", "rtmp", "rtmps", "rtsp", "rtsps", "srt":
		return nil
	default:
		return ErrInvalidInputURL
	}
}

type limitedBuffer struct {
	mu    sync.Mutex
	data  []byte
	limit int
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	originalLength := len(data)
	remaining := b.limit - len(b.data)
	if remaining > 0 {
		if len(data) > remaining {
			data = data[:remaining]
		}
		b.data = append(b.data, data...)
	}
	return originalLength, nil
}

func (b *limitedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(append([]byte(nil), b.data...))
}
