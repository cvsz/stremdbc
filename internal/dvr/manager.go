package dvr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

const (
	stateRecording = "recording"
	statePaused    = "paused"
	stateStopped   = "stopped"
)

var (
	ErrInvalidStreamID     = errors.New("invalid stream ID")
	ErrSessionNotFound     = errors.New("DVR session not found")
	ErrSessionNotRecording = errors.New("DVR session is not recording")
	ErrDVRStopped          = errors.New("DVR manager is stopped")
)

// Manager manages DVR functionality.
type Manager struct {
	config      *config.DVRConfig
	registry    *core.StreamRegistry
	logger      *zap.Logger
	outputPath  string
	mu          sync.Mutex
	sessions    map[string]*DVRSession
	maxDuration time.Duration
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	started     bool
	stopped     bool
}

// DVRSession represents a DVR recording session and is returned as a snapshot.
type DVRSession struct {
	ID         string
	StreamID   string
	CreatedAt  time.Time
	Duration   time.Duration
	State      string // recording, paused, stopped
	OutputPath string
	FileSize   int64

	file           *os.File
	activeSince    time.Time
	activeDuration time.Duration
	previousState  core.StreamState
}

// NewManager creates a new DVR manager.
func NewManager(cfg *config.DVRConfig, registry *core.StreamRegistry, logger *zap.Logger) (*Manager, error) {
	if cfg == nil {
		return nil, fmt.Errorf("DVR configuration is required")
	}
	copyCfg := *cfg
	if strings.TrimSpace(copyCfg.Path) == "" {
		copyCfg.Path = "/tmp/dvr"
	}
	if copyCfg.Format == "" {
		copyCfg.Format = "mpegts"
	}
	if copyCfg.Format != "mpegts" {
		return nil, fmt.Errorf("unsupported DVR format %q", copyCfg.Format)
	}
	root, err := filepath.Abs(copyCfg.Path)
	if err != nil {
		return nil, fmt.Errorf("resolve DVR path: %w", err)
	}
	copyCfg.Path = root
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create DVR directory: %w", err)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("DVR path must be a regular directory")
	}
	maxDuration := copyCfg.MaxDuration
	if maxDuration <= 0 {
		maxDuration = 4 * time.Hour
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if registry == nil {
		registry = core.NewStreamRegistry(nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		config:      &copyCfg,
		registry:    registry,
		logger:      logger.Named("dvr"),
		outputPath:  root,
		sessions:    make(map[string]*DVRSession),
		maxDuration: maxDuration,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Start starts the DVR cleanup loop. It is safe to call once; subsequent
// calls are idempotent while the manager is running.
func (m *Manager) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return ErrDVRStopped
	}
	if m.started {
		m.mu.Unlock()
		return nil
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.started = true
	m.wg.Add(1)
	m.mu.Unlock()

	m.logger.Info("DVR manager started", zap.String("output_path", m.outputPath), zap.Duration("max_duration", m.maxDuration))
	go m.cleanupLoop()
	return nil
}

func (m *Manager) cleanupLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			if err := m.CleanupOldRecordings(); err != nil {
				m.logger.Error("DVR cleanup failed", zap.Error(err))
			}
		}
	}
}

// Stop stops cleanup and closes all active DVR files.
func (m *Manager) Stop() error {
	m.logger.Info("stopping DVR manager")
	m.mu.Lock()
	if !m.stopped {
		m.stopped = true
	}
	var stopErr error
	for streamID, session := range m.sessions {
		if session.State == stateRecording || session.State == statePaused {
			if err := m.stopSessionLocked(streamID); err != nil {
				m.logger.Error("failed to stop DVR session", zap.String("stream_id", streamID), zap.Error(err))
				stopErr = errors.Join(stopErr, err)
			}
		}
	}
	cancel := m.cancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
	return stopErr
}

// StartRecording starts a file-backed DVR session for a registered stream.
func (m *Manager) StartRecording(streamID string) error {
	if err := core.ValidateStreamID(streamID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidStreamID, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return ErrDVRStopped
	}
	if existing, exists := m.sessions[streamID]; exists && existing.State != stateStopped {
		return fmt.Errorf("DVR session already exists for stream %s", streamID)
	}
	stream, exists := m.registry.Get(streamID)
	if !exists {
		return core.ErrStreamNotFound
	}

	streamPath, err := safeStreamDirectory(m.outputPath, streamID)
	if err != nil {
		return fmt.Errorf("failed to create stream directory: %w", err)
	}
	now := time.Now().UTC()
	filename := fmt.Sprintf("recording_%d.ts", now.UnixNano())
	outputPath := filepath.Join(streamPath, filename)
	rootHandle, err := os.OpenRoot(m.outputPath)
	if err != nil {
		return fmt.Errorf("open DVR output root: %w", err)
	}
	defer rootHandle.Close()
	file, err := rootHandle.OpenFile(filepath.Join(streamID, filename), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create DVR recording: %w", err)
	}
	session := &DVRSession{
		ID:            fmt.Sprintf("dvr_%s_%d", streamID, now.UnixNano()),
		StreamID:      streamID,
		CreatedAt:     now,
		State:         stateRecording,
		OutputPath:    outputPath,
		file:          file,
		activeSince:   now,
		previousState: stream.State,
	}
	if err := m.registry.SetState(streamID, core.StreamStateRecording); err != nil {
		_ = file.Close()
		_ = rootHandle.Remove(filepath.Join(streamID, filename))
		return fmt.Errorf("set stream recording: %w", err)
	}
	m.sessions[streamID] = session
	m.logger.Info("DVR recording started", zap.String("stream_id", streamID), zap.String("session_id", session.ID), zap.String("output_path", outputPath))
	return nil
}

// Write appends media bytes to an active DVR session.
func (m *Manager) Write(streamID string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, exists := m.sessions[streamID]
	if !exists {
		return ErrSessionNotFound
	}
	if session.State != stateRecording || session.file == nil {
		return ErrSessionNotRecording
	}
	n, err := session.file.Write(data)
	if err != nil {
		return fmt.Errorf("write DVR recording: %w", err)
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	session.FileSize += int64(n)
	return nil
}

// StopRecording stops a DVR recording.
func (m *Manager) StopRecording(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopSessionLocked(streamID)
}

func (m *Manager) stopSessionLocked(streamID string) error {
	session, exists := m.sessions[streamID]
	if !exists {
		return ErrSessionNotFound
	}
	if session.State == stateStopped {
		return nil
	}
	m.updateDurationLocked(session, time.Now())
	var closeErr error
	if session.file != nil {
		closeErr = errors.Join(session.file.Sync(), session.file.Close())
		session.file = nil
	}
	session.State = stateStopped
	if current, exists := m.registry.Get(streamID); exists && current.State == core.StreamStateRecording {
		if err := m.registry.SetState(streamID, session.previousState); err != nil && !errors.Is(err, core.ErrStreamNotFound) {
			return errors.Join(closeErr, err)
		}
	}
	m.logger.Info("DVR recording stopped", zap.String("stream_id", streamID), zap.String("session_id", session.ID), zap.Duration("duration", session.Duration))
	return closeErr
}

// PauseRecording pauses a DVR recording and excludes paused time from duration.
func (m *Manager) PauseRecording(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, exists := m.sessions[streamID]
	if !exists {
		return ErrSessionNotFound
	}
	if session.State != stateRecording {
		return fmt.Errorf("session is not recording")
	}
	m.updateDurationLocked(session, time.Now())
	session.State = statePaused
	session.activeSince = time.Time{}
	m.logger.Info("DVR recording paused", zap.String("stream_id", streamID))
	return nil
}

// ResumeRecording resumes a paused DVR recording.
func (m *Manager) ResumeRecording(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, exists := m.sessions[streamID]
	if !exists {
		return ErrSessionNotFound
	}
	if session.State != statePaused {
		return fmt.Errorf("session is not paused")
	}
	session.State = stateRecording
	session.activeSince = time.Now()
	m.logger.Info("DVR recording resumed", zap.String("stream_id", streamID))
	return nil
}

// GetSession returns a snapshot by stream ID.
func (m *Manager) GetSession(streamID string) (*DVRSession, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, exists := m.sessions[streamID]
	if !exists {
		return nil, false
	}
	copy := sessionSnapshot(session)
	if session.State == stateRecording {
		copy.Duration = session.activeDuration + time.Since(session.activeSince)
	}
	return copy, true
}

func (m *Manager) GetSessionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, session := range m.sessions {
		if session.State == stateRecording || session.State == statePaused {
			count++
		}
	}
	return count
}

func (m *Manager) GetStats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	activeSessions := 0
	var totalFileSize int64
	for _, session := range m.sessions {
		if session.State == stateRecording || session.State == statePaused {
			activeSessions++
		}
		totalFileSize += session.FileSize
	}
	return map[string]interface{}{
		"total_sessions":  len(m.sessions),
		"active_sessions": activeSessions,
		"total_file_size": totalFileSize,
		"output_path":     m.outputPath,
	}
}

// CleanupOldRecordings removes old regular files without touching active files.
func (m *Manager) CleanupOldRecordings() error {
	m.mu.Lock()
	activePaths := make(map[string]struct{})
	for _, session := range m.sessions {
		if session.State == stateRecording || session.State == statePaused {
			activePaths[session.OutputPath] = struct{}{}
		}
	}
	root := m.outputPath
	cutoff := time.Now().Add(-m.maxDuration)
	m.mu.Unlock()

	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("open DVR output root: %w", err)
	}
	defer rootHandle.Close()
	return fs.WalkDir(rootHandle.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == "." || entry.IsDir() || entry.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || !info.ModTime().Before(cutoff) {
			return nil
		}
		absolutePath := filepath.Join(root, filepath.FromSlash(path))
		if _, active := activePaths[absolutePath]; active {
			return nil
		}
		if err := rootHandle.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove old DVR recording %s: %w", absolutePath, err)
		}
		return nil
	})
}

func (m *Manager) updateDurationLocked(session *DVRSession, now time.Time) {
	if session.State == stateRecording && !session.activeSince.IsZero() {
		session.activeDuration += now.Sub(session.activeSince)
	}
	session.Duration = session.activeDuration
}

func sessionSnapshot(session *DVRSession) *DVRSession {
	copy := *session
	copy.file = nil
	copy.activeSince = time.Time{}
	return &copy
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
