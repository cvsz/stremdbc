package dvr

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

// Manager manages DVR functionality
type Manager struct {
	config       *config.DVRConfig
	registry     *core.StreamRegistry
	logger       *zap.Logger
	outputPath   string
	mu           sync.Mutex
	sessions     map[string]*DVRSession
	maxDuration  time.Duration
}

// DVRSession represents a DVR recording session
type DVRSession struct {
	ID          string
	StreamID    string
	CreatedAt   time.Time
	Duration    time.Duration
	State       string // "recording", "paused", "stopped"
	OutputPath  string
	FileSize    int64
}

// DVRConfig holds DVR configuration
type DVRConfig struct {
	Enable      bool          `yaml:"enable"`
	Path        string        `yaml:"path"`
	MaxDuration time.Duration `yaml:"max_duration"`
	Format      string        `yaml:"format"`
}

// NewManager creates a new DVR manager
func NewManager(cfg *config.DVRConfig, registry *core.StreamRegistry, logger *zap.Logger) (*Manager, error) {
	outputPath := cfg.Path
	if outputPath == "" {
		outputPath = "/tmp/dvr"
	}

	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create DVR directory: %w", err)
	}

	maxDuration := 4 * time.Hour
	if cfg.MaxDuration > 0 {
		maxDuration = cfg.MaxDuration
	}

	return &Manager{
		config:      cfg,
		registry:    registry,
		logger:      logger.Named("dvr"),
		outputPath:  outputPath,
		sessions:    make(map[string]*DVRSession),
		maxDuration: maxDuration,
	}, nil
}

// Start starts the DVR manager
func (m *Manager) Start(ctx context.Context) error {
	m.logger.Info("DVR manager started",
		zap.String("output_path", m.outputPath),
		zap.Duration("max_duration", m.maxDuration),
	)
	return nil
}

// Stop stops the DVR manager
func (m *Manager) Stop() error {
	m.logger.Info("stopping DVR manager")

	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop all active sessions
	for _, session := range m.sessions {
		if session.State == "recording" {
			m.stopSession(session.ID)
		}
	}

	return nil
}

// StartRecording starts DVR recording for a stream
func (m *Manager) StartRecording(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sessions[streamID]; exists {
		return fmt.Errorf("DVR session already exists for stream")
	}

	streamPath := filepath.Join(m.outputPath, streamID)
	if err := os.MkdirAll(streamPath, 0755); err != nil {
		return fmt.Errorf("failed to create stream directory: %w", err)
	}

	session := &DVRSession{
		ID:         fmt.Sprintf("dvr_%s", streamID),
		StreamID:   streamID,
		CreatedAt:  time.Now(),
		State:      "recording",
		OutputPath: filepath.Join(streamPath, fmt.Sprintf("recording_%d.ts", time.Now().Unix())),
	}

	m.sessions[streamID] = session

	m.logger.Info("DVR recording started",
		zap.String("stream_id", streamID),
		zap.String("session_id", session.ID),
		zap.String("output_path", session.OutputPath),
	)

	return nil
}

// StopRecording stops DVR recording for a stream
func (m *Manager) StopRecording(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.stopSession(streamID)
}

func (m *Manager) stopSession(streamID string) error {
	session, exists := m.sessions[streamID]
	if !exists {
		return fmt.Errorf("DVR session not found")
	}

	session.State = "stopped"
	session.Duration = time.Since(session.CreatedAt)

	m.logger.Info("DVR recording stopped",
		zap.String("stream_id", streamID),
		zap.String("session_id", session.ID),
		zap.Duration("duration", session.Duration),
	)

	return nil
}

// PauseRecording pauses DVR recording
func (m *Manager) PauseRecording(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[streamID]
	if !exists {
		return fmt.Errorf("DVR session not found")
	}

	if session.State != "recording" {
		return fmt.Errorf("session is not recording")
	}

	session.State = "paused"
	m.logger.Info("DVR recording paused", zap.String("stream_id", streamID))

	return nil
}

// ResumeRecording resumes DVR recording
func (m *Manager) ResumeRecording(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[streamID]
	if !exists {
		return fmt.Errorf("DVR session not found")
	}

	if session.State != "paused" {
		return fmt.Errorf("session is not paused")
	}

	session.State = "recording"
	m.logger.Info("DVR recording resumed", zap.String("stream_id", streamID))

	return nil
}

// GetSession returns a DVR session by stream ID
func (m *Manager) GetSession(streamID string) (*DVRSession, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, exists := m.sessions[streamID]
	return session, exists
}

// GetSessionCount returns the number of active DVR sessions
func (m *Manager) GetSessionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, session := range m.sessions {
		if session.State == "recording" {
			count++
		}
	}
	return count
}

// GetStats returns DVR statistics
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	totalSessions := len(m.sessions)
	activeSessions := 0
	totalFileSize := int64(0)

	for _, session := range m.sessions {
		if session.State == "recording" {
			activeSessions++
		}
		totalFileSize += session.FileSize
	}

	return map[string]interface{}{
		"total_sessions":  totalSessions,
		"active_sessions": activeSessions,
		"total_file_size": totalFileSize,
		"output_path":     m.outputPath,
	}
}

// CleanupOldRecordings removes recordings older than maxDuration
func (m *Manager) CleanupOldRecordings() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-m.maxDuration)

	err := filepath.Walk(m.outputPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if info.ModTime().Before(cutoff) {
			if err := os.Remove(path); err != nil {
				m.logger.Error("failed to remove old recording",
					zap.String("path", path),
					zap.Error(err),
				)
			} else {
				m.logger.Info("removed old recording", zap.String("path", path))
			}
		}

		return nil
	})

	return err
}
