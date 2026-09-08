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

// Manager manages DVR functionality.
type Manager struct {
	config      *config.DVRConfig
	registry    *core.StreamRegistry
	logger      *zap.Logger
	outputPath  string
	mu          sync.Mutex
	sessions    map[string]*DVRSession
	maxDuration time.Duration
}

// DVRSession represents a DVR recording session.
type DVRSession struct {
	ID         string
	StreamID   string
	CreatedAt  time.Time
	Duration   time.Duration
	State      string // recording, paused, stopped
	OutputPath string
	FileSize   int64
}

// NewManager creates a new DVR manager.
func NewManager(cfg *config.DVRConfig, registry *core.StreamRegistry, logger *zap.Logger) (*Manager, error) {
	outputPath := cfg.Path
	if outputPath == "" {
		outputPath = "/tmp/dvr"
	}

	if err := os.MkdirAll(outputPath, 0o755); err != nil {
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

func (m *Manager) Start(ctx context.Context) error {
	m.logger.Info("DVR manager started",
		zap.String("output_path", m.outputPath),
		zap.Duration("max_duration", m.maxDuration),
	)
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := m.CleanupOldRecordings(); err != nil {
					m.logger.Error("DVR cleanup failed", zap.Error(err))
				}
			}
		}
	}()
	return nil
}

func (m *Manager) Stop() error {
	m.logger.Info("stopping DVR manager")
	m.mu.Lock()
	defer m.mu.Unlock()

	for streamID, session := range m.sessions {
		if session.State == "recording" || session.State == "paused" {
			if err := m.stopSessionLocked(streamID); err != nil {
				m.logger.Error("failed to stop DVR session", zap.String("stream_id", streamID), zap.Error(err))
			}
		}
	}
	return nil
}

func (m *Manager) StartRecording(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sessions[streamID]; exists {
		return fmt.Errorf("DVR session already exists for stream")
	}
	if _, exists := m.registry.Get(streamID); !exists {
		return core.ErrStreamNotFound
	}

	streamPath := filepath.Join(m.outputPath, streamID)
	if err := os.MkdirAll(streamPath, 0o755); err != nil {
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
	_ = m.registry.SetState(streamID, core.StreamStateRecording)

	m.logger.Info("DVR recording started",
		zap.String("stream_id", streamID),
		zap.String("session_id", session.ID),
		zap.String("output_path", session.OutputPath),
	)
	return nil
}

func (m *Manager) StopRecording(streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopSessionLocked(streamID)
}

func (m *Manager) stopSessionLocked(streamID string) error {
	session, exists := m.sessions[streamID]
	if !exists {
		return fmt.Errorf("DVR session not found")
	}

	session.State = "stopped"
	session.Duration = time.Since(session.CreatedAt)
	_ = m.registry.SetState(streamID, core.StreamStateLive)

	m.logger.Info("DVR recording stopped",
		zap.String("stream_id", streamID),
		zap.String("session_id", session.ID),
		zap.Duration("duration", session.Duration),
	)
	return nil
}

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

func (m *Manager) GetSession(streamID string) (*DVRSession, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, exists := m.sessions[streamID]
	if !exists {
		return nil, false
	}
	copy := *session
	return &copy, true
}

func (m *Manager) GetSessionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, session := range m.sessions {
		if session.State == "recording" || session.State == "paused" {
			count++
		}
	}
	return count
}

func (m *Manager) GetStats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	activeSessions := 0
	totalFileSize := int64(0)
	for _, session := range m.sessions {
		if session.State == "recording" || session.State == "paused" {
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

func (m *Manager) CleanupOldRecordings() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-m.maxDuration)
	return filepath.Walk(m.outputPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !info.ModTime().Before(cutoff) {
			return nil
		}
		if err := os.Remove(path); err != nil {
			m.logger.Error("failed to remove old recording", zap.String("path", path), zap.Error(err))
			return nil
		}
		m.logger.Info("removed old recording", zap.String("path", path))
		return nil
	})
}
