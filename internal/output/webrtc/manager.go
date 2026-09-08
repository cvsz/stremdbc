package webrtc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

// OutputManager manages WebRTC output (WHEP)
type OutputManager struct {
	config     *config.WebRTCConfig
	registry   *core.StreamRegistry
	logger     *zap.Logger
	api        *webrtc.API
	mu         sync.Mutex
	viewers    map[string][]*ViewerSession
	meeting    *webrtc.MediaEngine
}

// ViewerSession represents a WebRTC viewer session
type ViewerSession struct {
	ID         string
	StreamID   string
	PeerConn   *webrtc.PeerConnection
	CreatedAt  time.Time
	RemoteAddr string
	State      string
	BytesSent  int64
}

// NewOutputManager creates a new WebRTC output manager
func NewOutputManager(cfg *config.WebRTCConfig, registry *core.StreamRegistry, logger *zap.Logger) (*OutputManager, error) {
	// Create media engine
	meeting := &webrtc.MediaEngine{}
	if err := meeting.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("failed to register codecs: %w", err)
	}

	// Create API
	api := webrtc.NewAPI(webrtc.WithMediaEngine(meeting))

	return &OutputManager{
		config:   cfg,
		registry: registry,
		logger:   logger.Named("webrtc-output"),
		api:      api,
		viewers:  make(map[string][]*ViewerSession),
		meeting:  meeting,
	}, nil
}

// Start starts the WebRTC output manager
func (m *OutputManager) Start(ctx context.Context) error {
	m.logger.Info("WebRTC output manager started")
	return nil
}

// Stop stops the WebRTC output manager
func (m *OutputManager) Stop() error {
	m.logger.Info("stopping WebRTC output manager")
	
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Close all viewer connections
	for _, sessions := range m.viewers {
		for _, session := range sessions {
			if session.PeerConn != nil {
				_ = session.PeerConn.Close()
			}
		}
	}
	
	return nil
}

// CreateViewer creates a new viewer session for a stream
func (m *OutputManager) CreateViewer(streamID string, offer webrtc.SessionDescription, remoteAddr string) (*ViewerSession, webrtc.SessionDescription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if stream exists and is live
	stream, exists := m.registry.Get(streamID)
	if !exists {
		return nil, webrtc.SessionDescription{}, fmt.Errorf("stream not found")
	}

	if stream.State != core.StreamStateLive {
		return nil, webrtc.SessionDescription{}, fmt.Errorf("stream is not live")
	}

	// Create peer connection
	pc, err := m.api.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{m.config.ICEServer},
	})
	if err != nil {
		m.logger.Error("failed to create peer connection", zap.Error(err))
		return nil, webrtc.SessionDescription{}, fmt.Errorf("failed to create peer connection: %w", err)
	}

	// Set remote description
	if err := pc.SetRemoteDescription(offer); err != nil {
		m.logger.Error("failed to set remote description", zap.Error(err))
		return nil, webrtc.SessionDescription{}, fmt.Errorf("failed to set remote description: %w", err)
	}

	// Create answer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		m.logger.Error("failed to create answer", zap.Error(err))
		return nil, webrtc.SessionDescription{}, fmt.Errorf("failed to create answer: %w", err)
	}

	// Set local description
	if err := pc.SetLocalDescription(answer); err != nil {
		m.logger.Error("failed to set local description", zap.Error(err))
		return nil, webrtc.SessionDescription{}, fmt.Errorf("failed to set local description: %w", err)
	}

	// Handle connection state changes
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed ||
			state == webrtc.PeerConnectionStateClosed ||
			state == webrtc.PeerConnectionStateDisconnected {
			m.removeViewer(streamID, pc)
		}
	})

	// Create viewer session
	sessionID := generateSessionID()
	session := &ViewerSession{
		ID:         sessionID,
		StreamID:   streamID,
		PeerConn:   pc,
		CreatedAt:  time.Now(),
		RemoteAddr: remoteAddr,
		State:      "active",
	}

	m.viewers[streamID] = append(m.viewers[streamID], session)

	// Increment viewers count
	_ = m.registry.IncrementViewers(streamID)

	m.logger.Info("WHEP viewer connected",
		zap.String("stream_id", streamID),
		zap.String("session_id", sessionID),
		zap.String("remote", remoteAddr),
	)

	return session, *pc.LocalDescription(), nil
}

// RemoveViewer removes a viewer session
func (m *OutputManager) RemoveViewer(streamID string, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	sessions, exists := m.viewers[streamID]
	if !exists {
		return fmt.Errorf("stream not found")
	}

	for i, session := range sessions {
		if session.ID == sessionID {
			if session.PeerConn != nil {
				_ = session.PeerConn.Close()
			}
			m.viewers[streamID] = append(sessions[:i], sessions[i+1:]...)
			_ = m.registry.DecrementViewers(streamID)
			m.logger.Info("WHEP viewer disconnected",
				zap.String("stream_id", streamID),
				zap.String("session_id", sessionID),
			)
			return nil
		}
	}

	return fmt.Errorf("session not found")
}

func (m *OutputManager) removeViewer(streamID string, pc *webrtc.PeerConnection) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sessions, exists := m.viewers[streamID]
	if !exists {
		return
	}

	for i, session := range sessions {
		if session.PeerConn == pc {
			m.viewers[streamID] = append(sessions[:i], sessions[i+1:]...)
			_ = m.registry.DecrementViewers(streamID)
			break
		}
	}
}

// GetViewerCount returns the total number of viewers
func (m *OutputManager) GetViewerCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, sessions := range m.viewers {
		count += len(sessions)
	}
	return count
}

// GetStreamViewerCount returns the number of viewers for a specific stream
func (m *OutputManager) GetStreamViewerCount(streamID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	sessions, exists := m.viewers[streamID]
	if !exists {
		return 0
	}
	return len(sessions)
}

// GetStats returns WebRTC output statistics
func (m *OutputManager) GetStats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	totalSessions := 0
	for _, sessions := range m.viewers {
		totalSessions += len(sessions)
	}

	return map[string]interface{}{
		"total_viewers":   totalSessions,
		"stream_sessions": len(m.viewers),
	}
}

func generateSessionID() string {
	return fmt.Sprintf("whep_%d", time.Now().UnixNano())
}
