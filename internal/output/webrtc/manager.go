package webrtc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v3"
	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

var (
	ErrOutputManagerStopped    = errors.New("WebRTC output manager is stopped")
	ErrOutputManagerNotStarted = errors.New("WebRTC output manager is not started")
	ErrInvalidOffer            = errors.New("invalid WebRTC offer")
)

// OutputManager manages WebRTC output (WHEP) control sessions. Peer
// negotiation is supported; forwarding publisher RTP tracks requires a media
// graph and is intentionally not reported as available here.
type OutputManager struct {
	config    *config.WebRTCConfig
	registry  *core.StreamRegistry
	logger    *zap.Logger
	api       *webrtc.API
	mu        sync.RWMutex
	lifecycle sync.Mutex
	viewers   map[string][]*ViewerSession
	meeting   *webrtc.MediaEngine
	started   bool
	stopped   bool
	ctx       context.Context
	cancel    context.CancelFunc
}

// ViewerSession represents a WebRTC viewer session and is returned as a
// snapshot by query methods.
type ViewerSession struct {
	ID         string
	StreamID   string
	PeerConn   *webrtc.PeerConnection
	CreatedAt  time.Time
	RemoteAddr string
	State      string
	BytesSent  int64
}

func NewOutputManager(cfg *config.WebRTCConfig, registry *core.StreamRegistry, logger *zap.Logger) (*OutputManager, error) {
	if cfg == nil {
		copyCfg := config.DefaultConfig().WebRTC
		cfg = &copyCfg
	}
	if registry == nil {
		registry = core.NewStreamRegistry(nil)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	copyCfg := *cfg
	copyCfg.ICEServer.URLs = append([]string(nil), cfg.ICEServer.URLs...)
	meeting := &webrtc.MediaEngine{}
	if err := meeting.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("failed to register codecs: %w", err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(meeting))
	ctx, cancel := context.WithCancel(context.Background())
	return &OutputManager{config: &copyCfg, registry: registry, logger: logger.Named("webrtc-output"), api: api, viewers: make(map[string][]*ViewerSession), meeting: meeting, ctx: ctx, cancel: cancel}, nil
}

// Start starts the manager. It is idempotent.
func (m *OutputManager) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	if m.stopped {
		return ErrOutputManagerStopped
	}
	if m.started {
		return nil
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.started = true
	m.logger.Info("WebRTC output manager started")
	return nil
}

// Stop closes all viewer peer connections and balances registry viewer counts.
func (m *OutputManager) Stop() error {
	m.lifecycle.Lock()
	if m.stopped {
		m.lifecycle.Unlock()
		return nil
	}
	m.stopped = true
	if m.cancel != nil {
		m.cancel()
	}
	m.mu.Lock()
	sessions := make([]*ViewerSession, 0)
	for streamID, viewers := range m.viewers {
		sessions = append(sessions, viewers...)
		for range viewers {
			_ = m.registry.DecrementViewers(streamID)
		}
	}
	m.viewers = make(map[string][]*ViewerSession)
	m.mu.Unlock()
	m.lifecycle.Unlock()

	m.logger.Info("stopping WebRTC output manager")
	for _, session := range sessions {
		if session.PeerConn != nil {
			_ = session.PeerConn.Close()
		}
	}
	return nil
}

// CreateViewer negotiates a WHEP viewer session for a live stream.
func (m *OutputManager) CreateViewer(streamID string, offer webrtc.SessionDescription, remoteAddr string) (*ViewerSession, webrtc.SessionDescription, error) {
	if err := core.ValidateStreamID(streamID); err != nil {
		return nil, webrtc.SessionDescription{}, err
	}
	if err := validateOffer(offer); err != nil {
		return nil, webrtc.SessionDescription{}, err
	}
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	if !m.started {
		return nil, webrtc.SessionDescription{}, ErrOutputManagerNotStarted
	}
	if m.stopped {
		return nil, webrtc.SessionDescription{}, ErrOutputManagerStopped
	}
	stream, exists := m.registry.Get(streamID)
	if !exists {
		return nil, webrtc.SessionDescription{}, fmt.Errorf("stream not found")
	}
	if stream.State != core.StreamStateLive {
		return nil, webrtc.SessionDescription{}, fmt.Errorf("stream is not live")
	}
	configuration := webrtc.Configuration{}
	if len(m.config.ICEServer.URLs) > 0 {
		configuration.ICEServers = []webrtc.ICEServer{m.config.ICEServer}
	}
	pc, err := m.api.NewPeerConnection(configuration)
	if err != nil {
		return nil, webrtc.SessionDescription{}, fmt.Errorf("create peer connection: %w", err)
	}
	if err := pc.SetRemoteDescription(offer); err != nil {
		_ = pc.Close()
		return nil, webrtc.SessionDescription{}, fmt.Errorf("set remote description: %w", err)
	}
	gatheringComplete := webrtc.GatheringCompletePromise(pc)
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		_ = pc.Close()
		return nil, webrtc.SessionDescription{}, fmt.Errorf("create answer: %w", err)
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		_ = pc.Close()
		return nil, webrtc.SessionDescription{}, fmt.Errorf("set local description: %w", err)
	}
	timer := time.NewTimer(10 * time.Second)
	select {
	case <-gatheringComplete:
		timer.Stop()
	case <-timer.C:
		_ = pc.Close()
		return nil, webrtc.SessionDescription{}, fmt.Errorf("ICE gathering timed out")
	}
	local := pc.LocalDescription()
	if local == nil {
		_ = pc.Close()
		return nil, webrtc.SessionDescription{}, fmt.Errorf("local description is unavailable")
	}
	session := &ViewerSession{ID: uuid.NewString(), StreamID: streamID, PeerConn: pc, CreatedAt: time.Now().UTC(), RemoteAddr: remoteAddr, State: "active"}
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed || state == webrtc.PeerConnectionStateDisconnected {
			m.removeViewer(streamID, pc)
		}
	})
	m.mu.Lock()
	m.viewers[streamID] = append(m.viewers[streamID], session)
	m.mu.Unlock()
	if err := m.registry.IncrementViewers(streamID); err != nil {
		m.removeViewer(streamID, pc)
		_ = pc.Close()
		return nil, webrtc.SessionDescription{}, fmt.Errorf("increment viewers: %w", err)
	}
	m.logger.Info("WHEP viewer connected", zap.String("stream_id", streamID), zap.String("session_id", session.ID), zap.String("remote", remoteAddr))
	return viewerSnapshot(session), *local, nil
}

// RemoveViewer removes and closes a viewer session.
func (m *OutputManager) RemoveViewer(streamID, sessionID string) error {
	session := m.removeViewerByID(streamID, sessionID)
	if session == nil {
		return fmt.Errorf("session not found")
	}
	if session.PeerConn != nil {
		_ = session.PeerConn.Close()
	}
	_ = m.registry.DecrementViewers(streamID)
	m.logger.Info("WHEP viewer disconnected", zap.String("stream_id", streamID), zap.String("session_id", sessionID))
	return nil
}

func (m *OutputManager) removeViewer(streamID string, pc *webrtc.PeerConnection) {
	if m.removeViewerByPeer(streamID, pc) != nil {
		_ = m.registry.DecrementViewers(streamID)
	}
}

func (m *OutputManager) removeViewerByPeer(streamID string, pc *webrtc.PeerConnection) *ViewerSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	sessions := m.viewers[streamID]
	for i, session := range sessions {
		if session.PeerConn == pc {
			m.viewers[streamID] = append(sessions[:i], sessions[i+1:]...)
			if len(m.viewers[streamID]) == 0 {
				delete(m.viewers, streamID)
			}
			return session
		}
	}
	return nil
}

func (m *OutputManager) removeViewerByID(streamID, sessionID string) *ViewerSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	sessions := m.viewers[streamID]
	for i, session := range sessions {
		if session.ID == sessionID {
			m.viewers[streamID] = append(sessions[:i], sessions[i+1:]...)
			if len(m.viewers[streamID]) == 0 {
				delete(m.viewers, streamID)
			}
			return session
		}
	}
	return nil
}

func (m *OutputManager) GetViewer(streamID, sessionID string) (*ViewerSession, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, session := range m.viewers[streamID] {
		if session.ID == sessionID {
			return viewerSnapshot(session), true
		}
	}
	return nil, false
}

func (m *OutputManager) GetViewerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getViewerCountLocked()
}

func (m *OutputManager) GetStreamViewerCount(streamID string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.viewers[streamID])
}

func (m *OutputManager) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return map[string]interface{}{"total_viewers": m.getViewerCountLocked(), "stream_sessions": len(m.viewers)}
}

func (m *OutputManager) getViewerCountLocked() int {
	count := 0
	for _, sessions := range m.viewers {
		count += len(sessions)
	}
	return count
}

func validateOffer(offer webrtc.SessionDescription) error {
	if offer.Type != webrtc.SDPTypeOffer || strings.TrimSpace(offer.SDP) == "" || len(offer.SDP) > 1<<20 {
		return ErrInvalidOffer
	}
	return nil
}

func viewerSnapshot(session *ViewerSession) *ViewerSession {
	copy := *session
	copy.PeerConn = nil
	return &copy
}
