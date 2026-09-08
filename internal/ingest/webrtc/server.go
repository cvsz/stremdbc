package webrtc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pion/webrtc/v3"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

// Server represents a WebRTC server supporting WHIP/WHEP
type Server struct {
	config      *config.WebRTCConfig
	registry    *core.StreamRegistry
	logger      *zap.Logger
	api         *gin.Engine
	mu          sync.Mutex
	publishers  map[string]*PeerConnection
	viewers     map[string][]*PeerConnection
	meeting     *webrtc.MediaEngine
	apiClient   *webrtc.API
}

// PeerConnection represents a WebRTC peer connection
type PeerConnection struct {
	ID           string
	StreamID     string
	Connection   *webrtc.PeerConnection
	Type         string // "whip" or "whep"
	CreatedAt    time.Time
	RemoteAddr   string
	State        string
}

// SDPOffer represents an SDP offer/answer
type SDPOffer struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

// NewServer creates a new WebRTC server
func NewServer(cfg *config.WebRTCConfig, registry *core.StreamRegistry, logger *zap.Logger) (*Server, error) {
	// Create media engine
	meeting := &webrtc.MediaEngine{}
	if err := meeting.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("failed to register codecs: %w", err)
	}

	// Create API
	api := webrtc.NewAPI(webrtc.WithMediaEngine(meeting))

	s := &Server{
		config:     cfg,
		registry:   registry,
		logger:     logger.Named("webrtc"),
		api:        gin.New(),
		publishers: make(map[string]*PeerConnection),
		viewers:    make(map[string][]*PeerConnection),
		meeting:    meeting,
		apiClient:  api,
	}

	s.setupRoutes()
	return s, nil
}

func (s *Server) setupRoutes() {
	s.api.Use(gin.Recovery())
	s.api.Use(gin.Logger())

	// WHIP endpoint for publishing
	s.api.POST("/whip/:streamid", s.handleWHIP)
	s.api.OPTIONS("/whip/:streamid", s.handleWHIPOptions)

	// WHEP endpoint for playback
	s.api.POST("/whep/:streamid", s.handleWHEP)
	s.api.OPTIONS("/whep/:streamid", s.handleWHEPOptions)

	// Delete endpoint for session termination
	s.api.DELETE("/whip/:streamid/:sessionid", s.handleDeleteSession)
	s.api.DELETE("/whep/:streamid/:sessionid", s.handleDeleteSession)
}

// Start starts the WebRTC server
func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	
	go func() {
		s.logger.Info("WebRTC server started", zap.String("address", addr))
		if err := s.api.Run(addr); err != nil && err != http.ErrServerClosed {
			s.logger.Error("WebRTC server failed", zap.Error(err))
		}
	}()

	return nil
}

// Stop stops the WebRTC server
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("stopping WebRTC server")
	return nil
}

func (s *Server) handleWHIPOptions(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Methods", "POST, OPTIONS, DELETE")
	c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
	c.Status(http.StatusNoContent)
}

func (s *Server) handleWHIP(c *gin.Context) {
	streamID := c.Param("streamid")
	
	c.Header("Access-Control-Allow-Origin", "*")

	// Read SDP offer from request body
	var offer webrtc.SessionDescription
	if err := c.ShouldBindJSON(&offer); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid SDP offer"})
		return
	}

	// Verify stream exists or create it
	if _, exists := s.registry.Get(streamID); !exists {
		_, err := s.registry.Register(streamID, streamID)
		if err != nil && err != core.ErrStreamExists {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to register stream"})
			return
		}
	}

	// Create peer connection
	pc, err := s.apiClient.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{s.config.ICEServer},
	})
	if err != nil {
		s.logger.Error("failed to create peer connection", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create peer connection"})
		return
	}

	// Handle incoming tracks
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		s.logger.Info("received track",
			zap.String("stream_id", streamID),
			zap.String("codec", track.Codec().MimeType),
		)
		
		// Update stream state
		_ = s.registry.SetState(streamID, core.StreamStateLive)
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		s.logger.Info("peer connection state changed",
			zap.String("state", state.String()),
			zap.String("stream_id", streamID),
		)
		
		if state == webrtc.PeerConnectionStateFailed || 
		   state == webrtc.PeerConnectionStateClosed ||
		   state == webrtc.PeerConnectionStateDisconnected {
			s.removePublisher(streamID)
		}
	})

	// Set remote description
	if err := pc.SetRemoteDescription(offer); err != nil {
		s.logger.Error("failed to set remote description", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to set remote description"})
		return
	}

	// Create answer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		s.logger.Error("failed to create answer", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create answer"})
		return
	}

	// Set local description
	if err := pc.SetLocalDescription(answer); err != nil {
		s.logger.Error("failed to set local description", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to set local description"})
		return
	}

	// Store publisher
	pcID := generateID()
	publisher := &PeerConnection{
		ID:         pcID,
		StreamID:   streamID,
		Connection: pc,
		Type:       "whip",
		CreatedAt:  time.Now(),
		RemoteAddr: c.ClientIP(),
		State:      "active",
	}

	s.mu.Lock()
	s.publishers[streamID] = publisher
	s.mu.Unlock()

	s.logger.Info("WHIP publisher connected",
		zap.String("stream_id", streamID),
		zap.String("pc_id", pcID),
	)

	// Return SDP answer
	response := SDPOffer{
		Type: string(pc.LocalDescription().Type),
		SDP:  pc.LocalDescription().SDP,
	}

	c.Header("Location", fmt.Sprintf("/whip/%s/%s", streamID, pcID))
	c.JSON(http.StatusCreated, response)
}

func (s *Server) handleWHEPOptions(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Methods", "POST, OPTIONS, DELETE")
	c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
	c.Status(http.StatusNoContent)
}

func (s *Server) handleWHEP(c *gin.Context) {
	streamID := c.Param("streamid")
	
	c.Header("Access-Control-Allow-Origin", "*")

	// Check if stream exists and is live
	stream, exists := s.registry.Get(streamID)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "stream not found"})
		return
	}

	if stream.State != core.StreamStateLive {
		c.JSON(http.StatusBadRequest, gin.H{"error": "stream is not live"})
		return
	}

	// Read SDP offer from request body
	var offer webrtc.SessionDescription
	if err := c.ShouldBindJSON(&offer); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid SDP offer"})
		return
	}

	// Create peer connection
	pc, err := s.apiClient.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{s.config.ICEServer},
	})
	if err != nil {
		s.logger.Error("failed to create peer connection", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create peer connection"})
		return
	}

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || 
		   state == webrtc.PeerConnectionStateClosed ||
		   state == webrtc.PeerConnectionStateDisconnected {
			s.removeViewer(streamID, pc)
		}
	})

	// Set remote description
	if err := pc.SetRemoteDescription(offer); err != nil {
		s.logger.Error("failed to set remote description", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to set remote description"})
		return
	}

	// Create answer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		s.logger.Error("failed to create answer", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create answer"})
		return
	}

	// Set local description
	if err := pc.SetLocalDescription(answer); err != nil {
		s.logger.Error("failed to set local description", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to set local description"})
		return
	}

	// Store viewer
	pcID := generateID()
	viewer := &PeerConnection{
		ID:         pcID,
		StreamID:   streamID,
		Connection: pc,
		Type:       "whep",
		CreatedAt:  time.Now(),
		RemoteAddr: c.ClientIP(),
		State:      "active",
	}

	s.mu.Lock()
	s.viewers[streamID] = append(s.viewers[streamID], viewer)
	s.mu.Unlock()

	// Increment viewers
	_ = s.registry.IncrementViewers(streamID)

	s.logger.Info("WHEP viewer connected",
		zap.String("stream_id", streamID),
		zap.String("pc_id", pcID),
	)

	// Return SDP answer
	response := SDPOffer{
		Type: string(pc.LocalDescription().Type),
		SDP:  pc.LocalDescription().SDP,
	}

	c.Header("Location", fmt.Sprintf("/whep/%s/%s", streamID, pcID))
	c.JSON(http.StatusCreated, response)
}

func (s *Server) handleDeleteSession(c *gin.Context) {
	streamID := c.Param("streamid")
	sessionID := c.Param("sessionid")
	path := c.Request.URL.Path

	s.mu.Lock()
	defer s.mu.Unlock()

	if strings.Contains(path, "/whip/") {
		if pub, exists := s.publishers[streamID]; exists && pub.ID == sessionID {
			_ = pub.Connection.Close()
			delete(s.publishers, streamID)
			s.logger.Info("WHIP session terminated",
				zap.String("stream_id", streamID),
				zap.String("session_id", sessionID),
			)
		}
	} else if strings.Contains(path, "/whep/") {
		if viewers, exists := s.viewers[streamID]; exists {
			for i, v := range viewers {
				if v.ID == sessionID {
					_ = v.Connection.Close()
					s.viewers[streamID] = append(viewers[:i], viewers[i+1:]...)
					_ = s.registry.DecrementViewers(streamID)
					s.logger.Info("WHEP session terminated",
						zap.String("stream_id", streamID),
						zap.String("session_id", sessionID),
					)
					break
				}
			}
		}
	}

	c.Status(http.StatusOK)
}

func (s *Server) removePublisher(streamID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.publishers, streamID)
}

func (s *Server) removeViewer(streamID string, pc *webrtc.PeerConnection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if viewers, exists := s.viewers[streamID]; exists {
		for i, v := range viewers {
			if v.Connection == pc {
				s.viewers[streamID] = append(viewers[:i], viewers[i+1:]...)
				_ = s.registry.DecrementViewers(streamID)
				break
			}
		}
	}
}

// GetPublisherCount returns the number of active publishers
func (s *Server) GetPublisherCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.publishers)
}

// GetViewerCount returns the total number of viewers
func (s *Server) GetViewerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, viewers := range s.viewers {
		count += len(viewers)
	}
	return count
}

// GetStats returns WebRTC statistics
func (s *Server) GetStats() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]interface{}{
		"publishers": len(s.publishers),
		"viewers":    s.getViewerCountLocked(),
	}
}

func (s *Server) getViewerCountLocked() int {
	count := 0
	for _, viewers := range s.viewers {
		count += len(viewers)
	}
	return count
}

func generateID() string {
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))[:16]
}
