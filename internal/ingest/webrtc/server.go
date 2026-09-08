package webrtc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pion/webrtc/v3"

	"github.com/policedbc/stremdbc/internal/auth"
	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

const maxSDPSize = 1 << 20

// Server represents a WebRTC server supporting WHIP/WHEP control exchanges.
// It negotiates peer connections and receives publisher tracks, but forwarding
// those tracks to viewers is outside this repository's current media graph.
type Server struct {
	config     *config.WebRTCConfig
	registry   *core.StreamRegistry
	logger     *zap.Logger
	api        *gin.Engine
	httpServer *http.Server
	listener   net.Listener
	mu         sync.RWMutex
	lifecycle  sync.Mutex
	running    bool
	publishers map[string]*PeerConnection
	viewers    map[string][]*PeerConnection
	meeting    *webrtc.MediaEngine
	apiClient  *webrtc.API
	auth       *auth.Manager
	authMu     sync.RWMutex
}

// PeerConnection represents a WebRTC peer connection.
type PeerConnection struct {
	ID         string
	StreamID   string
	Connection *webrtc.PeerConnection
	Type       string // whip or whep
	CreatedAt  time.Time
	RemoteAddr string
	State      string
}

// SDPOffer represents the JSON compatibility form of an SDP offer/answer.
type SDPOffer struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

func NewServer(cfg *config.WebRTCConfig, registry *core.StreamRegistry, logger *zap.Logger) (*Server, error) {
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
	gin.SetMode(gin.ReleaseMode)
	meeting := &webrtc.MediaEngine{}
	if err := meeting.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("failed to register codecs: %w", err)
	}
	apiClient := webrtc.NewAPI(webrtc.WithMediaEngine(meeting))
	s := &Server{
		config:     &copyCfg,
		registry:   registry,
		logger:     logger.Named("webrtc"),
		api:        gin.New(),
		publishers: make(map[string]*PeerConnection),
		viewers:    make(map[string][]*PeerConnection),
		meeting:    meeting,
		apiClient:  apiClient,
	}
	s.setupRoutes()
	return s, nil
}

func (s *Server) SetAuthManager(manager *auth.Manager) {
	s.authMu.Lock()
	s.auth = manager
	s.authMu.Unlock()
}

func (s *Server) authManagerSnapshot() *auth.Manager {
	s.authMu.RLock()
	defer s.authMu.RUnlock()
	return s.auth
}

func (s *Server) setupRoutes() {
	s.api.Use(gin.Recovery())
	s.api.POST("/whip/:streamid", s.handleWHIP)
	s.api.OPTIONS("/whip/:streamid", s.handleWHIPOptions)
	s.api.POST("/whep/:streamid", s.handleWHEP)
	s.api.OPTIONS("/whep/:streamid", s.handleWHEPOptions)
	s.api.DELETE("/whip/:streamid/:sessionid", s.handleDeleteSession)
	s.api.DELETE("/whep/:streamid/:sessionid", s.handleDeleteSession)
	s.api.OPTIONS("/whip/:streamid/:sessionid", s.handleWHIPOptions)
	s.api.OPTIONS("/whep/:streamid/:sessionid", s.handleWHEPOptions)
}

// Start binds before returning and serves HTTP or HTTPS according to config.
func (s *Server) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("WebRTC server already running")
	}
	s.mu.Unlock()
	addr := net.JoinHostPort(s.config.Host, fmt.Sprintf("%d", s.config.Port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	if err := ctx.Err(); err != nil {
		_ = listener.Close()
		return err
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           s.api,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	if s.config.TLSEnabled {
		cert, err := tls.LoadX509KeyPair(s.config.CertFile, s.config.KeyFile)
		if err != nil {
			_ = listener.Close()
			return fmt.Errorf("load WebRTC TLS certificate: %w", err)
		}
		server.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}
	s.mu.Lock()
	s.listener = listener
	s.httpServer = server
	s.running = true
	s.mu.Unlock()
	s.logger.Info("WebRTC server started", zap.String("address", listener.Addr().String()), zap.Bool("tls", s.config.TLSEnabled))
	serveDone := make(chan struct{})
	go func() {
		var serveErr error
		if s.config.TLSEnabled {
			serveErr = server.ServeTLS(listener, "", "")
		} else {
			serveErr = server.Serve(listener)
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) && !errors.Is(serveErr, net.ErrClosed) {
			s.logger.Error("WebRTC server failed", zap.Error(serveErr))
		}
		s.finishServe(server)
		close(serveDone)
	}()
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Close()
		case <-serveDone:
		}
	}()
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	s.mu.Lock()
	if !s.running && s.httpServer == nil && len(s.publishers) == 0 && len(s.viewers) == 0 {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	server := s.httpServer
	listener := s.listener
	s.httpServer = nil
	s.listener = nil
	publishers := make([]*PeerConnection, 0, len(s.publishers))
	for _, publisher := range s.publishers {
		publishers = append(publishers, publisher)
	}
	viewers := make([]*PeerConnection, 0)
	for streamID, sessions := range s.viewers {
		viewers = append(viewers, sessions...)
		for range sessions {
			_ = s.registry.DecrementViewers(streamID)
		}
	}
	s.publishers = make(map[string]*PeerConnection)
	s.viewers = make(map[string][]*PeerConnection)
	s.mu.Unlock()

	s.logger.Info("stopping WebRTC server")
	if listener != nil {
		_ = listener.Close()
	}
	var shutdownErr error
	if server != nil {
		shutdownErr = server.Shutdown(ctx)
		if shutdownErr != nil {
			shutdownErr = errors.Join(shutdownErr, server.Close())
		}
	}
	for _, publisher := range publishers {
		if publisher.Connection != nil {
			_ = publisher.Connection.Close()
		}
	}
	for _, viewer := range viewers {
		if viewer.Connection != nil {
			_ = viewer.Connection.Close()
		}
	}
	for _, publisher := range publishers {
		s.setIdleIfLive(publisher.StreamID)
	}
	return shutdownErr
}

func (s *Server) finishServe(server *http.Server) {
	s.mu.Lock()
	if s.httpServer != server {
		s.mu.Unlock()
		return
	}
	s.running = false
	s.httpServer = nil
	s.listener = nil
	publishers := make([]*PeerConnection, 0, len(s.publishers))
	for _, publisher := range s.publishers {
		publishers = append(publishers, publisher)
	}
	viewers := make([]*PeerConnection, 0)
	for streamID, sessions := range s.viewers {
		viewers = append(viewers, sessions...)
		for range sessions {
			_ = s.registry.DecrementViewers(streamID)
		}
	}
	s.publishers = make(map[string]*PeerConnection)
	s.viewers = make(map[string][]*PeerConnection)
	s.mu.Unlock()

	for _, publisher := range publishers {
		if publisher.Connection != nil {
			_ = publisher.Connection.Close()
		}
		s.setIdleIfLive(publisher.StreamID)
	}
	for _, viewer := range viewers {
		if viewer.Connection != nil {
			_ = viewer.Connection.Close()
		}
	}
}

func (s *Server) handleWHIPOptions(c *gin.Context) {
	s.setCORS(c)
	c.Status(http.StatusNoContent)
}

func (s *Server) handleWHIP(c *gin.Context) {
	s.setCORS(c)
	streamID := c.Param("streamid")
	if err := core.ValidateStreamID(streamID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid stream id"})
		return
	}
	if !s.authorize(c, "publish", streamID) {
		return
	}
	offer, standardSDP, err := parseSDPOffer(c.Request)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid SDP offer"})
		return
	}
	if _, exists := s.registry.Get(streamID); !exists {
		if _, err := s.registry.Register(streamID, streamID); err != nil && !errors.Is(err, core.ErrStreamExists) {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to register stream"})
			return
		}
	}
	pc, err := s.newPeerConnection()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create peer connection"})
		return
	}
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		s.logger.Info("received WebRTC publisher track", zap.String("stream_id", streamID), zap.String("codec", track.Codec().MimeType))
		s.setLiveIfAvailable(streamID)
	})
	if err := pc.SetRemoteDescription(offer); err != nil {
		_ = pc.Close()
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to set remote description"})
		return
	}
	answer, err := createAnswer(pc)
	if err != nil {
		_ = pc.Close()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create answer"})
		return
	}
	pcID := uuid.NewString()
	publisher := &PeerConnection{ID: pcID, StreamID: streamID, Connection: pc, Type: "whip", CreatedAt: time.Now().UTC(), RemoteAddr: requestRemoteAddr(c.Request), State: "active"}
	s.mu.Lock()
	previous := s.publishers[streamID]
	s.publishers[streamID] = publisher
	s.mu.Unlock()
	if previous != nil && previous.Connection != nil {
		_ = previous.Connection.Close()
	}
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed || state == webrtc.PeerConnectionStateDisconnected {
			s.removePublisher(streamID, pc)
		}
	})
	c.Header("Location", fmt.Sprintf("/whip/%s/%s", streamID, pcID))
	writeSDPAnswer(c, answer, standardSDP)
}

func (s *Server) handleWHEPOptions(c *gin.Context) {
	s.setCORS(c)
	c.Status(http.StatusNoContent)
}

func (s *Server) handleWHEP(c *gin.Context) {
	s.setCORS(c)
	streamID := c.Param("streamid")
	if err := core.ValidateStreamID(streamID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid stream id"})
		return
	}
	if !s.authorize(c, "play", streamID) {
		return
	}
	stream, exists := s.registry.Get(streamID)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "stream not found"})
		return
	}
	if stream.State != core.StreamStateLive {
		c.JSON(http.StatusBadRequest, gin.H{"error": "stream is not live"})
		return
	}
	offer, standardSDP, err := parseSDPOffer(c.Request)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid SDP offer"})
		return
	}
	pc, err := s.newPeerConnection()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create peer connection"})
		return
	}
	if err := pc.SetRemoteDescription(offer); err != nil {
		_ = pc.Close()
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to set remote description"})
		return
	}
	answer, err := createAnswer(pc)
	if err != nil {
		_ = pc.Close()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create answer"})
		return
	}
	pcID := uuid.NewString()
	viewer := &PeerConnection{ID: pcID, StreamID: streamID, Connection: pc, Type: "whep", CreatedAt: time.Now().UTC(), RemoteAddr: requestRemoteAddr(c.Request), State: "active"}
	s.mu.Lock()
	s.viewers[streamID] = append(s.viewers[streamID], viewer)
	s.mu.Unlock()
	if err := s.registry.IncrementViewers(streamID); err != nil {
		s.removeViewer(streamID, pc)
		_ = pc.Close()
		c.JSON(http.StatusGone, gin.H{"error": "stream is no longer available"})
		return
	}
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed || state == webrtc.PeerConnectionStateDisconnected {
			s.removeViewer(streamID, pc)
		}
	})
	c.Header("Location", fmt.Sprintf("/whep/%s/%s", streamID, pcID))
	writeSDPAnswer(c, answer, standardSDP)
}

func (s *Server) handleDeleteSession(c *gin.Context) {
	s.setCORS(c)
	streamID := c.Param("streamid")
	sessionID := c.Param("sessionid")
	if err := core.ValidateStreamID(streamID); err != nil || strings.TrimSpace(sessionID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid session path"})
		return
	}
	if _, err := uuid.Parse(sessionID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid session path"})
		return
	}
	if strings.Contains(c.Request.URL.Path, "/whip/") {
		if !s.authorize(c, "publish", streamID) {
			return
		}
		if publisher := s.removePublisherByID(streamID, sessionID); publisher != nil {
			if publisher.Connection != nil {
				_ = publisher.Connection.Close()
			}
			_ = s.registry.SetState(streamID, core.StreamStateIdle)
		}
	} else {
		if !s.authorize(c, "play", streamID) {
			return
		}
		if viewer := s.removeViewerByID(streamID, sessionID); viewer != nil {
			if viewer.Connection != nil {
				_ = viewer.Connection.Close()
			}
			_ = s.registry.DecrementViewers(streamID)
		}
	}
	c.Status(http.StatusNoContent)
}

func (s *Server) newPeerConnection() (*webrtc.PeerConnection, error) {
	configuration := webrtc.Configuration{}
	if len(s.config.ICEServer.URLs) > 0 {
		configuration.ICEServers = []webrtc.ICEServer{s.config.ICEServer}
	}
	return s.apiClient.NewPeerConnection(configuration)
}

func createAnswer(pc *webrtc.PeerConnection) (webrtc.SessionDescription, error) {
	gatheringComplete := webrtc.GatheringCompletePromise(pc)
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return webrtc.SessionDescription{}, err
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		return webrtc.SessionDescription{}, err
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-gatheringComplete:
	case <-timer.C:
		return webrtc.SessionDescription{}, fmt.Errorf("ICE gathering timed out")
	}
	local := pc.LocalDescription()
	if local == nil {
		return webrtc.SessionDescription{}, fmt.Errorf("local SDP is unavailable")
	}
	return *local, nil
}

func (s *Server) removePublisher(streamID string, pc *webrtc.PeerConnection) {
	publisher := s.removePublisherByPeer(streamID, pc)
	if publisher != nil {
		s.setIdleIfLive(streamID)
	}
}

func (s *Server) setLiveIfAvailable(streamID string) {
	stream, exists := s.registry.Get(streamID)
	if !exists || stream.State == core.StreamStateRecording {
		return
	}
	_ = s.registry.SetState(streamID, core.StreamStateLive)
}

func (s *Server) setIdleIfLive(streamID string) {
	stream, exists := s.registry.Get(streamID)
	if exists && stream.State == core.StreamStateLive {
		_ = s.registry.SetState(streamID, core.StreamStateIdle)
	}
}

func (s *Server) removePublisherByPeer(streamID string, pc *webrtc.PeerConnection) *PeerConnection {
	s.mu.Lock()
	defer s.mu.Unlock()
	publisher, exists := s.publishers[streamID]
	if !exists || publisher.Connection != pc {
		return nil
	}
	delete(s.publishers, streamID)
	return publisher
}

func (s *Server) removePublisherByID(streamID, sessionID string) *PeerConnection {
	s.mu.Lock()
	defer s.mu.Unlock()
	publisher, exists := s.publishers[streamID]
	if !exists || publisher.ID != sessionID {
		return nil
	}
	delete(s.publishers, streamID)
	return publisher
}

func (s *Server) removeViewer(streamID string, pc *webrtc.PeerConnection) {
	if s.removeViewerByPeer(streamID, pc) != nil {
		_ = s.registry.DecrementViewers(streamID)
	}
}

func (s *Server) removeViewerByPeer(streamID string, pc *webrtc.PeerConnection) *PeerConnection {
	s.mu.Lock()
	defer s.mu.Unlock()
	sessions := s.viewers[streamID]
	for i, session := range sessions {
		if session.Connection == pc {
			s.viewers[streamID] = append(sessions[:i], sessions[i+1:]...)
			if len(s.viewers[streamID]) == 0 {
				delete(s.viewers, streamID)
			}
			return session
		}
	}
	return nil
}

func (s *Server) removeViewerByID(streamID, sessionID string) *PeerConnection {
	s.mu.Lock()
	defer s.mu.Unlock()
	sessions := s.viewers[streamID]
	for i, session := range sessions {
		if session.ID == sessionID {
			s.viewers[streamID] = append(sessions[:i], sessions[i+1:]...)
			if len(s.viewers[streamID]) == 0 {
				delete(s.viewers, streamID)
			}
			return session
		}
	}
	return nil
}

func (s *Server) GetPublisherCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.publishers)
}

func (s *Server) GetViewerCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getViewerCountLocked()
}

func (s *Server) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]interface{}{"publishers": len(s.publishers), "viewers": s.getViewerCountLocked()}
}

func (s *Server) getViewerCountLocked() int {
	count := 0
	for _, sessions := range s.viewers {
		count += len(sessions)
	}
	return count
}

func (s *Server) authorize(c *gin.Context, action, streamID string) bool {
	manager := s.authManagerSnapshot()
	if manager == nil {
		return true
	}
	token := c.Query("token")
	if token == "" {
		const prefix = "Bearer "
		header := c.GetHeader("Authorization")
		if strings.HasPrefix(header, prefix) {
			token = strings.TrimSpace(strings.TrimPrefix(header, prefix))
		}
	}
	if token == "" && action == "play" && manager.AllowAnonymous() {
		return true
	}
	claims, err := manager.ValidateToken(token)
	remoteIP := requestRemoteAddr(c.Request)
	if err != nil || (action == "publish" && !manager.CanPublishFromIP(claims, streamID, remoteIP)) || (action == "play" && !manager.CanPlayFromIP(claims, streamID, remoteIP)) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return false
	}
	return true
}

func (s *Server) setCORS(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Methods", "POST, OPTIONS, DELETE")
	c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func parseSDPOffer(request *http.Request) (webrtc.SessionDescription, bool, error) {
	if request == nil || request.Body == nil {
		return webrtc.SessionDescription{}, false, fmt.Errorf("request body is required")
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(request.Header.Get("Content-Type"), ";")[0]))
	body, err := io.ReadAll(io.LimitReader(request.Body, maxSDPSize+1))
	if err != nil || len(body) == 0 || len(body) > maxSDPSize {
		return webrtc.SessionDescription{}, contentType == "application/sdp", fmt.Errorf("invalid SDP body")
	}
	if contentType == "application/sdp" {
		if !strings.Contains(string(body), "v=") {
			return webrtc.SessionDescription{}, true, fmt.Errorf("SDP version is missing")
		}
		return webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: string(body)}, true, nil
	}
	if contentType != "application/json" {
		return webrtc.SessionDescription{}, false, fmt.Errorf("unsupported SDP content type")
	}
	var encoded SDPOffer
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&encoded); err != nil || strings.TrimSpace(encoded.SDP) == "" || strings.ToLower(encoded.Type) != "offer" {
		return webrtc.SessionDescription{}, false, fmt.Errorf("invalid JSON SDP offer")
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		return webrtc.SessionDescription{}, false, fmt.Errorf("multiple JSON values are not allowed")
	}
	return webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: encoded.SDP}, false, nil
}

func writeSDPAnswer(c *gin.Context, answer webrtc.SessionDescription, standard bool) {
	if standard {
		c.Data(http.StatusCreated, "application/sdp", []byte(answer.SDP))
		return
	}
	c.JSON(http.StatusCreated, SDPOffer{Type: answer.Type.String(), SDP: answer.SDP})
}

func requestRemoteAddr(request *http.Request) string {
	if request == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(request.RemoteAddr)
	if err == nil {
		return host
	}
	return request.RemoteAddr
}
