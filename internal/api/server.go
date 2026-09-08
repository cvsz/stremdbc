package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/auth"
	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"github.com/policedbc/stremdbc/internal/metrics"
	"go.uber.org/zap"
)

// Server represents the REST and management HTTP server.
type Server struct {
	config         *config.APIConfig
	registry       *core.StreamRegistry
	metrics        *metrics.Metrics
	logger         *zap.Logger
	mux            *http.ServeMux
	authManager    *auth.Manager
	httpServer     *http.Server
	basePath       string
	version        string
	startedAt      time.Time
	componentStats map[string]func() map[string]interface{}
	statsMu        sync.RWMutex
}

func NewServer(cfg *config.APIConfig, registry *core.StreamRegistry, m *metrics.Metrics, logger *zap.Logger) *Server {
	basePath := strings.TrimRight(cfg.BasePath, "/")
	if basePath == "" {
		basePath = "/api/v1"
	}

	s := &Server{
		config:         cfg,
		registry:       registry,
		metrics:        m,
		logger:         logger.Named("api"),
		mux:            http.NewServeMux(),
		basePath:       basePath,
		version:        "dev",
		startedAt:      time.Now(),
		componentStats: make(map[string]func() map[string]interface{}),
	}
	s.registerRoutes()
	return s
}

func (s *Server) SetAuthManager(am *auth.Manager) {
	s.authManager = am
}

func (s *Server) SetVersion(version string) {
	if strings.TrimSpace(version) != "" {
		s.version = version
	}
}

// RegisterStats adds a component statistics provider to /api/v1/stats.
func (s *Server) RegisterStats(name string, provider func() map[string]interface{}) {
	if name == "" || provider == nil {
		return
	}
	s.statsMu.Lock()
	s.componentStats[name] = provider
	s.statsMu.Unlock()
}

// SetStaticRoutes exposes streaming assets and the zero-build management UI.
func (s *Server) SetStaticRoutes(hlsPath, llhlsPath, playerFile, dashboardDir string) {
	if hlsPath != "" {
		s.mux.Handle("/hls/", http.StripPrefix("/hls/", http.FileServer(http.Dir(hlsPath))))
	}
	if llhlsPath != "" {
		s.mux.Handle("/llhls/", http.StripPrefix("/llhls/", http.FileServer(http.Dir(llhlsPath))))
	}
	if playerFile != "" {
		s.mux.HandleFunc("/player/", func(w http.ResponseWriter, r *http.Request) {
			if strings.TrimPrefix(r.URL.Path, "/player/") == "" {
				http.NotFound(w, r)
				return
			}
			http.ServeFile(w, r, playerFile)
		})
	}
	if dashboardDir != "" {
		s.mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/dashboard/", http.StatusTemporaryRedirect)
		})
		s.mux.Handle("/dashboard/", http.StripPrefix("/dashboard/", http.FileServer(http.Dir(dashboardDir))))
	}
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/ready", s.handleReady)
	s.mux.HandleFunc("/metrics", s.handleMetrics)
	s.mux.HandleFunc(s.basePath+"/info", s.handleInfo)
	s.mux.HandleFunc(s.basePath+"/stats", s.handleStats)
	s.mux.HandleFunc(s.basePath+"/auth/token", s.handleToken)
	s.mux.HandleFunc(s.basePath+"/streams", s.handleStreams)
	s.mux.HandleFunc(s.basePath+"/streams/", s.handleStreamByID)
}

func (s *Server) Handler() http.Handler {
	return s.corsMiddleware(s.loggingMiddleware(s.securityHeadersMiddleware(s.mux)))
}

func (s *Server) Start(addr string) error {
	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      s.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	s.logger.Info("API server started", zap.String("address", addr))
	err := s.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Stop(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		s.logger.Debug("request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Duration("duration", time.Since(start)),
		)
	})
}

func (s *Server) securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "SAMEORIGIN")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if allowed := s.allowedOrigin(origin); allowed != "" {
			w.Header().Set("Access-Control-Allow-Origin", allowed)
			w.Header().Add("Vary", "Origin")
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) allowedOrigin(origin string) string {
	if origin == "" {
		return ""
	}
	for _, allowed := range s.config.CORSOrigins {
		if allowed == "*" {
			return "*"
		}
		if origin == allowed {
			return origin
		}
	}
	return ""
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s.methodNotAllowed(w)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "healthy",
		"version": s.version,
		"uptime":  time.Since(s.startedAt).Round(time.Second).String(),
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s.methodNotAllowed(w)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleStreams(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listStreams(w)
	case http.MethodPost:
		if s.authorizeMutation(w, r) {
			s.createStream(w, r)
		}
	default:
		s.methodNotAllowed(w)
	}
}

func (s *Server) handleStreamByID(w http.ResponseWriter, r *http.Request) {
	streamID := strings.TrimPrefix(r.URL.Path, s.basePath+"/streams/")
	if streamID == "" || strings.Contains(streamID, "/") {
		s.badRequest(w, "valid stream ID required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getStream(w, streamID)
	case http.MethodDelete:
		if s.authorizeMutation(w, r) {
			s.deleteStream(w, streamID)
		}
	default:
		s.methodNotAllowed(w)
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w)
		return
	}
	s.metrics.Handler().ServeHTTP(w, r)
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":       "STREMDBC",
		"version":    s.version,
		"go_version": runtime.Version(),
		"goroutines": runtime.NumGoroutine(),
		"cpus":       runtime.NumCPU(),
		"uptime":     time.Since(s.startedAt).Round(time.Second).String(),
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w)
		return
	}
	streams := s.registry.List()
	viewers := 0
	for _, stream := range streams {
		viewers += stream.Viewers
	}

	components := make(map[string]interface{})
	s.statsMu.RLock()
	for name, provider := range s.componentStats {
		components[name] = provider()
	}
	s.statsMu.RUnlock()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"streams":       len(streams),
		"live_streams":  s.registry.LiveCount(),
		"viewers":       viewers,
		"goroutines":    runtime.NumGoroutine(),
		"memory_bytes":  mem.Alloc,
		"uptime_seconds": int64(time.Since(s.startedAt).Seconds()),
		"components":    components,
	})
}

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.methodNotAllowed(w)
		return
	}
	if s.authManager == nil {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "authentication is disabled"})
		return
	}
	if !s.authManager.ValidateAPIKey(r.Header.Get("X-API-Key")) {
		s.unauthorized(w)
		return
	}

	var req struct {
		StreamID string `json:"stream_id"`
		Action   string `json:"action"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		s.badRequest(w, "invalid request body")
		return
	}

	var (
		token string
		err   error
	)
	switch req.Action {
	case "publish":
		token, err = s.authManager.GeneratePublishToken(req.StreamID, r.Header.Get("X-API-Key"), clientIP(r))
	case "play":
		token, err = s.authManager.GeneratePlayToken(req.StreamID, clientIP(r))
	default:
		s.badRequest(w, "action must be publish or play")
		return
	}
	if err != nil {
		s.badRequest(w, err.Error())
		return
	}
	s.writeJSON(w, http.StatusCreated, map[string]string{"token": token, "action": req.Action, "stream_id": req.StreamID})
}

func (s *Server) authorizeMutation(w http.ResponseWriter, r *http.Request) bool {
	if s.authManager == nil {
		return true
	}
	if s.authManager.ValidateAPIKey(r.Header.Get("X-API-Key")) {
		return true
	}
	s.unauthorized(w)
	return false
}

func (s *Server) listStreams(w http.ResponseWriter) {
	streams := s.registry.List()
	s.writeJSON(w, http.StatusOK, map[string]interface{}{"streams": streams, "count": len(streams)})
}

func (s *Server) createStream(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		s.badRequest(w, "invalid request body")
		return
	}
	req.ID = strings.TrimSpace(req.ID)
	if req.ID == "" || strings.ContainsAny(req.ID, "/\\") {
		s.badRequest(w, "valid stream ID required")
		return
	}
	if req.Name == "" {
		req.Name = req.ID
	}

	stream, err := s.registry.Register(req.ID, req.Name)
	if err != nil {
		if errors.Is(err, core.ErrStreamExists) {
			s.conflict(w, "stream already exists")
			return
		}
		s.internalError(w, "failed to create stream")
		return
	}
	s.metrics.RecordStreamCreated()
	s.logger.Info("stream created", zap.String("id", req.ID))
	s.writeJSON(w, http.StatusCreated, stream)
}

func (s *Server) getStream(w http.ResponseWriter, id string) {
	stream, exists := s.registry.Get(id)
	if !exists {
		s.notFound(w, "stream not found")
		return
	}
	s.writeJSON(w, http.StatusOK, stream)
}

func (s *Server) deleteStream(w http.ResponseWriter, id string) {
	if err := s.registry.Delete(id); err != nil {
		if errors.Is(err, core.ErrStreamNotFound) {
			s.notFound(w, "stream not found")
			return
		}
		s.internalError(w, "failed to delete stream")
		return
	}
	s.logger.Info("stream deleted", zap.String("id", id))
	w.WriteHeader(http.StatusNoContent)
}

func clientIP(r *http.Request) string {
	if forwarded := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-For"), ",")[0]); forwarded != "" {
		return forwarded
	}
	return r.RemoteAddr
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("encode response", zap.Error(err))
	}
}

func (s *Server) badRequest(w http.ResponseWriter, message string) {
	s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": message})
}

func (s *Server) unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "ApiKey")
	s.writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
}

func (s *Server) notFound(w http.ResponseWriter, message string) {
	s.writeJSON(w, http.StatusNotFound, map[string]string{"error": message})
}

func (s *Server) conflict(w http.ResponseWriter, message string) {
	s.writeJSON(w, http.StatusConflict, map[string]string{"error": message})
}

func (s *Server) methodNotAllowed(w http.ResponseWriter) {
	s.writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
}

func (s *Server) internalError(w http.ResponseWriter, message string) {
	s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": message})
}

func (s *Server) String() string {
	return fmt.Sprintf("STREMDBC API %s", s.version)
}
