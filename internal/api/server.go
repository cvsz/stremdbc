package api

import (
	"encoding/json"
	"net/http"
	"runtime"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"github.com/policedbc/stremdbc/internal/metrics"
	"go.uber.org/zap"
)

// Server represents the REST API server
type Server struct {
	config      *config.APIConfig
	registry    *core.StreamRegistry
	metrics     *metrics.Metrics
	logger      *zap.Logger
	mux         *http.ServeMux
}

// NewServer creates a new API server
func NewServer(cfg *config.APIConfig, registry *core.StreamRegistry, m *metrics.Metrics, logger *zap.Logger) *Server {
	s := &Server{
		config:   cfg,
		registry: registry,
		metrics:  m,
		logger:   logger.Named("api"),
		mux:      http.NewServeMux(),
	}

	s.registerRoutes()
	return s
}

// registerRoutes registers all API routes
func (s *Server) registerRoutes() {
	// Health endpoints
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/ready", s.handleReady)

	// Stream endpoints
	s.mux.HandleFunc("/api/v1/streams", s.handleStreams)
	s.mux.HandleFunc("/api/v1/streams/", s.handleStreamByID)

	// Metrics endpoint
	s.mux.HandleFunc("/metrics", s.handleMetrics)

	// Info endpoint
	s.mux.HandleFunc("/api/v1/info", s.handleInfo)
}

// Start starts the API server
func (s *Server) Start(addr string) error {
	server := &http.Server{
		Addr:         addr,
		Handler:      s.corsMiddleware(s.loggingMiddleware(s.mux)),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	s.logger.Info("API server started", zap.String("address", addr))
	return server.ListenAndServe()
}

// loggingMiddleware logs HTTP requests
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

// corsMiddleware adds CORS headers
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// handleHealth handles health check requests
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

// handleReady handles readiness check requests
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	// Check if server is ready to accept streams
	s.writeJSON(w, http.StatusOK, map[string]string{
		"status": "ready",
	})
}

// handleStreams handles stream list and creation requests
func (s *Server) handleStreams(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listStreams(w, r)
	case http.MethodPost:
		s.createStream(w, r)
	default:
		s.methodNotAllowed(w)
	}
}

// handleStreamByID handles individual stream requests
func (s *Server) handleStreamByID(w http.ResponseWriter, r *http.Request) {
	// Extract stream ID from path
	streamID := r.URL.Path[len("/api/v1/streams/"):]
	if streamID == "" {
		s.badRequest(w, "stream ID required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getStream(w, r, streamID)
	case http.MethodDelete:
		s.deleteStream(w, r, streamID)
	default:
		s.methodNotAllowed(w)
	}
}

// handleMetrics handles Prometheus metrics requests
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.metrics.Handler().ServeHTTP(w, r)
}

// handleInfo handles server info requests
func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":       "STREMDBC",
		"version":    "0.1.0",
		"go_version": runtime.Version(),
		"goroutines": runtime.NumGoroutine(),
		"cpus":       runtime.NumCPU(),
	})
}

// listStreams returns a list of all streams
func (s *Server) listStreams(w http.ResponseWriter, r *http.Request) {
	streams := s.registry.List()
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"streams": streams,
		"count":   len(streams),
	})
}

// createStream creates a new stream
func (s *Server) createStream(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.badRequest(w, "invalid request body")
		return
	}

	if req.ID == "" {
		s.badRequest(w, "stream ID required")
		return
	}

	if req.Name == "" {
		req.Name = req.ID
	}

	stream, err := s.registry.Register(req.ID, req.Name)
	if err != nil {
		if err == core.ErrStreamExists {
			s.conflict(w, "stream already exists")
			return
		}
		s.internalError(w, "failed to create stream")
		return
	}

	s.logger.Info("stream created", zap.String("id", req.ID))
	s.writeJSON(w, http.StatusCreated, stream)
}

// getStream returns information about a specific stream
func (s *Server) getStream(w http.ResponseWriter, r *http.Request, id string) {
	stream, exists := s.registry.Get(id)
	if !exists {
		s.notFound(w, "stream not found")
		return
	}

	s.writeJSON(w, http.StatusOK, stream)
}

// deleteStream deletes a stream
func (s *Server) deleteStream(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.registry.Delete(id); err != nil {
		if err == core.ErrStreamNotFound {
			s.notFound(w, "stream not found")
			return
		}
		s.internalError(w, "failed to delete stream")
		return
	}

	s.logger.Info("stream deleted", zap.String("id", id))
	w.WriteHeader(http.StatusNoContent)
}

// Helper methods

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("encode response", zap.Error(err))
	}
}

func (s *Server) badRequest(w http.ResponseWriter, message string) {
	s.writeJSON(w, http.StatusBadRequest, map[string]string{
		"error": message,
	})
}

func (s *Server) notFound(w http.ResponseWriter, message string) {
	s.writeJSON(w, http.StatusNotFound, map[string]string{
		"error": message,
	})
}

func (s *Server) conflict(w http.ResponseWriter, message string) {
	s.writeJSON(w, http.StatusConflict, map[string]string{
		"error": message,
	})
}

func (s *Server) methodNotAllowed(w http.ResponseWriter) {
	s.writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
		"error": "method not allowed",
	})
}

func (s *Server) internalError(w http.ResponseWriter, message string) {
	s.writeJSON(w, http.StatusInternalServerError, map[string]string{
		"error": message,
	})
}
