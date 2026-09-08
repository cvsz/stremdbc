package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/policedbc/stremdbc/internal/auth"
	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"github.com/policedbc/stremdbc/internal/metrics"
	"go.uber.org/zap"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.DefaultConfig()
	registry := core.NewStreamRegistry(cfg)
	server := NewServer(&cfg.API, registry, metrics.NewMetrics(), zap.NewNop())
	server.SetVersion("test")
	return server
}

func TestHealthAndStats(t *testing.T) {
	server := newTestServer(t)
	for _, path := range []string{"/health", "/api/v1/stats"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		res := httptest.NewRecorder()
		server.Handler().ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			t.Fatalf("%s returned %d: %s", path, res.Code, res.Body.String())
		}
	}
}

func TestMutationRequiresAPIKeyWhenAuthEnabled(t *testing.T) {
	server := newTestServer(t)
	manager, err := auth.NewManager("0123456789abcdef0123456789abcdef0123456789abcdef", "15m", []string{"secret-key"}, false)
	if err != nil {
		t.Fatalf("new auth manager: %v", err)
	}
	server.SetAuthManager(manager)

	body := `{"id":"demo","name":"Demo"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/streams", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without API key, got %d", res.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/streams", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "secret-key")
	res = httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201 with API key, got %d: %s", res.Code, res.Body.String())
	}
}

func TestTokenEndpoint(t *testing.T) {
	server := newTestServer(t)
	manager, err := auth.NewManager("0123456789abcdef0123456789abcdef0123456789abcdef", "15m", []string{"secret-key"}, false)
	if err != nil {
		t.Fatalf("new auth manager: %v", err)
	}
	server.SetAuthManager(manager)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/token", strings.NewReader(`{"stream_id":"demo","action":"play"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "secret-key")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("expected token creation, got %d: %s", res.Code, res.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload["token"] == "" {
		t.Fatal("token response should contain a token")
	}
}
