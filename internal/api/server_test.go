package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	for _, path := range []string{"/health", "/health/live", "/health/ready", "/api/v1/stats"} {
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
	manager, err := auth.NewManager("0123456789abcdef0123456789abcdef0123456789abcdef", "15m", []string{"secret-key-123456"}, false)
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
	req.Header.Set("X-API-Key", "secret-key-123456")
	res = httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201 with API key, got %d: %s", res.Code, res.Body.String())
	}
}

func TestAnonymousPlaybackDoesNotAuthorizeManagementMutations(t *testing.T) {
	server := newTestServer(t)
	manager, err := auth.NewManager("0123456789abcdef0123456789abcdef0123456789abcdef", "15m", []string{"secret-key-123456"}, true)
	if err != nil {
		t.Fatalf("new auth manager: %v", err)
	}
	server.SetAuthManager(manager)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/streams", strings.NewReader(`{"id":"demo"}`))
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous mutation returned %d: %s", res.Code, res.Body.String())
	}
}

func TestTokenEndpoint(t *testing.T) {
	server := newTestServer(t)
	manager, err := auth.NewManager("0123456789abcdef0123456789abcdef0123456789abcdef", "15m", []string{"secret-key-123456"}, false)
	if err != nil {
		t.Fatalf("new auth manager: %v", err)
	}
	server.SetAuthManager(manager)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/token", strings.NewReader(`{"stream_id":"demo","action":"play"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "secret-key-123456")
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

func TestCreateStreamRejectsTrailingJSONAndUnsafeID(t *testing.T) {
	server := newTestServer(t)
	for _, body := range []string{
		`{"id":"demo","name":"Demo"}{"id":"second"}`,
		`{"id":"../secret","name":"Secret"}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/streams", strings.NewReader(body))
		res := httptest.NewRecorder()
		server.Handler().ServeHTTP(res, req)
		if res.Code != http.StatusBadRequest {
			t.Fatalf("body %q: expected 400, got %d: %s", body, res.Code, res.Body.String())
		}
	}
}

func TestCORSRejectsDisallowedPreflight(t *testing.T) {
	server := newTestServer(t)
	server.config.CORSOrigins = []string{"https://trusted.example"}
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/streams", nil)
	req.Header.Set("Origin", "https://attacker.example")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("expected disallowed preflight to return 403, got %d", res.Code)
	}
}

func TestStaticPlaybackRequiresPlayTokenWhenAuthenticationIsRequired(t *testing.T) {
	server := newTestServer(t)
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "demo"), 0o700); err != nil {
		t.Fatalf("create stream directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "demo", "index.m3u8"), []byte("#EXTM3U\n"), 0o600); err != nil {
		t.Fatalf("write playlist: %v", err)
	}
	server.SetStaticRoutes(dir, "", "", "")
	manager, err := auth.NewManager("0123456789abcdef0123456789abcdef0123456789abcdef", "15m", []string{"secret-key-123456"}, false)
	if err != nil {
		t.Fatalf("new auth manager: %v", err)
	}
	server.SetAuthManager(manager)

	req := httptest.NewRequest(http.MethodGet, "/hls/demo/index.m3u8", nil)
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized playback, got %d", res.Code)
	}

	token, err := manager.GeneratePlayToken("demo", clientIP(req))
	if err != nil {
		t.Fatalf("generate play token: %v", err)
	}
	req = httptest.NewRequest(http.MethodGet, "/hls/demo/index.m3u8?token="+token, nil)
	res = httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected authorized playback, got %d: %s", res.Code, res.Body.String())
	}
}

func TestStaticDirectoryIndexCannotEscapeThroughSymlink(t *testing.T) {
	server := newTestServer(t)
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.html"), []byte("secret"), 0o600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o700); err != nil {
		t.Fatalf("create static subdirectory: %v", err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.html"), filepath.Join(root, "sub", "index.html")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	server.SetStaticRoutes("", "", "", root)
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/dashboard/sub/", nil))
	if res.Code != http.StatusNotFound {
		t.Fatalf("symlink escape returned %d: %s", res.Code, res.Body.String())
	}
}

func TestMetricsConfigurationControlsOnlyConfiguredPath(t *testing.T) {
	server := newTestServer(t)
	server.SetMetricsConfig(true, "/internal/metrics")
	for _, path := range []string{"/metrics", "/internal/metrics"} {
		res := httptest.NewRecorder()
		server.Handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, path, nil))
		if path == "/metrics" && res.Code != http.StatusNotFound {
			t.Fatalf("default metrics path returned %d", res.Code)
		}
		if path == "/internal/metrics" && res.Code != http.StatusOK {
			t.Fatalf("configured metrics path returned %d", res.Code)
		}
	}
}

func TestMetricsConfigurationRejectsDynamicRouteShadowing(t *testing.T) {
	server := newTestServer(t)
	server.SetMetricsConfig(true, "/api/v1/streams/demo")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/v1/streams/demo", nil))
	if res.Code != http.StatusNotFound {
		t.Fatalf("shadowed stream route returned %d: %s", res.Code, res.Body.String())
	}
}

func TestMetricsConfigurationRejectsStaticRouteShadowing(t *testing.T) {
	server := newTestServer(t)
	server.SetStaticRoutes("", "", "", t.TempDir())
	server.SetMetricsConfig(true, "/dashboard")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
	if res.Code != http.StatusTemporaryRedirect {
		t.Fatalf("static dashboard route was shadowed by metrics, got %d: %s", res.Code, res.Body.String())
	}
}
