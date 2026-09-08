package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should validate: %v", err)
	}
	if cfg.RTSP.Port != 8554 {
		t.Fatalf("expected non-root RTSP port 8554, got %d", cfg.RTSP.Port)
	}
}

func TestAuthRequiresStrongSecret(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Auth.Enable = true
	cfg.Auth.JWTSecret = "short"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "32") {
		t.Fatalf("expected strong JWT secret validation, got %v", err)
	}
}

func TestLLHLSPartMustBeShorterThanSegment(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LLHLS.Enable = true
	cfg.LLHLS.PartDuration = 2 * time.Second
	cfg.LLHLS.SegmentDuration = time.Second
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid LL-HLS timing to fail")
	}
}

func TestClusterRequiresRedis(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Cluster.Enable = true
	cfg.Cluster.NodeID = "node-a"
	cfg.Redis.Enable = false
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected cluster without Redis to fail")
	}
}

func TestLoadParsesDurationStringsAndRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("server:\n  read_timeout: 7s\n"), 0o600); err != nil {
		t.Fatalf("write valid config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load valid config: %v", err)
	}
	if cfg.Server.ReadTimeout != 7*time.Second {
		t.Fatalf("expected read timeout 7s, got %s", cfg.Server.ReadTimeout)
	}

	if err := os.WriteFile(path, []byte("server:\n  unknown_field: true\n"), 0o600); err != nil {
		t.Fatalf("write invalid config: %v", err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "unknown_field") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestConfigRejectsInvalidTimeouts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Server.ReadTimeout = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "read_timeout") {
		t.Fatalf("expected invalid read timeout error, got %v", err)
	}
}

func TestConfigRejectsDuplicateEnabledListenerAddresses(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RTMP.Enable = true
	cfg.RTSP.Enable = true
	cfg.RTSP.Host = cfg.RTMP.Host
	cfg.RTSP.Port = cfg.RTMP.Port
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "same address") {
		t.Fatalf("expected duplicate listener error, got %v", err)
	}
}

func TestConfigRequiresAuthenticationCredential(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Auth.Enable = true
	cfg.Auth.JWTSecret = strings.Repeat("x", 32)
	cfg.Auth.AllowAnonymous = false
	cfg.Auth.APIKeys = nil
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "api_keys") {
		t.Fatalf("expected API key credential error, got %v", err)
	}
}

func TestConfigRejectsUnauthenticatedLegacyIngestWithAuthEnabled(t *testing.T) {
	for _, name := range []string{"RTMP", "RTSP"} {
		cfg := DefaultConfig()
		cfg.Auth.Enable = true
		cfg.Auth.JWTSecret = strings.Repeat("x", 32)
		cfg.Auth.APIKeys = []string{"api-key-123456789"}
		if name == "RTMP" {
			cfg.RTMP.Enable = true
		} else {
			cfg.RTSP.Enable = true
		}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "publish authentication") {
			t.Fatalf("%s with auth should be rejected, got %v", name, err)
		}
	}
}

func TestConfigAcceptsStandardWebRTCICEServerURLs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WebRTC.Enable = true
	cfg.WebRTC.UseTURN = true
	cfg.WebRTC.ICEServer.URLs = []string{
		"stun:stun.example.com:3478",
		"turn:turn.example.com:3478?transport=udp",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("standard ICE server URLs should validate: %v", err)
	}
	cfg.WebRTC.ICEServer.URLs = []string{"stun:stun.example.com:3478"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "TURN") {
		t.Fatalf("use_turn without a TURN URL should fail, got %v", err)
	}
}

func TestLoadRejectsExplicitlyMissingConfig(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("explicitly missing config should not silently load defaults")
	}
}

func TestConfigRejectsInvalidHTTPConfigurationValues(t *testing.T) {
	cfg := DefaultConfig()
	cfg.API.CORSOrigins = []string{"not-an-origin"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "CORS") {
		t.Fatalf("expected invalid CORS error, got %v", err)
	}
	cfg = DefaultConfig()
	cfg.Metrics.Path = "metrics"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "metrics.path") {
		t.Fatalf("expected invalid metrics path error, got %v", err)
	}
	cfg = DefaultConfig()
	cfg.Logging.Level = "trace"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "logging.level") {
		t.Fatalf("expected invalid logging level error, got %v", err)
	}
	for _, origin := range []string{"https://trusted.example:bad", "https://trusted.example:", "https://trusted.example:0", "https://trusted.example:65536"} {
		cfg = DefaultConfig()
		cfg.API.CORSOrigins = []string{origin}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "CORS") {
			t.Fatalf("expected invalid CORS port error for %q, got %v", origin, err)
		}
	}
	cfg = DefaultConfig()
	cfg.Server.Host = " 127.0.0.1"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "HTTP host") {
		t.Fatalf("expected whitespace host error, got %v", err)
	}
	for _, host := range []string{"bad..host", "bad_host", "-bad.example"} {
		cfg = DefaultConfig()
		cfg.Server.Host = host
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "HTTP host") {
			t.Fatalf("expected invalid hostname error for %q, got %v", host, err)
		}
	}
	cfg = DefaultConfig()
	cfg.API.BasePath = "/api/../v1"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "base_path") {
		t.Fatalf("expected dot-segment base path error, got %v", err)
	}
	cfg = DefaultConfig()
	cfg.Metrics.Path = "/api/v1/streams/demo"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("expected metrics dynamic-route conflict, got %v", err)
	}
	for _, path := range []string{"/health/live", "/health/ready"} {
		cfg = DefaultConfig()
		cfg.Metrics.Path = path
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "conflicts") {
			t.Fatalf("expected metrics health-route conflict for %q, got %v", path, err)
		}
	}
	for _, path := range []string{"/dashboard", "/dashboard/status", "/player/demo", "/hls/demo", "/llhls/demo"} {
		cfg = DefaultConfig()
		cfg.Metrics.Path = path
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "conflicts") {
			t.Fatalf("expected metrics static-route conflict for %q, got %v", path, err)
		}
	}
}

func TestConfigRejectsUnboundedPlaylistWindows(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HLS.Enable = true
	cfg.HLS.PlaylistSize = MaxPlaylistSize + 1
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "HLS playlist_size") {
		t.Fatalf("expected HLS playlist bound error, got %v", err)
	}

	cfg = DefaultConfig()
	cfg.LLHLS.Enable = true
	cfg.LLHLS.PlaylistSize = MaxPlaylistSize + 1
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "LL-HLS playlist_size") {
		t.Fatalf("expected LL-HLS playlist bound error, got %v", err)
	}
}

func TestConfigRejectsUnboundedTranscoderResources(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{name: "workers", change: func(cfg *Config) { cfg.Transcoder.WorkerCount = 257 }, want: "worker_count"},
		{name: "width", change: func(cfg *Config) { cfg.Transcoder.ABRLadder[0].Width = 16385 }, want: "ABR profile"},
		{name: "height", change: func(cfg *Config) { cfg.Transcoder.ABRLadder[0].Height = 16385 }, want: "ABR profile"},
		{name: "video bitrate", change: func(cfg *Config) { cfg.Transcoder.ABRLadder[0].Bitrate = 100000001 }, want: "ABR profile"},
		{name: "frame rate", change: func(cfg *Config) { cfg.Transcoder.ABRLadder[0].FrameRate = 241 }, want: "ABR profile"},
		{name: "audio bitrate", change: func(cfg *Config) { cfg.Transcoder.ABRLadder[0].AudioBitrate = 10000001 }, want: "ABR profile"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Transcoder.Enable = true
			test.change(cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected bounded transcoder validation error, got %v", err)
			}
		})
	}
}
