package config

import (
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
