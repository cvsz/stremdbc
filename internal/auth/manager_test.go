package auth

import "testing"

const testSecret = "0123456789abcdef0123456789abcdef0123456789abcdef"

func TestPlayTokenRoundTrip(t *testing.T) {
	manager, err := NewManager(testSecret, "15m", []string{"test-key"}, false)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	token, err := manager.GeneratePlayToken("stream-1", "127.0.0.1")
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}
	claims, err := manager.ValidateToken(token)
	if err != nil {
		t.Fatalf("validate token: %v", err)
	}
	if !manager.CanPlay(claims, "stream-1") {
		t.Fatal("play token should authorize its stream")
	}
	if manager.CanPublish(claims, "stream-1") {
		t.Fatal("play token must not authorize publishing")
	}
}

func TestAPIKeyValidation(t *testing.T) {
	manager, err := NewManager(testSecret, "15m", []string{"good"}, false)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if !manager.ValidateAPIKey("good") {
		t.Fatal("configured API key should validate")
	}
	if manager.ValidateAPIKey("bad") || manager.ValidateAPIKey("") {
		t.Fatal("unknown or empty API key should not validate")
	}
}

func TestShortSecretRejected(t *testing.T) {
	if _, err := NewManager("short", "15m", nil, false); err == nil {
		t.Fatal("expected short secret to be rejected")
	}
}
