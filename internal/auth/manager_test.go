package auth

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const testSecret = "0123456789abcdef0123456789abcdef0123456789abcdef"
const testAPIKey = "test-api-key-123456"

func TestPlayTokenRoundTrip(t *testing.T) {
	manager, err := NewManager(testSecret, "15m", []string{testAPIKey}, false)
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
	manager, err := NewManager(testSecret, "15m", []string{testAPIKey}, false)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if !manager.ValidateAPIKey(testAPIKey) {
		t.Fatal("configured API key should validate")
	}
	if manager.ValidateAPIKey("bad") || manager.ValidateAPIKey("") {
		t.Fatal("unknown or empty API key should not validate")
	}
}

func TestNewManagerRejectsInvalidOrMissingAPIKeys(t *testing.T) {
	for _, keys := range [][]string{nil, []string{""}, []string{"short"}, []string{testAPIKey, testAPIKey}, []string{"bad key with spaces"}, []string{"bad\nkey-123456789"}} {
		if _, err := NewManager(testSecret, "15m", keys, false); err == nil {
			t.Fatalf("API keys %q should be rejected", keys)
		}
	}
}

func TestTokenIPBindingIsEnforced(t *testing.T) {
	manager, err := NewManager(testSecret, "15m", []string{testAPIKey}, false)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	token, err := manager.GeneratePlayToken("stream-1", "192.0.2.10")
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}
	claims, err := manager.ValidateToken(token)
	if err != nil {
		t.Fatalf("validate token: %v", err)
	}
	if !manager.CanPlayFromIP(claims, "stream-1", "192.0.2.10") {
		t.Fatal("token should authorize the bound IP")
	}
	if manager.CanPlayFromIP(claims, "stream-1", "192.0.2.11") {
		t.Fatal("token should reject a different IP")
	}
}

func TestShortSecretRejected(t *testing.T) {
	if _, err := NewManager("short", "15m", nil, false); err == nil {
		t.Fatal("expected short secret to be rejected")
	}
}

func TestValidateTokenRejectsIncompleteClaims(t *testing.T) {
	manager, err := NewManager(testSecret, "15m", []string{testAPIKey}, false)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": "stremdbc",
		"exp": time.Now().Add(time.Hour).Unix(),
		"sub": "stream-1",
	})
	signed, err := token.SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	if _, err := manager.ValidateToken(signed); err == nil {
		t.Fatal("token without stream action must be rejected")
	}
}

func TestValidateTokenRequiresIssuedClaims(t *testing.T) {
	manager, err := NewManager(testSecret, "15m", []string{testAPIKey}, false)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	claims := jwt.MapClaims{
		"iss":       "stremdbc",
		"sub":       "stream-1",
		"stream_id": "stream-1",
		"action":    "play",
		"exp":       time.Now().Add(time.Hour).Unix(),
	}
	signedToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	if _, err := manager.ValidateToken(signedToken); err == nil {
		t.Fatal("token without issued-at, not-before, and token ID must be rejected")
	}
}

func TestPublishTokenDoesNotContainAPISecret(t *testing.T) {
	manager, err := NewManager(testSecret, "15m", []string{testAPIKey}, false)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	token, err := manager.GeneratePublishToken("stream-1", testAPIKey, "127.0.0.1")
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}
	if strings.Contains(token, testAPIKey) {
		t.Fatal("signed token must not expose the API key")
	}
	claims, err := manager.ValidateToken(token)
	if err != nil {
		t.Fatalf("validate token: %v", err)
	}
	if claims.APIKey != "" {
		t.Fatalf("expected API key claim to be empty, got %q", claims.APIKey)
	}
}

func TestNilManagerTokenGenerationFailsClosed(t *testing.T) {
	var manager *Manager
	if _, err := manager.GeneratePlayToken("stream-1", ""); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("nil manager error = %v", err)
	}
}

func TestGenerateSignedURLRejectsUnsafeBaseURLs(t *testing.T) {
	manager, err := NewManager(testSecret, "15m", []string{testAPIKey}, false)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	for _, baseURL := range []string{"javascript:alert(1)", "//attacker.example/path", "http://user:pass@example.test/path"} {
		if _, err := manager.GenerateSignedURL(baseURL, "stream-1", ""); err == nil {
			t.Fatalf("unsafe base URL %q was accepted", baseURL)
		}
	}
}
