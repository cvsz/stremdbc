package auth

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/policedbc/stremdbc/internal/core"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token expired")
	ErrUnauthorized = errors.New("unauthorized")
)

const minAPIKeyLength = 16
const maxTokenLength = 16 << 10

// Manager handles authentication and authorization.
type Manager struct {
	jwtSecret      []byte
	jwtExpiry      time.Duration
	apiKeys        [][]byte
	allowAnonymous bool
}

// Claims represents stream-scoped JWT claims.
type Claims struct {
	StreamID string `json:"stream_id"`
	Action   string `json:"action"` // publish or play
	APIKey   string `json:"api_key,omitempty"`
	IP       string `json:"ip,omitempty"`
	jwt.RegisteredClaims
}

func NewManager(secret string, expiry string, apiKeys []string, allowAnonymous bool) (*Manager, error) {
	if strings.TrimSpace(secret) != secret || len(secret) < 32 || strings.IndexFunc(secret, unicode.IsControl) >= 0 {
		return nil, fmt.Errorf("JWT secret must be at least 32 characters")
	}
	expiryDuration, err := time.ParseDuration(expiry)
	if err != nil || expiryDuration <= 0 {
		return nil, fmt.Errorf("invalid JWT expiry %q", expiry)
	}

	keys := make([][]byte, 0, len(apiKeys))
	seenKeys := make(map[string]struct{}, len(apiKeys))
	for _, key := range apiKeys {
		if len(key) < minAPIKeyLength {
			return nil, fmt.Errorf("API keys must be at least %d characters", minAPIKeyLength)
		}
		if strings.TrimSpace(key) != key || strings.IndexFunc(key, func(r rune) bool {
			return unicode.IsControl(r) || unicode.IsSpace(r)
		}) >= 0 {
			return nil, fmt.Errorf("API keys must not contain surrounding whitespace or control characters")
		}
		if _, exists := seenKeys[key]; exists {
			return nil, fmt.Errorf("API keys must be unique")
		}
		seenKeys[key] = struct{}{}
		keys = append(keys, []byte(key))
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("at least one API key is required")
	}

	return &Manager{
		jwtSecret:      []byte(secret),
		jwtExpiry:      expiryDuration,
		apiKeys:        keys,
		allowAnonymous: allowAnonymous,
	}, nil
}

func (m *Manager) GeneratePublishToken(streamID, apiKey, ip string) (string, error) {
	return m.generateToken(streamID, "publish", apiKey, ip)
}

func (m *Manager) GeneratePlayToken(streamID, ip string) (string, error) {
	return m.generateToken(streamID, "play", "", ip)
}

func (m *Manager) generateToken(streamID, action, apiKey, ip string) (string, error) {
	if m == nil {
		return "", ErrUnauthorized
	}
	if err := core.ValidateStreamID(streamID); err != nil {
		return "", fmt.Errorf("stream ID is invalid: %w", err)
	}
	if strings.IndexFunc(ip, func(r rune) bool {
		return r == '\x00' || r == '\r' || r == '\n' || r == '\t'
	}) >= 0 {
		return "", fmt.Errorf("invalid client IP")
	}
	if action != "publish" && action != "play" {
		return "", fmt.Errorf("unsupported token action %q", action)
	}
	if action == "publish" && !m.ValidateAPIKey(apiKey) {
		return "", ErrUnauthorized
	}

	now := time.Now()
	claims := Claims{
		StreamID: streamID,
		Action:   action,
		IP:       ip,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "stremdbc",
			Subject:   streamID,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now.Add(-5 * time.Second)),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.jwtExpiry)),
			ID:        uuid.New().String(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.jwtSecret)
}

func (m *Manager) ValidateToken(tokenString string) (*Claims, error) {
	if m == nil {
		return nil, ErrInvalidToken
	}
	if tokenString == "" || len(tokenString) > maxTokenLength {
		return nil, ErrInvalidToken
	}
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, ErrInvalidToken
		}
		return m.jwtSecret, nil
	}, jwt.WithIssuer("stremdbc"), jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, ErrInvalidToken
	}
	if !token.Valid || claims.ExpiresAt == nil || claims.IssuedAt == nil || claims.NotBefore == nil || claims.ID == "" {
		return nil, ErrInvalidToken
	}
	if core.ValidateStreamID(claims.StreamID) != nil || claims.Subject != claims.StreamID || (claims.Action != "publish" && claims.Action != "play") || claims.APIKey != "" || strings.IndexFunc(claims.IP, func(r rune) bool {
		return r == '\x00' || r == '\r' || r == '\n' || r == '\t'
	}) >= 0 {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

func (m *Manager) ValidateAPIKey(key string) bool {
	if m == nil || len(key) < minAPIKeyLength || strings.TrimSpace(key) != key || strings.IndexFunc(key, func(r rune) bool {
		return unicode.IsControl(r) || unicode.IsSpace(r)
	}) >= 0 {
		return false
	}
	for _, configured := range m.apiKeys {
		if subtle.ConstantTimeCompare(configured, []byte(key)) == 1 {
			return true
		}
	}
	return false
}

func (m *Manager) AllowAnonymous() bool {
	return m != nil && m.allowAnonymous
}

func (m *Manager) CanPublish(claims *Claims, streamID string) bool {
	return claims != nil && claims.Action == "publish" && claims.StreamID == streamID
}

func (m *Manager) CanPlay(claims *Claims, streamID string) bool {
	return claims != nil && claims.Action == "play" && claims.StreamID == streamID
}

// CanPublishFromIP and CanPlayFromIP enforce the optional client-IP binding
// embedded in tokens generated by this manager. Tokens with no IP claim remain
// usable from any address, which supports deployments behind an explicit proxy
// that cannot preserve a stable client address.
func (m *Manager) CanPublishFromIP(claims *Claims, streamID, ip string) bool {
	return m.CanPublish(claims, streamID) && tokenIPMatches(claims, ip)
}

func (m *Manager) CanPlayFromIP(claims *Claims, streamID, ip string) bool {
	return m.CanPlay(claims, streamID) && tokenIPMatches(claims, ip)
}

func tokenIPMatches(claims *Claims, ip string) bool {
	return claims != nil && (claims.IP == "" || claims.IP == ip)
}

func (m *Manager) GenerateSignedURL(baseURL, streamID, ip string) (string, error) {
	token, err := m.GeneratePlayToken(streamID, ip)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(baseURL) != baseURL || baseURL == "" || strings.IndexFunc(baseURL, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("invalid base URL")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid base URL: %w", err)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if parsed.User != nil || parsed.Fragment != "" || (scheme != "" && (scheme != "http" && scheme != "https" || parsed.Host == "")) || (scheme == "" && parsed.Host != "") {
		return "", fmt.Errorf("invalid base URL")
	}
	query := parsed.Query()
	query.Set("token", token)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
