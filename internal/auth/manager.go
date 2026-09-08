package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token expired")
	ErrUnauthorized = errors.New("unauthorized")
)

// Manager handles authentication and authorization.
type Manager struct {
	jwtSecret      []byte
	jwtExpiry      time.Duration
	apiKeys        map[string]bool
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
	if len(secret) < 32 {
		return nil, fmt.Errorf("JWT secret must be at least 32 characters")
	}
	expiryDuration, err := time.ParseDuration(expiry)
	if err != nil || expiryDuration <= 0 {
		return nil, fmt.Errorf("invalid JWT expiry %q", expiry)
	}

	keyMap := make(map[string]bool, len(apiKeys))
	for _, key := range apiKeys {
		if key != "" {
			keyMap[key] = true
		}
	}

	return &Manager{
		jwtSecret:      []byte(secret),
		jwtExpiry:      expiryDuration,
		apiKeys:        keyMap,
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
	if streamID == "" {
		return "", fmt.Errorf("stream ID is required")
	}
	if action != "publish" && action != "play" {
		return "", fmt.Errorf("unsupported token action %q", action)
	}

	now := time.Now()
	claims := Claims{
		StreamID: streamID,
		Action:   action,
		APIKey:   apiKey,
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
	if !token.Valid || claims.ExpiresAt == nil {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

func (m *Manager) ValidateAPIKey(key string) bool {
	if m.allowAnonymous && key == "" {
		return true
	}
	return key != "" && m.apiKeys[key]
}

func (m *Manager) AllowAnonymous() bool {
	return m.allowAnonymous
}

func (m *Manager) CanPublish(claims *Claims, streamID string) bool {
	return claims != nil && claims.Action == "publish" && claims.StreamID == streamID
}

func (m *Manager) CanPlay(claims *Claims, streamID string) bool {
	return claims != nil && claims.Action == "play" && (claims.StreamID == streamID || claims.StreamID == "*")
}

func (m *Manager) GenerateSignedURL(baseURL, streamID, ip string) (string, error) {
	token, err := m.GeneratePlayToken(streamID, ip)
	if err != nil {
		return "", err
	}
	return baseURL + "?token=" + token, nil
}
