package auth

import (
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token expired")
	ErrUnauthorized = errors.New("unauthorized")
)

// Manager handles authentication and authorization
type Manager struct {
	jwtSecret      []byte
	jwtExpiry      time.Duration
	apiKeys        map[string]bool
	allowAnonymous bool
}

// Claims represents JWT claims
type Claims struct {
	StreamID   string   `json:"stream_id"`
	Action     string   `json:"action"` // "publish" or "play"
	APIKey     string   `json:"api_key,omitempty"`
	IP         string   `json:"ip,omitempty"`
	Expiration int64    `json:"exp"`
	IssuedAt   int64    `json:"iat"`
	ID         string   `json:"jti"`
	jwt.RegisteredClaims
}

// NewManager creates a new auth manager
func NewManager(secret string, expiry string, apiKeys []string, allowAnonymous bool) (*Manager, error) {
	expiryDuration, err := time.ParseDuration(expiry)
	if err != nil {
		expiryDuration = 24 * time.Hour
	}

	keyMap := make(map[string]bool)
	for _, key := range apiKeys {
		keyMap[key] = true
	}

	return &Manager{
		jwtSecret:      []byte(secret),
		jwtExpiry:      expiryDuration,
		apiKeys:        keyMap,
		allowAnonymous: allowAnonymous,
	}, nil
}

// GeneratePublishToken generates a JWT for publishing
func (m *Manager) GeneratePublishToken(streamID, apiKey, ip string) (string, error) {
	now := time.Now()
	claims := Claims{
		StreamID: streamID,
		Action:   "publish",
		APIKey:   apiKey,
		IP:       ip,
		Expiration: now.Add(m.jwtExpiry).Unix(),
		IssuedAt:   now.Unix(),
		ID:         uuid.New().String(),
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "stremdbc",
			Subject:   streamID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.jwtExpiry)),
			ID:        uuid.New().String(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.jwtSecret)
}

// GeneratePlayToken generates a JWT for playback
func (m *Manager) GeneratePlayToken(streamID, ip string) (string, error) {
	now := time.Now()
	claims := Claims{
		StreamID: streamID,
		Action:   "play",
		IP:       ip,
		Expiration: now.Add(m.jwtExpiry).Unix(),
		IssuedAt:   now.Unix(),
		ID:         uuid.New().String(),
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "stremdbc",
			Subject:   streamID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.jwtExpiry)),
			ID:        uuid.New().String(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.jwtSecret)
}

// ValidateToken validates a JWT token
func (m *Manager) ValidateToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return m.jwtSecret, nil
	})

	if err != nil {
		return nil, ErrInvalidToken
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	// Check expiration
	if time.Now().Unix() > claims.Expiration {
		return nil, ErrExpiredToken
	}

	return claims, nil
}

// ValidateAPIKey validates an API key
func (m *Manager) ValidateAPIKey(key string) bool {
	if m.allowAnonymous && key == "" {
		return true
	}
	return m.apiKeys[key]
}

// CanPublish checks if the claims allow publishing to the stream
func (m *Manager) CanPublish(claims *Claims, streamID string) bool {
	if claims == nil {
		return false
	}
	return claims.Action == "publish" && claims.StreamID == streamID
}

// CanPlay checks if the claims allow playing the stream
func (m *Manager) CanPlay(claims *Claims, streamID string) bool {
	if claims == nil {
		return false
	}
	return claims.Action == "play" && (claims.StreamID == streamID || claims.StreamID == "*")
}

// GenerateSignedURL generates a signed playback URL
func (m *Manager) GenerateSignedURL(baseURL, streamID, ip string) (string, error) {
	token, err := m.GeneratePlayToken(streamID, ip)
	if err != nil {
		return "", err
	}
	return baseURL + "?token=" + token, nil
}
