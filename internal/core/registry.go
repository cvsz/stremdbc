package core

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
)

// StreamState represents the state of a stream
type StreamState string

const (
	StreamStateIdle      StreamState = "IDLE"
	StreamStateLive      StreamState = "LIVE"
	StreamStateRecording StreamState = "RECORDING"
	StreamStateError     StreamState = "ERROR"
)

// StreamInfo contains information about a stream
type StreamInfo struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	State          StreamState       `json:"state"`
	CreatedAt      time.Time         `json:"created_at"`
	StartedAt      *time.Time        `json:"started_at,omitempty"`
	Codec          string            `json:"codec,omitempty"`
	Bitrate        int64             `json:"bitrate,omitempty"`
	Viewers        int               `json:"viewers"`
	PublisherIP    string            `json:"publisher_ip,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	LastActivityAt time.Time         `json:"last_activity_at"`
}

// StreamRegistry manages all active streams
type StreamRegistry struct {
	mu        sync.RWMutex
	streams   map[string]*StreamInfo
	streamTTL time.Duration
}

// NewStreamRegistry creates a new stream registry
func NewStreamRegistry(cfg *config.Config) *StreamRegistry {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}
	ttl := 24 * time.Hour
	if cfg.Server.StreamTTL > 0 {
		ttl = cfg.Server.StreamTTL
	}
	return &StreamRegistry{
		streams:   make(map[string]*StreamInfo),
		streamTTL: ttl,
	}
}

// Register registers a new stream
func (r *StreamRegistry) Register(id, name string) (*StreamInfo, error) {
	if err := ValidateStreamID(id); err != nil {
		return nil, err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = id
	}
	if err := validateStreamName(name); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.streams[id]; exists {
		return nil, ErrStreamExists
	}

	now := time.Now().UTC()
	info := &StreamInfo{
		ID:             id,
		Name:           name,
		State:          StreamStateIdle,
		CreatedAt:      now,
		Viewers:        0,
		Metadata:       make(map[string]string),
		LastActivityAt: now,
	}

	r.streams[id] = info
	return cloneStreamInfo(info), nil
}

// Get retrieves a stream by ID
func (r *StreamRegistry) Get(id string) (*StreamInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.streams[id]
	if !exists {
		return nil, false
	}

	return cloneStreamInfo(info), true
}

// Update updates stream information
func (r *StreamRegistry) Update(id string, updater func(*StreamInfo)) error {
	if updater == nil {
		return ErrInvalidUpdater
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	info, exists := r.streams[id]
	if !exists {
		return ErrStreamNotFound
	}

	candidate := cloneStreamInfo(info)
	updater(candidate)
	if candidate.ID != id {
		return ErrInvalidStreamID
	}
	if err := ValidateStreamID(candidate.ID); err != nil {
		return err
	}
	candidate.Name = strings.TrimSpace(candidate.Name)
	if err := validateStreamName(candidate.Name); err != nil {
		return err
	}
	if !validState(candidate.State) {
		return ErrInvalidStreamState
	}
	if candidate.Viewers < 0 {
		return fmt.Errorf("viewer count cannot be negative")
	}
	if candidate.Bitrate < 0 {
		return fmt.Errorf("bitrate cannot be negative")
	}
	for key, value := range candidate.Metadata {
		if strings.TrimSpace(key) == "" || strings.IndexFunc(key, func(r rune) bool { return r == '\x00' || r == '\r' || r == '\n' }) >= 0 {
			return fmt.Errorf("metadata key is invalid")
		}
		if strings.IndexFunc(value, func(r rune) bool { return r == '\x00' || r == '\r' || r == '\n' }) >= 0 {
			return fmt.Errorf("metadata value is invalid")
		}
	}
	*info = *candidate
	info.Metadata = cloneMetadata(candidate.Metadata)
	return nil
}

// SetState sets the state of a stream
func (r *StreamRegistry) SetState(id string, state StreamState) error {
	if !validState(state) {
		return ErrInvalidStreamState
	}
	return r.Update(id, func(info *StreamInfo) {
		if info.State == state {
			return
		}
		now := time.Now().UTC()
		info.State = state
		if state == StreamStateLive {
			info.StartedAt = &now
		} else {
			info.StartedAt = nil
		}
		info.LastActivityAt = now
	})
}

// IncrementViewers increments the viewer count
func (r *StreamRegistry) IncrementViewers(id string) error {
	return r.Update(id, func(info *StreamInfo) {
		info.Viewers++
		info.LastActivityAt = time.Now().UTC()
	})
}

// DecrementViewers decrements the viewer count
func (r *StreamRegistry) DecrementViewers(id string) error {
	return r.Update(id, func(info *StreamInfo) {
		if info.Viewers > 0 {
			info.Viewers--
		}
		info.LastActivityAt = time.Now().UTC()
	})
}

// List returns all streams
func (r *StreamRegistry) List() []*StreamInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*StreamInfo, 0, len(r.streams))
	for _, info := range r.streams {
		result = append(result, cloneStreamInfo(info))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// Delete removes a stream from the registry
func (r *StreamRegistry) Delete(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.streams[id]; !exists {
		return ErrStreamNotFound
	}
	info := r.streams[id]
	if info.Viewers > 0 || info.State == StreamStateLive || info.State == StreamStateRecording {
		return ErrStreamActive
	}

	delete(r.streams, id)
	return nil
}

// Count returns the number of registered streams
func (r *StreamRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.streams)
}

// LiveCount returns the number of live streams
func (r *StreamRegistry) LiveCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, info := range r.streams {
		if info.State == StreamStateLive {
			count++
		}
	}
	return count
}

// Cleanup starts a background goroutine to cleanup stale streams
func (r *StreamRegistry) Cleanup(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.cleanupStaleStreams()
			}
		}
	}()
}

func (r *StreamRegistry) cleanupStaleStreams() {
	cutoff := time.Now().UTC().Add(-r.streamTTL)
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, info := range r.streams {
		if info.Viewers > 0 || info.State == StreamStateLive || info.State == StreamStateRecording {
			continue
		}
		lastActivity := info.LastActivityAt
		if lastActivity.IsZero() {
			lastActivity = info.CreatedAt
		}
		if lastActivity.Before(cutoff) {
			delete(r.streams, id)
		}
	}
}

// Errors
var (
	ErrStreamExists       = &StreamError{Message: "stream already exists"}
	ErrStreamNotFound     = &StreamError{Message: "stream not found"}
	ErrInvalidStreamID    = &StreamError{Message: "invalid stream ID"}
	ErrInvalidUpdater     = &StreamError{Message: "stream updater is required"}
	ErrInvalidStreamState = &StreamError{Message: "invalid stream state"}
	ErrStreamActive       = &StreamError{Message: "stream is active"}
)

// StreamError represents a stream-related error
type StreamError struct {
	Message string
}

func (e *StreamError) Error() string {
	return e.Message
}

func validState(state StreamState) bool {
	switch state {
	case StreamStateIdle, StreamStateLive, StreamStateRecording, StreamStateError:
		return true
	default:
		return false
	}
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	copy := make(map[string]string, len(metadata))
	for key, value := range metadata {
		copy[key] = value
	}
	return copy
}

func cloneStreamInfo(info *StreamInfo) *StreamInfo {
	if info == nil {
		return nil
	}
	copy := *info
	copy.Metadata = cloneMetadata(info.Metadata)
	if info.StartedAt != nil {
		startedAt := *info.StartedAt
		copy.StartedAt = &startedAt
	}
	return &copy
}
