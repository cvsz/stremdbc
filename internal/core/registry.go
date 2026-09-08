package core

import (
	"context"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
)

// StreamState represents the state of a stream
type StreamState string

const (
	StreamStateIdle     StreamState = "IDLE"
	StreamStateLive     StreamState = "LIVE"
	StreamStateRecording StreamState = "RECORDING"
	StreamStateError    StreamState = "ERROR"
)

// StreamInfo contains information about a stream
type StreamInfo struct {
	ID           string      `json:"id"`
	Name         string      `json:"name"`
	State        StreamState `json:"state"`
	CreatedAt    time.Time   `json:"created_at"`
	StartedAt    *time.Time  `json:"started_at,omitempty"`
	Codec        string      `json:"codec,omitempty"`
	Bitrate      int64       `json:"bitrate,omitempty"`
	Viewers      int         `json:"viewers"`
	PublisherIP  string      `json:"publisher_ip,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// StreamRegistry manages all active streams
type StreamRegistry struct {
	mu      sync.RWMutex
	streams map[string]*StreamInfo
	config  *config.Config
}

// NewStreamRegistry creates a new stream registry
func NewStreamRegistry(cfg *config.Config) *StreamRegistry {
	return &StreamRegistry{
		streams: make(map[string]*StreamInfo),
		config:  cfg,
	}
}

// Register registers a new stream
func (r *StreamRegistry) Register(id, name string) (*StreamInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.streams[id]; exists {
		return nil, ErrStreamExists
	}

	now := time.Now()
	info := &StreamInfo{
		ID:        id,
		Name:      name,
		State:     StreamStateIdle,
		CreatedAt: now,
		Viewers:   0,
		Metadata:  make(map[string]string),
	}

	r.streams[id] = info
	return info, nil
}

// Get retrieves a stream by ID
func (r *StreamRegistry) Get(id string) (*StreamInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.streams[id]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	copy := *info
	return &copy, true
}

// Update updates stream information
func (r *StreamRegistry) Update(id string, updater func(*StreamInfo)) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, exists := r.streams[id]
	if !exists {
		return ErrStreamNotFound
	}

	updater(info)
	return nil
}

// SetState sets the state of a stream
func (r *StreamRegistry) SetState(id string, state StreamState) error {
	return r.Update(id, func(info *StreamInfo) {
		info.State = state
		if state == StreamStateLive {
			now := time.Now()
			info.StartedAt = &now
		}
	})
}

// IncrementViewers increments the viewer count
func (r *StreamRegistry) IncrementViewers(id string) error {
	return r.Update(id, func(info *StreamInfo) {
		info.Viewers++
	})
}

// DecrementViewers decrements the viewer count
func (r *StreamRegistry) DecrementViewers(id string) error {
	return r.Update(id, func(info *StreamInfo) {
		if info.Viewers > 0 {
			info.Viewers--
		}
	})
}

// List returns all streams
func (r *StreamRegistry) List() []*StreamInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*StreamInfo, 0, len(r.streams))
	for _, info := range r.streams {
		copy := *info
		result = append(result, &copy)
	}
	return result
}

// Delete removes a stream from the registry
func (r *StreamRegistry) Delete(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.streams[id]; !exists {
		return ErrStreamNotFound
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
	// Implement cleanup logic for stale streams
	// This can be enhanced based on requirements
}

// Errors
var (
	ErrStreamExists    = &StreamError{Message: "stream already exists"}
	ErrStreamNotFound  = &StreamError{Message: "stream not found"}
)

// StreamError represents a stream-related error
type StreamError struct {
	Message string
}

func (e *StreamError) Error() string {
	return e.Message
}
