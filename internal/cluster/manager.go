package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

// Node represents a cluster node
type Node struct {
	ID           string    `json:"id"`
	Host         string    `json:"host"`
	HTTPPort     int       `json:"http_port"`
	RTMPPort     int       `json:"rtmp_port"`
	SRTPort      int       `json:"srt_port"`
	WebRTCPPort  int       `json:"webrtc_port"`
	LastSeen     time.Time `json:"last_seen"`
	State        string    `json:"state"` // "active", "inactive", "draining"
	StreamCount  int       `json:"stream_count"`
	Viewers      int       `json:"viewers"`
	CPULoad      float64   `json:"cpu_load"`
	MemoryUsage  float64   `json:"memory_usage"`
}

// Manager handles cluster coordination
type Manager struct {
	config  *config.ClusterConfig
	logger  *zap.Logger
	client  *redis.Client
	node    *Node
	mu      sync.Mutex
	nodes   map[string]*Node
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewManager creates a new cluster manager
func NewManager(cfg *config.ClusterConfig, redisCfg *config.RedisConfig, logger *zap.Logger) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	var client *redis.Client
	if redisCfg.Enable {
		client = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", redisCfg.Host, redisCfg.Port),
			Password: redisCfg.Password,
			DB:       redisCfg.DB,
		})
	}

	return &Manager{
		config:  cfg,
		logger:  logger.Named("cluster"),
		client:  client,
		nodes:   make(map[string]*Node),
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// Start starts the cluster manager
func (m *Manager) Start(node *Node) error {
	m.mu.Lock()
	m.node = node
	m.mu.Unlock()

	m.logger.Info("starting cluster manager",
		zap.String("node_id", node.ID),
	)

	// Register node
	if err := m.registerNode(); err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	// Start heartbeat
	go m.heartbeatLoop()

	// Start node discovery
	go m.discoveryLoop()

	return nil
}

// Stop stops the cluster manager
func (m *Manager) Stop() error {
	m.logger.Info("stopping cluster manager")

	// Deregister node
	if err := m.deregisterNode(); err != nil {
		m.logger.Error("failed to deregister node", zap.Error(err))
	}

	m.cancel()
	return nil
}

func (m *Manager) registerNode() error {
	if m.client == nil {
		return nil
	}

	key := fmt.Sprintf("stremdbc:nodes:%s", m.node.ID)
	data, _ := json.Marshal(m.node)

	return m.client.Set(m.ctx, key, data, 30*time.Second).Err()
}

func (m *Manager) deregisterNode() error {
	if m.client == nil {
		return nil
	}

	key := fmt.Sprintf("stremdbc:nodes:%s", m.node.ID)
	return m.client.Del(m.ctx, key).Err()
}

func (m *Manager) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.node.LastSeen = time.Now()
			if err := m.registerNode(); err != nil {
				m.logger.Error("heartbeat failed", zap.Error(err))
			}
		}
	}
}

func (m *Manager) discoveryLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.discoverNodes()
		}
	}
}

func (m *Manager) discoverNodes() {
	if m.client == nil {
		return
	}

	keys, err := m.client.Keys(m.ctx, "stremdbc:nodes:*").Result()
	if err != nil {
		m.logger.Error("failed to discover nodes", zap.Error(err))
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, key := range keys {
		data, err := m.client.Get(m.ctx, key).Result()
		if err != nil {
			continue
		}

		var node Node
		if err := json.Unmarshal([]byte(data), &node); err != nil {
			continue
		}

		// Skip self
		if node.ID == m.node.ID {
			continue
		}

		// Check if node is alive (seen within last 30 seconds)
		if time.Since(node.LastSeen) > 30*time.Second {
			node.State = "inactive"
		} else {
			node.State = "active"
		}

		m.nodes[node.ID] = &node
	}
}

// GetNodes returns all known nodes
func (m *Manager) GetNodes() []*Node {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		result = append(result, node)
	}
	return result
}

// GetActiveNodes returns active nodes
func (m *Manager) GetActiveNodes() []*Node {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*Node, 0)
	for _, node := range m.nodes {
		if node.State == "active" {
			result = append(result, node)
		}
	}
	return result
}

// GetNode returns a node by ID
func (m *Manager) GetNode(id string) (*Node, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, exists := m.nodes[id]
	return node, exists
}

// UpdateNodeStats updates node statistics
func (m *Manager) UpdateNodeStats(streamCount, viewers int, cpuLoad, memoryUsage float64) {
	m.mu.Lock()
	if m.node != nil {
		m.node.StreamCount = streamCount
		m.node.Viewers = viewers
		m.node.CPULoad = cpuLoad
		m.node.MemoryUsage = memoryUsage
	}
	m.mu.Unlock()
}

// SelectBestNode selects the best node for a new stream based on load
func (m *Manager) SelectBestNode() *Node {
	m.mu.Lock()
	defer m.mu.Unlock()

	var best *Node
	minLoad := 100.0

	for _, node := range m.nodes {
		if node.State == "active" && node.CPULoad < minLoad {
			minLoad = node.CPULoad
			best = node
		}
	}

	return best
}
