package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Node struct {
	ID          string    `json:"id"`
	Host        string    `json:"host"`
	HTTPPort    int       `json:"http_port"`
	RTMPPort    int       `json:"rtmp_port"`
	SRTPort     int       `json:"srt_port"`
	WebRTCPPort int       `json:"webrtc_port"`
	LastSeen    time.Time `json:"last_seen"`
	State       string    `json:"state"`
	StreamCount int       `json:"stream_count"`
	Viewers     int       `json:"viewers"`
	CPULoad     float64   `json:"cpu_load"`
	MemoryUsage float64   `json:"memory_usage"`
}

type Manager struct {
	config            *config.ClusterConfig
	logger            *zap.Logger
	client            *redis.Client
	prefix            string
	heartbeatInterval time.Duration
	node              *Node
	mu                sync.RWMutex
	nodes             map[string]*Node
	ctx               context.Context
	cancel            context.CancelFunc
}

func NewManager(cfg *config.ClusterConfig, redisCfg *config.RedisConfig, logger *zap.Logger) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	prefix := strings.TrimSpace(redisCfg.Prefix)
	if prefix == "" {
		prefix = "stremdbc:"
	}
	if !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}
	interval := cfg.HealthCheckInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}

	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", redisCfg.Host, redisCfg.Port),
		Password: redisCfg.Password,
		DB:       redisCfg.DB,
	})

	return &Manager{
		config:            cfg,
		logger:            logger.Named("cluster"),
		client:            client,
		prefix:            prefix,
		heartbeatInterval: interval,
		nodes:             make(map[string]*Node),
		ctx:               ctx,
		cancel:            cancel,
	}, nil
}

func (m *Manager) Start(node *Node) error {
	if node == nil || strings.TrimSpace(node.ID) == "" {
		return fmt.Errorf("cluster node ID is required")
	}
	pingCtx, cancel := context.WithTimeout(m.ctx, 3*time.Second)
	defer cancel()
	if err := m.client.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("redis unavailable: %w", err)
	}

	copy := *node
	copy.State = "active"
	copy.LastSeen = time.Now()
	m.mu.Lock()
	m.node = &copy
	m.mu.Unlock()

	if err := m.registerNode(); err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}
	m.logger.Info("cluster manager started", zap.String("node_id", node.ID), zap.Duration("heartbeat", m.heartbeatInterval))
	go m.heartbeatLoop()
	go m.discoveryLoop()
	return nil
}

func (m *Manager) Stop() error {
	m.logger.Info("stopping cluster manager")
	if err := m.deregisterNode(); err != nil {
		m.logger.Error("failed to deregister node", zap.Error(err))
	}
	m.cancel()
	return m.client.Close()
}

func (m *Manager) nodeKey(id string) string {
	return m.prefix + "nodes:" + id
}

func (m *Manager) snapshotNode() *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.node == nil {
		return nil
	}
	copy := *m.node
	return &copy
}

func (m *Manager) registerNode() error {
	node := m.snapshotNode()
	if node == nil {
		return fmt.Errorf("local node is not initialized")
	}
	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("marshal node: %w", err)
	}
	ttl := 6 * m.heartbeatInterval
	if ttl < 30*time.Second {
		ttl = 30 * time.Second
	}
	return m.client.Set(m.ctx, m.nodeKey(node.ID), data, ttl).Err()
}

func (m *Manager) deregisterNode() error {
	node := m.snapshotNode()
	if node == nil {
		return nil
	}
	return m.client.Del(m.ctx, m.nodeKey(node.ID)).Err()
}

func (m *Manager) heartbeatLoop() {
	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()
			if m.node != nil {
				m.node.LastSeen = time.Now()
				m.node.State = "active"
			}
			m.mu.Unlock()
			if err := m.registerNode(); err != nil {
				m.logger.Error("cluster heartbeat failed", zap.Error(err))
			}
		}
	}
}

func (m *Manager) discoveryLoop() {
	interval := 2 * m.heartbeatInterval
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
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
	local := m.snapshotNode()
	if local == nil {
		return
	}

	discovered := make(map[string]*Node)
	iterator := m.client.Scan(m.ctx, 0, m.prefix+"nodes:*", 0).Iterator()
	for iterator.Next(m.ctx) {
		data, err := m.client.Get(m.ctx, iterator.Val()).Bytes()
		if err != nil {
			continue
		}
		var node Node
		if err := json.Unmarshal(data, &node); err != nil || node.ID == "" || node.ID == local.ID {
			continue
		}
		if time.Since(node.LastSeen) > 6*m.heartbeatInterval {
			node.State = "inactive"
		} else {
			node.State = "active"
		}
		copy := node
		discovered[node.ID] = &copy
	}
	if err := iterator.Err(); err != nil {
		m.logger.Error("failed to discover cluster nodes", zap.Error(err))
		return
	}

	m.mu.Lock()
	m.nodes = discovered
	m.mu.Unlock()
}

func (m *Manager) GetNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Node, 0, len(m.nodes)+1)
	if m.node != nil {
		copy := *m.node
		result = append(result, &copy)
	}
	for _, node := range m.nodes {
		copy := *node
		result = append(result, &copy)
	}
	return result
}

func (m *Manager) GetActiveNodes() []*Node {
	nodes := m.GetNodes()
	result := make([]*Node, 0, len(nodes))
	for _, node := range nodes {
		if node.State == "active" {
			result = append(result, node)
		}
	}
	return result
}

func (m *Manager) GetNode(id string) (*Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.node != nil && m.node.ID == id {
		copy := *m.node
		return &copy, true
	}
	node, exists := m.nodes[id]
	if !exists {
		return nil, false
	}
	copy := *node
	return &copy, true
}

func (m *Manager) UpdateNodeStats(streamCount, viewers int, cpuLoad, memoryUsage float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.node != nil {
		m.node.StreamCount = streamCount
		m.node.Viewers = viewers
		m.node.CPULoad = cpuLoad
		m.node.MemoryUsage = memoryUsage
	}
}

func (m *Manager) SelectBestNode() *Node {
	nodes := m.GetActiveNodes()
	var best *Node
	minLoad := 101.0
	for _, node := range nodes {
		if node.CPULoad < minLoad {
			copy := *node
			best = &copy
			minLoad = node.CPULoad
		}
	}
	return best
}
