package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

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
	wg                sync.WaitGroup
	lifecycleMu       sync.Mutex
	started           bool
	stopped           bool
}

func NewManager(cfg *config.ClusterConfig, redisCfg *config.RedisConfig, logger *zap.Logger) (*Manager, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cluster configuration is required")
	}
	if redisCfg == nil {
		return nil, fmt.Errorf("redis configuration is required")
	}
	copyCfg := *cfg
	copyRedis := *redisCfg
	if !copyRedis.Enable {
		return nil, fmt.Errorf("cluster mode requires Redis to be enabled")
	}
	redisHost := strings.TrimSpace(copyRedis.Host)
	if redisHost == "" || strings.ContainsAny(redisHost, "/?#\\[]\x00\r\n\t ") {
		return nil, fmt.Errorf("redis host is required")
	}
	copyRedis.Host = redisHost
	if strings.Contains(redisHost, ":") {
		ipHost := redisHost
		if zone := strings.LastIndexByte(ipHost, '%'); zone >= 0 {
			ipHost = ipHost[:zone]
		}
		if net.ParseIP(ipHost) == nil {
			return nil, fmt.Errorf("invalid Redis host: %q", copyRedis.Host)
		}
	}
	if copyRedis.Port <= 0 || copyRedis.Port > 65535 {
		return nil, fmt.Errorf("invalid Redis port: %d", copyRedis.Port)
	}
	prefix := strings.TrimSpace(copyRedis.Prefix)
	if prefix == "" {
		prefix = "stremdbc:"
	}
	if strings.IndexFunc(prefix, unicode.IsControl) >= 0 {
		return nil, fmt.Errorf("redis prefix contains a control character")
	}
	if copyRedis.DB < 0 || copyRedis.DB > 15 {
		return nil, fmt.Errorf("redis DB must be between 0 and 15")
	}
	if !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}
	interval := copyCfg.HealthCheckInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := redis.NewClient(&redis.Options{
		Addr:     net.JoinHostPort(copyRedis.Host, fmt.Sprintf("%d", copyRedis.Port)),
		Password: copyRedis.Password,
		DB:       copyRedis.DB,
	})
	return &Manager{
		config:            &copyCfg,
		logger:            logger.Named("cluster"),
		client:            client,
		prefix:            prefix,
		heartbeatInterval: interval,
		nodes:             make(map[string]*Node),
		ctx:               ctx,
		cancel:            cancel,
	}, nil
}

// Start registers a node and starts heartbeat/discovery loops. It is
// idempotent while running and does not report success if Redis is unavailable.
func (m *Manager) Start(node *Node) error {
	if err := validateNode(node); err != nil {
		return err
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.stopped {
		return errors.New("cluster manager is stopped")
	}
	if m.started {
		return nil
	}
	pingCtx, cancel := context.WithTimeout(m.ctx, 3*time.Second)
	defer cancel()
	if err := m.client.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("redis unavailable: %w", err)
	}
	copy := *node
	copy.State = "active"
	copy.LastSeen = time.Now().UTC()
	m.mu.Lock()
	m.node = &copy
	m.mu.Unlock()
	if err := m.registerNode(m.ctx); err != nil {
		m.mu.Lock()
		m.node = nil
		m.mu.Unlock()
		return fmt.Errorf("failed to register node: %w", err)
	}
	m.started = true
	m.wg.Add(2)
	m.logger.Info("cluster manager started", zap.String("node_id", node.ID), zap.Duration("heartbeat", m.heartbeatInterval))
	go func() {
		defer m.wg.Done()
		m.heartbeatLoop()
	}()
	go func() {
		defer m.wg.Done()
		m.discoveryLoop()
	}()
	return nil
}

// Stop stops loops, removes the node from Redis, and closes the client.
func (m *Manager) Stop() error {
	m.lifecycleMu.Lock()
	if m.stopped {
		m.lifecycleMu.Unlock()
		return nil
	}
	m.stopped = true
	m.cancel()
	m.started = false
	m.lifecycleMu.Unlock()
	m.logger.Info("stopping cluster manager")

	m.wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	deregisterErr := m.deregisterNode(ctx)
	cancel()
	closeErr := m.client.Close()
	m.mu.Lock()
	m.node = nil
	m.nodes = make(map[string]*Node)
	m.mu.Unlock()
	if deregisterErr != nil {
		m.logger.Error("failed to deregister node", zap.Error(deregisterErr))
	}
	if closeErr != nil {
		return closeErr
	}
	return deregisterErr
}

func (m *Manager) nodeKey(id string) string { return m.prefix + "nodes:" + id }

func (m *Manager) snapshotNode() *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.node == nil {
		return nil
	}
	copy := *m.node
	return &copy
}

func (m *Manager) registerNode(parent context.Context) error {
	node := m.snapshotNode()
	if node == nil {
		return fmt.Errorf("local node is not initialized")
	}
	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("marshal node: %w", err)
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	ttl := multiplyDuration(m.heartbeatInterval, 6)
	if ttl < 30*time.Second {
		ttl = 30 * time.Second
	}
	return m.client.Set(ctx, m.nodeKey(node.ID), data, ttl).Err()
}

func (m *Manager) deregisterNode(ctx context.Context) error {
	node := m.snapshotNode()
	if node == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return m.client.Del(ctx, m.nodeKey(node.ID)).Err()
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
				m.node.LastSeen = time.Now().UTC()
				m.node.State = "active"
			}
			m.mu.Unlock()
			if err := m.registerNode(m.ctx); err != nil && m.ctx.Err() == nil {
				m.logger.Error("cluster heartbeat failed", zap.Error(err))
			}
		}
	}
}

func (m *Manager) discoveryLoop() {
	interval := multiplyDuration(m.heartbeatInterval, 2)
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
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()
	discovered := make(map[string]*Node)
	iterator := m.client.Scan(ctx, 0, m.prefix+"nodes:*", 0).Iterator()
	for iterator.Next(ctx) {
		data, err := m.client.Get(ctx, iterator.Val()).Bytes()
		if err != nil {
			continue
		}
		var node Node
		if err := json.Unmarshal(data, &node); err != nil || validateNode(&node) != nil || node.ID == local.ID {
			continue
		}
		node = sanitizeNode(node)
		if node.LastSeen.IsZero() || time.Since(node.LastSeen) > multiplyDuration(m.heartbeatInterval, 6) {
			node.State = "inactive"
		} else {
			node.State = "active"
		}
		copy := node
		discovered[node.ID] = &copy
	}
	if err := iterator.Err(); err != nil && m.ctx.Err() == nil {
		m.logger.Error("failed to discover cluster nodes", zap.Error(err))
		return
	}
	m.mu.Lock()
	m.nodes = discovered
	m.mu.Unlock()
}

func (m *Manager) GetNodes() []*Node {
	m.mu.RLock()
	result := make([]*Node, 0, len(m.nodes)+1)
	if m.node != nil {
		copy := *m.node
		result = append(result, &copy)
	}
	for _, node := range m.nodes {
		copy := *node
		result = append(result, &copy)
	}
	m.mu.RUnlock()
	if len(result) > 1 {
		sort.SliceStable(result[1:], func(i, j int) bool { return result[1+i].ID < result[1+j].ID })
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
	if m.node == nil {
		return
	}
	m.node.StreamCount = maxInt(streamCount, 0)
	m.node.Viewers = maxInt(viewers, 0)
	m.node.CPULoad = boundedPercent(cpuLoad)
	m.node.MemoryUsage = boundedPercent(memoryUsage)
}

func (m *Manager) SelectBestNode() *Node {
	nodes := m.GetActiveNodes()
	var best *Node
	for _, node := range nodes {
		if best == nil || node.CPULoad < best.CPULoad || (node.CPULoad == best.CPULoad && node.ID < best.ID) {
			copy := *node
			best = &copy
		}
	}
	return best
}

func validateNode(node *Node) error {
	if node == nil || !validNodeID(node.ID) {
		return fmt.Errorf("cluster node ID is required and must be safe")
	}
	if strings.TrimSpace(node.Host) == "" || strings.ContainsAny(node.Host, "/?#\\[]\x00\r\n\t ") {
		return fmt.Errorf("cluster node host is invalid")
	}
	if strings.Contains(node.Host, ":") {
		ipHost := node.Host
		if zone := strings.LastIndexByte(ipHost, '%'); zone >= 0 {
			ipHost = ipHost[:zone]
		}
		if net.ParseIP(ipHost) == nil {
			return fmt.Errorf("cluster node host is invalid")
		}
	}
	for name, port := range map[string]int{"HTTP": node.HTTPPort, "RTMP": node.RTMPPort, "SRT": node.SRTPort, "WebRTC": node.WebRTCPPort} {
		if port < 0 || port > 65535 {
			return fmt.Errorf("invalid cluster node %s port: %d", name, port)
		}
	}
	return nil
}

func validNodeID(id string) bool {
	if id == "" || len(id) > 128 || strings.TrimSpace(id) != id {
		return false
	}
	for i, r := range id {
		if i == 0 && !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
		if !(r == '-' || r == '_' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func sanitizeNode(node Node) Node {
	node.StreamCount = maxInt(node.StreamCount, 0)
	node.Viewers = maxInt(node.Viewers, 0)
	node.CPULoad = boundedPercent(node.CPULoad)
	node.MemoryUsage = boundedPercent(node.MemoryUsage)
	return node
}

func boundedPercent(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func multiplyDuration(value time.Duration, factor int64) time.Duration {
	if value <= 0 || factor <= 0 {
		return 0
	}
	maxDuration := time.Duration(math.MaxInt64)
	if value > maxDuration/time.Duration(factor) {
		return maxDuration
	}
	return value * time.Duration(factor)
}

func maxInt(value, minimum int) int {
	if value < minimum {
		return minimum
	}
	return value
}
