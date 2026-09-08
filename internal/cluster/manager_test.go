package cluster

import (
	"math"
	"testing"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

func offlineClusterManager() *Manager {
	return &Manager{
		logger: zap.NewNop(),
		node:   &Node{ID: "local", State: "active", CPULoad: 10},
		nodes: map[string]*Node{
			"zulu":  {ID: "zulu", State: "active", CPULoad: 5},
			"alpha": {ID: "alpha", State: "active", CPULoad: 5},
			"dead":  {ID: "dead", State: "inactive", CPULoad: 1},
		},
	}
}

func TestClusterNodeSnapshotsAreSortedAndImmutable(t *testing.T) {
	manager := offlineClusterManager()
	nodes := manager.GetNodes()
	if len(nodes) != 4 || nodes[0].ID != "local" || nodes[1].ID != "alpha" || nodes[2].ID != "dead" || nodes[3].ID != "zulu" {
		t.Fatalf("nodes = %+v", nodes)
	}
	nodes[0].State = "corrupted"
	if got, _ := manager.GetNode("alpha"); got.State != "active" {
		t.Fatalf("node snapshot leaked: %+v", got)
	}
}

func TestClusterStatsAreBoundedAndBestNodeIsDeterministic(t *testing.T) {
	manager := offlineClusterManager()
	manager.UpdateNodeStats(-1, -4, 5, math.Inf(1))
	if manager.node.StreamCount != 0 || manager.node.Viewers != 0 || manager.node.CPULoad != 5 || manager.node.MemoryUsage != 0 {
		t.Fatalf("unbounded stats = %+v", manager.node)
	}
	best := manager.SelectBestNode()
	if best == nil || best.ID != "alpha" {
		t.Fatalf("best node = %+v", best)
	}
}

func TestClusterManagerRejectsMissingConfiguration(t *testing.T) {
	if _, err := NewManager(nil, &config.RedisConfig{}, zap.NewNop()); err == nil {
		t.Fatal("nil cluster config accepted")
	}
	if _, err := NewManager(&config.ClusterConfig{}, nil, zap.NewNop()); err == nil {
		t.Fatal("nil Redis config accepted")
	}
}

func TestClusterDurationMultiplicationSaturates(t *testing.T) {
	if got := multiplyDuration(time.Second, 2); got != 2*time.Second {
		t.Fatalf("duration multiplication = %s", got)
	}
	if got := multiplyDuration(time.Duration(math.MaxInt64), 2); got != time.Duration(math.MaxInt64) {
		t.Fatalf("overflowing duration multiplication = %s", got)
	}
}
