package metrics

import (
	"fmt"
	"strings"
	"testing"
)

func gatherText(m *Metrics) (string, error) {
	families, err := m.registry.Gather()
	if err != nil {
		return "", err
	}
	var result strings.Builder
	for _, family := range families {
		if family.GetType().String() == "GAUGE" {
			for _, metric := range family.Metric {
				result.WriteString(fmt.Sprintf("%s %f\n", family.GetName(), metric.GetGauge().GetValue()))
			}
		}
	}
	return result.String(), nil
}

func TestMetricsClampCurrentCountsAndNeverExposeNegativeLiveGauge(t *testing.T) {
	m := NewMetrics()
	m.RecordStreamOffline()
	m.RecordViewers(-5)
	m.SetCurrentState(-2, -3)
	m.RecordStreamOffline()

	data, err := gatherText(m)
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	if strings.Contains(data, "stremdbc_streams_live -") || strings.Contains(data, "stremdbc_viewers_total -") {
		t.Fatalf("negative gauge exported:\n%s", data)
	}
}
