package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestResetForTest_AllowsReregistration(t *testing.T) {
	// Save default and reset
	ResetForTest()
	defer func() {
		ResetForTest()
		MustRegisterAll(prometheus.DefaultRegisterer)
	}()

	r := prometheus.NewRegistry()
	MustRegisterAll(r)

	// Verify we can register metric values
	TaskTotal.WithLabelValues("test-cluster", "running").Set(1)

	families, err := r.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	found := false
	for _, f := range families {
		if strings.Contains(f.GetName(), "task_total") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("datastream_task_total not found in independent registry")
	}
}

func TestMustRegisterAll_DoublePanics(t *testing.T) {
	ResetForTest()
	defer func() {
		ResetForTest()
		MustRegisterAll(prometheus.DefaultRegisterer)
	}()

	r := prometheus.NewRegistry()
	MustRegisterAll(r)

	defer func() {
		if recover() == nil {
			t.Error("expected panic on double MustRegisterAll, got none")
		}
	}()
	MustRegisterAll(r) // should panic — duplicate registration
}
