//go:build integration

package integration

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsRegistry_RoundtripWithIndependentRegistry(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	metrics.TaskTotal.WithLabelValues("itest", "running").Set(1)
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) == 0 {
		t.Fatal("no metric families gathered")
	}
	found := false
	for _, f := range families {
		if f.GetName() == "datastream_task_total" {
			found = true
		}
	}
	if !found {
		t.Error("datastream_task_total not registered in independent registry")
	}
}

func TestMetricsEndpoint_LiveHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	addr := os.Getenv("DATASTREAM_TEST_ADDR")
	if addr == "" {
		t.Skip("set DATASTREAM_TEST_ADDR to run live HTTP test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", addr+"/metrics", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "datastream_") {
		t.Errorf("response missing datastream_ metrics; first 200 chars: %s", string(body[:200]))
	}
}
