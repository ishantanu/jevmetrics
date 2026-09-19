package exporter

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ishantanu/jevmetrics/internal/config"
	"github.com/ishantanu/jevmetrics/internal/evaluator"
	promclient "github.com/ishantanu/jevmetrics/internal/prom"
)

func TestMetricsEndpointContainsJevMetrics(t *testing.T) {
	var cfg config.Config
	cfg.Signals = []config.Signal{{Name: "checkout"}}
	e := New(promclient.NewClient("http://127.0.0.1", "", ""), evaluator.NewBaseline(), cfg)
	rr := httptest.NewRecorder()
	e.ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(rr.Body.String(), `jev_anomaly_probability{signal="checkout"}`) {
		t.Fatalf("expected jev metric, got %s", rr.Body.String())
	}
}
