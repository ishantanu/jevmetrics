package jevmetricsprocessor

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/metric"
)

type policyCounter struct {
	metric.Int64Counter
	value  int64
	labels map[string]string
}

func (c *policyCounter) Add(_ context.Context, n int64, opts ...metric.AddOption) {
	c.value += n
	c.labels = map[string]string{}
	attrs := metric.NewAddConfig(opts).Attributes()
	for _, a := range attrs.ToSlice() {
		c.labels[string(a.Key)] = a.Value.AsString()
	}
}

func TestShadowMatchesFiltering(t *testing.T) {
	data, err := os.ReadFile("testdata/shadow_policy.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name                       string
		Protected, Cached, Expired bool
		Score                      float64
		Decision, Reason           string
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		for _, mode := range []string{"annotate", "reduce"} {
			t.Run(tc.Name+"/"+mode, func(t *testing.T) {
				cfg := testConfig()
				cfg.Mode = mode
				if tc.Protected {
					cfg.Policy.ProtectedMetrics = []string{"custom.requests"}
				}
				input := metricInput()
				input.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().AppendEmpty().SetIntValue(9)
				var output pmetric.Metrics
				p := testProcessor(t, cfg, func(_ context.Context, md pmetric.Metrics) error { output = md; return nil })
				instruments, points := &policyCounter{}, &policyCounter{}
				p.telemetry.policyInstruments, p.telemetry.policyDatapoints = instruments, points
				if tc.Cached {
					at := time.Now()
					if tc.Expired {
						at = at.Add(-2 * cfg.scoreTTL())
					}
					p.scores.put(inputKey(input), metricScore{Keep: tc.Score, ScoredAt: at})
				}
				if err := p.ConsumeMetrics(context.Background(), input); err != nil {
					t.Fatal(err)
				}
				for _, counter := range []*policyCounter{instruments, points} {
					if len(counter.labels) != 3 || counter.labels["mode"] != mode || counter.labels["decision"] != tc.Decision || counter.labels["reason"] != tc.Reason {
						t.Fatalf("wrong policy labels: %v", counter.labels)
					}
				}
				if instruments.value != 1 || points.value != 2 {
					t.Fatal("must count original input only")
				}
				want := 1
				if mode == "reduce" && tc.Decision == "drop" {
					want = 0
				}
				if mode == "annotate" && tc.Cached && !tc.Expired && !tc.Protected {
					want += 4
				}
				if output.MetricCount() != want || input.DataPointCount() != 2 {
					t.Fatal("shadow/filter mismatch or mutated input")
				}
			})
		}
	}
}

func TestMetricDatapointCounts(t *testing.T) {
	for _, kind := range []string{"gauge", "sum", "histogram", "exponential", "summary"} {
		m := pmetric.NewMetric()
		switch kind {
		case "gauge":
			m.SetEmptyGauge().DataPoints().AppendEmpty()
		case "sum":
			m.SetEmptySum().DataPoints().AppendEmpty()
		case "histogram":
			m.SetEmptyHistogram().DataPoints().AppendEmpty()
		case "exponential":
			m.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
		case "summary":
			m.SetEmptySummary().DataPoints().AppendEmpty()
		}
		if metricDatapoints(m) != 1 {
			t.Fatalf("wrong count for %s", kind)
		}
	}
}
