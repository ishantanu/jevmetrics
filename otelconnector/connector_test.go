package jevmetricsconnector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testConfig() *Config {
	c := createDefaultConfig().(*Config)
	c.APIKey = "test"
	return c
}
func metricInput() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("instrumentation")
	m := sm.Metrics().AppendEmpty()
	m.SetName("custom.requests")
	m.SetUnit("{request}")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(42)
	dp.Attributes().PutStr("route", "/private")
	return md
}
func inputKey(md pmetric.Metrics) string {
	rm := md.ResourceMetrics().At(0)
	sm := rm.ScopeMetrics().At(0)
	return scoreKey(rm.Resource().Attributes(), sm.Scope(), sm.Metrics().At(0), rm.SchemaUrl(), sm.SchemaUrl())
}
func testConnector(t *testing.T, cfg *Config, fn consumer.ConsumeMetricsFunc) *connectorImp {
	t.Helper()
	next, err := consumer.NewMetrics(fn)
	if err != nil {
		t.Fatal(err)
	}
	c, err := newConnector(connector.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}, cfg, next)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestResponseValidation(t *testing.T) {
	valid := `{"answers":{"relevance":{"type":"noul","noul":0.9},"redundancy":{"type":"noul","noul":0.1},"keep":{"type":"noul","noul":0},"action":{"type":"choice","choice":"keep"}}}`
	for _, tc := range []struct {
		name, body string
		bad        bool
	}{
		{"valid zero", valid, false},
		{"missing numeric field", strings.Replace(valid, `"noul":0}`, `"other":0}`, 1), true},
		{"null numeric field", strings.Replace(valid, `"noul":0}`, `"noul":null}`, 1), true},
		{"wrong type", strings.Replace(valid, `"type":"noul"`, `"type":"choice"`, 1), true},
		{"outside range", strings.Replace(valid, `"noul":0}`, `"noul":-0.1}`, 1), true},
		{"invalid action", strings.Replace(valid, `"choice":"keep"`, `"choice":"delete"`, 1), true},
		{"missing answers", `{"answers":{}}`, true},
		{"bad JSON", `{`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := newJevClient(testConfig())
			if err != nil {
				t.Fatal(err)
			}
			client.http.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/v1/systemone" || r.Header.Get("Authorization") != "Bearer test" {
					t.Fatal("incorrect request")
				}
				var req request
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatal(err)
				}
				if len(req.Questions) != 4 {
					t.Fatal("missing questions")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			_, err = client.scoreMetric(context.Background(), metricSummary{Name: "test"})
			if (err != nil) != tc.bad {
				t.Fatalf("error = %v, want bad=%v", err, tc.bad)
			}
		})
	}
}

func TestPolicyAndPreservation(t *testing.T) {
	for _, tc := range []struct {
		mode              string
		score             float64
		cached, protected bool
		want              int
	}{
		{"ANNOTATE", 0, true, false, 5},
		{"route", 0.3, true, false, 0},
		{"reduce", 0.7, true, false, 1},
		{"reduce", 0.5, true, false, 1},
		{"reduce", 0, false, false, 1},
		{"reduce", 0, true, true, 1},
	} {
		t.Run(fmt.Sprintf("%s-%v-%v-%v", tc.mode, tc.score, tc.cached, tc.protected), func(t *testing.T) {
			cfg := testConfig()
			cfg.Mode = tc.mode
			if tc.protected {
				cfg.Policy.ProtectedMetrics = []string{"custom.requests"}
			}
			var output pmetric.Metrics
			c := testConnector(t, cfg, func(_ context.Context, md pmetric.Metrics) error { output = md; return nil })
			input := metricInput()
			if tc.cached {
				c.scores.put(inputKey(input), metricScore{Keep: tc.score, ScoredAt: time.Now()})
			}
			if err := c.ConsumeMetrics(context.Background(), input); err != nil {
				t.Fatal(err)
			}
			if output.MetricCount() != tc.want {
				t.Fatalf("got %d metrics, want %d", output.MetricCount(), tc.want)
			}
			if input.MetricCount() != 1 {
				t.Fatal("input mutated")
			}
			if tc.want > 0 {
				original := input.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
				kept := output.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
				if original.Name() != kept.Name() || kept.Gauge().DataPoints().At(0).IntValue() != 42 {
					t.Fatal("payload changed")
				}
				v, _ := kept.Gauge().DataPoints().At(0).Attributes().Get("route")
				if v.Str() != "/private" {
					t.Fatal("attributes changed")
				}
			}
		})
	}
}

func TestCacheIdentity(t *testing.T) {
	base := metricInput()
	key := inputKey(base)
	for _, mutate := range []func(pmetric.ResourceMetrics, pmetric.ScopeMetrics){
		func(r pmetric.ResourceMetrics, s pmetric.ScopeMetrics) { s.Scope().SetName("other") },
		func(r pmetric.ResourceMetrics, s pmetric.ScopeMetrics) { s.Scope().SetVersion("2") },
		func(r pmetric.ResourceMetrics, s pmetric.ScopeMetrics) {
			s.Scope().Attributes().PutStr("tenant", "other")
		},
		func(r pmetric.ResourceMetrics, s pmetric.ScopeMetrics) {
			r.Resource().Attributes().PutStr("tenant", "other")
		},
		func(r pmetric.ResourceMetrics, s pmetric.ScopeMetrics) { s.SetSchemaUrl("other") },
		func(r pmetric.ResourceMetrics, s pmetric.ScopeMetrics) {
			s.Metrics().At(0).SetDescription("different meaning")
		},
	} {
		copy := pmetric.NewMetrics()
		base.CopyTo(copy)
		r := copy.ResourceMetrics().At(0)
		mutate(r, r.ScopeMetrics().At(0))
		if inputKey(copy) == key {
			t.Fatal("distinct identities share a key")
		}
	}
}

func TestCacheBoundAndExpiry(t *testing.T) {
	c := newScoreCache(2)
	now := time.Now()
	s := metricScore{ScoredAt: now}
	c.put("a", s)
	c.put("b", s)
	c.get("a", now, time.Minute)
	c.put("c", s)
	if _, ok := c.get("b", now, time.Minute); ok {
		t.Fatal("least recently used score not evicted")
	}
	if len(c.entries) != 2 {
		t.Fatal("cache exceeds capacity")
	}
	if _, ok := c.get("a", now.Add(time.Minute), time.Minute); ok {
		t.Fatal("expired score returned")
	}
	c.prune(now.Add(time.Minute), time.Minute)
	if len(c.entries) != 0 {
		t.Fatal("expired entries retained")
	}
}

func TestUnknownExpiredAndQueueFullFailOpen(t *testing.T) {
	cfg := testConfig()
	cfg.QueueSize = 1
	c := testConnector(t, cfg, func(_ context.Context, md pmetric.Metrics) error {
		if md.MetricCount() != 1 {
			t.Error("metric lost")
		}
		return nil
	})
	input := metricInput()
	c.scores.put(inputKey(input), metricScore{Keep: 0, ScoredAt: time.Now().Add(-time.Hour)})
	if err := c.ConsumeMetrics(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		input.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).SetName(fmt.Sprintf("custom.%d", i))
		if err := c.ConsumeMetrics(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}
	if len(c.jobs) != 1 || len(c.queued) != 1 {
		t.Fatal("queue bounds violated")
	}
}

func TestFailureBackoffAndRecovery(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1
	cfg.Mode = "reduce"
	c := testConnector(t, cfg, func(_ context.Context, md pmetric.Metrics) error {
		if md.MetricCount() != 1 {
			t.Error("unscored metric lost")
		}
		return nil
	})
	var clockMu sync.Mutex
	now := time.Now()
	c.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	var calls int
	c.client.http.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"answers":{"relevance":{},"redundancy":{},"keep":{},"action":{}}}`))}, nil
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"answers":{"relevance":{"type":"noul","noul":1},"redundancy":{"type":"noul","noul":0},"keep":{"type":"noul","noul":1},"action":{"type":"choice","choice":"keep"}}}`))}, nil
	})
	if err := c.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())
	input := metricInput()
	c.ConsumeMetrics(context.Background(), input)
	waitFor := func(check func() bool) {
		t.Helper()
		deadline := time.Now().Add(time.Second * 3)
		for time.Now().Before(deadline) {
			c.mu.Lock()
			ok := check()
			c.mu.Unlock()
			if ok {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("worker did not complete")
	}
	waitFor(func() bool { return c.failures == 1 && len(c.queued) == 0 })
	c.ConsumeMetrics(context.Background(), input)
	c.mu.Lock()
	if len(c.queued) != 0 {
		t.Error("retried during cooldown")
	}
	c.mu.Unlock()
	clockMu.Lock()
	now = now.Add(time.Second * 2)
	clockMu.Unlock()
	c.ConsumeMetrics(context.Background(), input)
	waitFor(func() bool { return len(c.scores.entries) == 1 && c.failures == 0 })
}

func TestConfigRejectsInvalidLimits(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.Timeout = "0s" }, func(c *Config) { c.Policy.ScoreTTL = "-1s" }, func(c *Config) { c.CacheSize = 0 },
	} {
		c := testConfig()
		change(c)
		if c.Validate() == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}

func TestSummaryDoesNotExportDatapointValues(t *testing.T) {
	md := metricInput()
	rm := md.ResourceMetrics().At(0)
	sm := rm.ScopeMetrics().At(0)
	s := summarizeMetric(sm.Metrics().At(0), sm.Scope(), rm.Resource().Attributes(), nil)
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "/private") || strings.Contains(string(b), "42") {
		t.Fatal("raw datapoint leaked")
	}
	if len(s.AttributeKeys) != 1 || s.EstimatedSeries != 1 {
		t.Fatal("missing shape information")
	}
}

func TestAllMetricShapesPreserved(t *testing.T) {
	for _, kind := range []string{"gauge", "sum", "histogram", "exponential", "summary"} {
		t.Run(kind, func(t *testing.T) {
			input := metricInput()
			m := input.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
			switch kind {
			case "sum":
				s := m.SetEmptySum()
				s.SetIsMonotonic(true)
				s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				s.DataPoints().AppendEmpty().SetIntValue(19)
			case "histogram":
				h := m.SetEmptyHistogram()
				h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := h.DataPoints().AppendEmpty()
				dp.SetCount(2)
				dp.SetSum(7)
				dp.ExplicitBounds().FromRaw([]float64{5})
				dp.BucketCounts().FromRaw([]uint64{1, 1})
			case "exponential":
				h := m.SetEmptyExponentialHistogram()
				h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				dp := h.DataPoints().AppendEmpty()
				dp.SetCount(1)
				dp.SetSum(2)
				dp.Positive().BucketCounts().FromRaw([]uint64{1})
			case "summary":
				dp := m.SetEmptySummary().DataPoints().AppendEmpty()
				dp.SetCount(2)
				dp.SetSum(8)
				q := dp.QuantileValues().AppendEmpty()
				q.SetQuantile(0.5)
				q.SetValue(4)
			}
			marshaler := pmetric.ProtoMarshaler{}
			before, err := marshaler.MarshalMetrics(input)
			if err != nil {
				t.Fatal(err)
			}
			c := testConnector(t, testConfig(), func(_ context.Context, output pmetric.Metrics) error {
				after, err := marshaler.MarshalMetrics(output)
				if err != nil {
					t.Fatal(err)
				}
				if string(before) != string(after) {
					t.Fatal("retained telemetry changed")
				}
				return nil
			})
			if err := c.ConsumeMetrics(context.Background(), input); err != nil {
				t.Fatal(err)
			}
		})
	}
}
