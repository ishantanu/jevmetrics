package jevmetricsprocessor

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/metric"
)

type recordingCounter struct {
	metric.Int64Counter
	value atomic.Int64
}

func (c *recordingCounter) Add(_ context.Context, value int64, _ ...metric.AddOption) {
	c.value.Add(value)
}

func TestRetentionCounters(t *testing.T) {
	for _, tc := range []struct {
		name, mode                                           string
		cached, expired, protected, invalidIdentity, cooling bool
		score                                                float64
		wantKept, wantDropped, wantAnnotated                 int64
	}{
		{name: "cold", mode: "reduce", wantKept: 1},
		{name: "expired", mode: "reduce", cached: true, expired: true, wantKept: 1},
		{name: "failure cooldown", mode: "reduce", cooling: true, wantKept: 1},
		{name: "unsupported identity", mode: "reduce", invalidIdentity: true, wantKept: 1},
		{name: "protected", mode: "reduce", cached: true, protected: true, wantKept: 1},
		{name: "cached keep", mode: "reduce", cached: true, score: 1, wantKept: 1},
		{name: "uncertain", mode: "reduce", cached: true, score: 0.5, wantKept: 1},
		{name: "cached drop", mode: "reduce", cached: true, wantDropped: 1},
		{name: "cold annotate", mode: "annotate", wantKept: 1},
		{name: "cached annotate", mode: "annotate", cached: true, wantKept: 1, wantAnnotated: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.Mode = tc.mode
			if tc.protected {
				cfg.Policy.ProtectedMetrics = []string{"custom.requests"}
			}
			var output pmetric.Metrics
			p := testProcessor(t, cfg, func(_ context.Context, md pmetric.Metrics) error { output = md; return nil })
			processed, kept, dropped, annotated := &recordingCounter{}, &recordingCounter{}, &recordingCounter{}, &recordingCounter{}
			p.telemetry.processed, p.telemetry.kept, p.telemetry.dropped, p.telemetry.annotated = processed, kept, dropped, annotated
			input := metricInput()
			if tc.invalidIdentity {
				input.ResourceMetrics().At(0).Resource().Attributes().PutDouble("invalid", math.NaN())
			}
			if tc.cached {
				scoredAt := time.Now()
				if tc.expired {
					scoredAt = scoredAt.Add(-2 * cfg.scoreTTL())
				}
				p.scores.put(inputKey(input), metricScore{Keep: tc.score, ScoredAt: scoredAt})
			}
			if tc.cooling {
				p.retryUntil = time.Now().Add(time.Hour)
			}
			if err := p.ConsumeMetrics(context.Background(), input); err != nil {
				t.Fatal(err)
			}
			if kept.value.Load() != tc.wantKept || dropped.value.Load() != tc.wantDropped || annotated.value.Load() != tc.wantAnnotated {
				t.Fatalf("kept/dropped/annotated = %d/%d/%d, want %d/%d/%d", kept.value.Load(), dropped.value.Load(), annotated.value.Load(), tc.wantKept, tc.wantDropped, tc.wantAnnotated)
			}
			if processed.value.Load() != kept.value.Load()+dropped.value.Load() {
				t.Fatal("input accounting does not balance")
			}
			if output.MetricCount() != int(tc.wantKept+4*tc.wantAnnotated) {
				t.Fatal("counters disagree with output")
			}
		})
	}
}

func TestQueueAdmissionCounters(t *testing.T) {
	cfg := testConfig()
	cfg.QueueSize = 1
	cfg.Mode = "reduce"
	var forwarded int
	p := testProcessor(t, cfg, func(_ context.Context, md pmetric.Metrics) error { forwarded += md.MetricCount(); return nil })
	queued, rejected, kept := &recordingCounter{}, &recordingCounter{}, &recordingCounter{}
	p.telemetry.queued, p.telemetry.queueRejected, p.telemetry.kept = queued, rejected, kept
	// Do not start workers: queue occupancy stays deterministic and no inference runs.
	for i, name := range []string{"first", "first", "full", "cooldown"} {
		if name == "cooldown" {
			p.retryUntil = time.Now().Add(time.Hour)
		}
		input := metricInput()
		input.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).SetName(name)
		if err := p.ConsumeMetrics(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		wantRejected := int64(0)
		if i >= 2 {
			wantRejected = int64(i - 1)
		}
		if queued.value.Load() != 1 || rejected.value.Load() != wantRejected {
			t.Fatalf("%s: queued/rejected = %d/%d, want 1/%d", name, queued.value.Load(), rejected.value.Load(), wantRejected)
		}
	}
	if len(p.jobs) != 1 || len(p.queued) != 1 {
		t.Fatal("unexpected pending jobs")
	}
	if kept.value.Load() != 4 || forwarded != 4 {
		t.Fatal("queue pressure lost retention accounting or input")
	}
}
