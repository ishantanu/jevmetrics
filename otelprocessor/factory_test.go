package jevmetricsprocessor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

func TestProcessorFactory(t *testing.T) {
	factory := NewFactory()
	settings := processor.Settings{ID: component.NewID(Type), TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.APIKey = "test"
	if cfg.Mode != "annotate" || cfg.Coordination.RedisURL != "" || factory.MetricsStability() != component.StabilityLevelAlpha {
		t.Fatal("expected alpha, local, annotate defaults")
	}
	downstreamErr := errors.New("downstream unavailable")
	next, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error { return downstreamErr })
	if err != nil {
		t.Fatal(err)
	}
	p, err := factory.CreateMetrics(context.Background(), settings, cfg, next)
	if err != nil {
		t.Fatal(err)
	}
	if p.Capabilities().MutatesData {
		t.Fatal("must preserve input shared with archive pipeline")
	}
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	// Protected input exercises synchronous forwarding without external inference.
	input := metricInput()
	input.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).SetName("up")
	if err := p.ConsumeMetrics(context.Background(), input); !errors.Is(err, downstreamErr) {
		t.Fatalf("downstream error not propagated: %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := factory.CreateTraces(context.Background(), settings, cfg, nil); err == nil {
		t.Fatal("unexpected traces support")
	}
	if _, err := factory.CreateLogs(context.Background(), settings, cfg, nil); err == nil {
		t.Fatal("unexpected logs support")
	}
	cfg.Mode = "route"
	if _, err := factory.CreateMetrics(context.Background(), settings, cfg, next); err == nil {
		t.Fatal("removed route mode accepted")
	}
}

func TestLocalReplicaAssessmentLifecycle(t *testing.T) {
	// No Redis server or configuration. A new owner independently assesses its input.
	input := metricInput()
	var calls atomic.Int32
	var writer string
	for replica := 0; replica < 2; replica++ {
		outputs := make(chan pmetric.Metrics, 16)
		p := testProcessor(t, testConfig(), func(_ context.Context, md pmetric.Metrics) error { outputs <- md; return nil })
		if p.coordinator != nil {
			t.Fatal("local mode created a coordinator")
		}
		p.client.http.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"answers":{"relevance":{"type":"noul","noul":0.1},"redundancy":{"type":"noul","noul":0.9},"keep":{"type":"noul","noul":0.1},"action":{"type":"choice","choice":"drop"}}}`))}, nil
		})
		if err := p.Start(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { p.Shutdown(context.Background()) })
		if err := p.ConsumeMetrics(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		if (<-outputs).MetricCount() != 1 {
			t.Fatal("cold replica did not retain original input")
		}
		awaitCondition(t, func() bool { _, ok := p.cachedScore(inputKey(input), time.Now()); return ok })
		if err := p.ConsumeMetrics(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		// Joining workers makes the absence of asynchronous downstream output deterministic.
		if err := p.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(outputs) != 1 {
			t.Fatal("expected exactly one synchronous annotated batch")
		}
		output := <-outputs
		if output.MetricCount() != 5 || input.MetricCount() != 1 {
			t.Fatal("annotation or archive preservation failed")
		}
		id, ok := output.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(1).Gauge().DataPoints().At(0).Attributes().Get("jev.collector.id")
		if !ok || id.Str() == "" || id.Str() == writer {
			t.Fatal("local replicas share assessment writer identity")
		}
		writer = id.Str()
	}
	if calls.Load() != 2 {
		t.Fatalf("local replicas made %d requests, want one per owner", calls.Load())
	}
}
