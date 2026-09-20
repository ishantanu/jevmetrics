package jevmetricsprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

func benchmarkProcessor(b *testing.B, cfg *Config) *metricsProcessor {
	b.Helper()
	next, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error { return nil })
	if err != nil {
		b.Fatal(err)
	}
	p, err := newProcessor(processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}, cfg, next)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func BenchmarkConsumeMetricsCached(b *testing.B) {
	p := benchmarkProcessor(b, testConfig())
	input := metricInput()
	p.scores.put(inputKey(input), metricScore{Keep: 1, ScoredAt: time.Now()})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.ConsumeMetrics(context.Background(), input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConsumeMetricsColdQueue(b *testing.B) {
	cfg := testConfig()
	cfg.QueueSize = b.N + 1
	p := benchmarkProcessor(b, cfg)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		input := metricInput()
		input.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).SetName("custom." + benchmarkName(i))
		if err := p.ConsumeMetrics(context.Background(), input); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkName(index int) string {
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	if index == 0 {
		return "0"
	}
	name := ""
	for index > 0 {
		name = string(alphabet[index%len(alphabet)]) + name
		index /= len(alphabet)
	}
	return name
}
