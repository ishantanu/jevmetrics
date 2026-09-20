package jevmetricsprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

var Type = component.MustNewType("jevmetrics")

func NewFactory() processor.Factory {
	return processor.NewFactory(
		Type,
		createDefaultConfig,
		processor.WithMetrics(createMetrics, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Coordination: CoordinationConfig{
			Namespace: "jevmetrics", Revision: "1", Timeout: "1s", LeaseTTL: "10s",
			MaxInFlight: 4, RequestsPerSecond: 10,
		},
		BaseURL:   "https://api.typesafe.ai",
		Model:     "jev-latest",
		Timeout:   "3s",
		Mode:      "annotate",
		QueueSize: 256,
		Workers:   2,
		CacheSize: 10000,
		Policy: Policy{
			KeepThreshold: 0.70,
			DropThreshold: 0.30,
			ScoreTTL:      "15m",
			ProtectedPrefixes: []string{
				"otelcol_", "up", "scrape_", "target_",
			},
		},
		ContextAttributes: []string{
			"service.name", "service.namespace", "deployment.environment.name",
			"cloud.region", "k8s.namespace.name",
		},
	}
}

func createMetrics(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
	return newProcessor(set, cfg.(*Config), next)
}
