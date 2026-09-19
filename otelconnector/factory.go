package jevmetricsconnector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

var Type = component.MustNewType("jevmetrics")

func NewFactory() connector.Factory {
	return connector.NewFactory(
		Type,
		createDefaultConfig,
		connector.WithMetricsToMetrics(createMetricsToMetrics, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
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

func createMetricsToMetrics(_ context.Context, set connector.Settings, cfg component.Config, next consumer.Metrics) (connector.Metrics, error) {
	return newConnector(set, cfg.(*Config), next)
}
