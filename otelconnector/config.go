package jevmetricsconnector

import (
	"fmt"
	"math"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
)

type Policy struct {
	KeepThreshold     float64  `mapstructure:"keep_threshold"`
	DropThreshold     float64  `mapstructure:"drop_threshold"`
	ScoreTTL          string   `mapstructure:"score_ttl"`
	ProtectedMetrics  []string `mapstructure:"protected_metrics"`
	ProtectedPrefixes []string `mapstructure:"protected_prefixes"`
}

type Config struct {
	APIKey            configopaque.String `mapstructure:"api_key"`
	BaseURL           string              `mapstructure:"base_url"`
	Model             string              `mapstructure:"model"`
	Timeout           string              `mapstructure:"timeout"`
	Mode              string              `mapstructure:"mode"`
	QueueSize         int                 `mapstructure:"queue_size"`
	CacheSize         int                 `mapstructure:"cache_size"`
	Workers           int                 `mapstructure:"workers"`
	Policy            Policy              `mapstructure:"policy"`
	ContextAttributes []string            `mapstructure:"context_attributes"`
}

func (c *Config) Validate() error {
	if c.APIKey == "" {
		return fmt.Errorf("api_key is required (use ${env:JEV_API_KEY})")
	}
	if c.BaseURL == "" || c.Model == "" {
		return fmt.Errorf("base_url and model are required")
	}
	for name, value := range map[string]string{"timeout": c.Timeout, "policy.score_ttl": c.Policy.ScoreTTL} {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return fmt.Errorf("%s must be a positive duration", name)
		}
	}
	switch strings.ToLower(c.Mode) {
	case "annotate", "route", "reduce":
	default:
		return fmt.Errorf("mode must be annotate, route, or reduce")
	}
	if c.QueueSize <= 0 || c.Workers <= 0 || c.CacheSize <= 0 {
		return fmt.Errorf("queue_size, workers and cache_size must be > 0")
	}
	if math.IsNaN(c.Policy.KeepThreshold) || math.IsNaN(c.Policy.DropThreshold) || c.Policy.KeepThreshold < 0 || c.Policy.KeepThreshold > 1 || c.Policy.DropThreshold < 0 || c.Policy.DropThreshold > 1 {
		return fmt.Errorf("policy thresholds must be in [0,1]")
	}
	if c.Policy.DropThreshold > c.Policy.KeepThreshold {
		return fmt.Errorf("policy.drop_threshold must be <= policy.keep_threshold")
	}
	return nil
}

func (c *Config) scoreTTL() time.Duration {
	d, _ := time.ParseDuration(c.Policy.ScoreTTL)
	return d
}
