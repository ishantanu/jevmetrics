package jevmetricsprocessor

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
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
	Coordination      CoordinationConfig  `mapstructure:"coordination"`
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

// CoordinationConfig enables shared assessments through one Redis primary.
// A namespace is an inference budget and tenant boundary.
type CoordinationConfig struct {
	RedisURL          configopaque.String `mapstructure:"redis_url"`
	Namespace         string              `mapstructure:"namespace"`
	Revision          string              `mapstructure:"revision"`
	Timeout           string              `mapstructure:"timeout"`
	LeaseTTL          string              `mapstructure:"lease_ttl"`
	MaxInFlight       int                 `mapstructure:"max_in_flight"`
	RequestsPerSecond int                 `mapstructure:"requests_per_second"`
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
	case "annotate", "reduce":
	default:
		return fmt.Errorf("mode must be annotate or reduce")
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
	if c.Coordination.RedisURL != "" {
		if _, err := redis.ParseURL(string(c.Coordination.RedisURL)); err != nil {
			return fmt.Errorf("coordination.redis_url must be a valid redis:// or rediss:// URL")
		}
		if strings.TrimSpace(c.Coordination.Namespace) == "" || strings.TrimSpace(c.Coordination.Revision) == "" {
			return fmt.Errorf("coordination.namespace and revision are required")
		}
		timeout, err := time.ParseDuration(c.Coordination.Timeout)
		if err != nil || timeout < time.Millisecond {
			return fmt.Errorf("coordination.timeout must be at least 1ms")
		}
		lease, err := time.ParseDuration(c.Coordination.LeaseTTL)
		inferenceTimeout, _ := time.ParseDuration(c.Timeout)
		if err != nil || lease <= inferenceTimeout+2*timeout {
			return fmt.Errorf("coordination.lease_ttl must exceed timeout + twice coordination.timeout")
		}
		if c.scoreTTL() < time.Millisecond || c.Coordination.MaxInFlight <= 0 || c.Coordination.RequestsPerSecond <= 0 {
			return fmt.Errorf("coordination requires positive limits and policy.score_ttl >= 1ms")
		}
	}
	return nil
}

func (c *Config) scoreTTL() time.Duration {
	d, _ := time.ParseDuration(c.Policy.ScoreTTL)
	return d
}
