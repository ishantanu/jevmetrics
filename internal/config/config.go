package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Config struct {
	Server struct {
		Listen string `json:"listen"`
	} `json:"server"`
	Prometheus struct {
		URL          string `json:"url"`
		TenantHeader string `json:"tenant_header"`
		Tenant       string `json:"tenant"`
	} `json:"prometheus"`
	Evaluator struct {
		Type             string `json:"type"`
		Model            string `json:"model"`
		BaseURL          string `json:"base_url"`
		Timeout          string `json:"timeout"`
		FallbackBaseline bool   `json:"fallback_baseline"`
	} `json:"evaluator"`
	PollInterval time.Duration `json:"-"`
	Poll         string        `json:"poll_interval"`
	Signals      []Signal      `json:"signals"`
}

type Signal struct {
	Name            string `json:"name"`
	ErrorQuery      string `json:"error_query"`
	LatencyQuery    string `json:"latency_query"`
	TrafficQuery    string `json:"traffic_query"`
	SaturationQuery string `json:"saturation_query"`
}

func Load(path string) (Config, error) {
	var cfg Config
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Server.Listen == "" {
		cfg.Server.Listen = ":8080"
	}
	if cfg.Prometheus.URL == "" {
		return cfg, fmt.Errorf("prometheus.url is required")
	}
	if cfg.Poll == "" {
		cfg.Poll = "30s"
	}
	if cfg.Evaluator.Type == "" {
		cfg.Evaluator.Type = "jev"
	}
	if cfg.Evaluator.Model == "" {
		cfg.Evaluator.Model = "jev-latest"
	}
	if cfg.Evaluator.BaseURL == "" {
		cfg.Evaluator.BaseURL = "https://api.typesafe.ai"
	}
	if cfg.Evaluator.Timeout == "" {
		cfg.Evaluator.Timeout = "3s"
	}
	cfg.PollInterval, err = time.ParseDuration(cfg.Poll)
	if err != nil {
		return cfg, fmt.Errorf("parse poll_interval: %w", err)
	}
	if _, err := time.ParseDuration(cfg.Evaluator.Timeout); err != nil {
		return cfg, fmt.Errorf("parse evaluator.timeout: %w", err)
	}
	for i, s := range cfg.Signals {
		if s.Name == "" {
			return cfg, fmt.Errorf("signals[%d].name is required", i)
		}
	}
	if cfg.Evaluator.Type != "jev" && cfg.Evaluator.Type != "baseline" {
		return cfg, fmt.Errorf("evaluator.type must be jev or baseline")
	}
	return cfg, nil
}
