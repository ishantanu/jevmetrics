package evaluator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ishantanu/jevmetrics/internal/features"
)

const (
	DefaultJevBaseURL = "https://api.typesafe.ai"
	DefaultJevModel   = "jev-latest"
)

type JevConfig struct {
	APIKey  string
	BaseURL string
	Model   string
	Timeout time.Duration
}

type Jev struct {
	apiKey string
	url    string
	model  string
	http   *http.Client
}

func NewJev(cfg JevConfig) (*Jev, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("Jev API key is required")
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultJevBaseURL
	}
	if cfg.Model == "" {
		cfg.Model = DefaultJevModel
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 3 * time.Second
	}
	return &Jev{
		apiKey: cfg.APIKey,
		url:    strings.TrimRight(cfg.BaseURL, "/") + "/v1/systemone",
		model:  cfg.Model,
		http:   &http.Client{Timeout: cfg.Timeout},
	}, nil
}

type jevQuestion struct {
	Type         string            `json:"type"`
	Instructions string            `json:"instructions"`
	Criteria     map[string]string `json:"criteria,omitempty"`
}

type jevRequest struct {
	Model     string                 `json:"model"`
	State     any                    `json:"state"`
	Questions map[string]jevQuestion `json:"questions"`
}

type jevAnswer struct {
	Type          string             `json:"type"`
	Noul          float64            `json:"noul"`
	Choice        string             `json:"choice"`
	Probabilities map[string]float64 `json:"probabilities"`
	Confidence    float64            `json:"confidence"`
}

type jevResponse struct {
	Model   string               `json:"model"`
	Answers map[string]jevAnswer `json:"answers"`
	Usage   struct {
		InputTokens  int     `json:"input_tokens"`
		OutputTokens int     `json:"output_tokens"`
		Cost         float64 `json:"cost"`
	} `json:"usage"`
}

func (j *Jev) Evaluate(f features.Features) (Result, error) {
	return j.EvaluateContext(context.Background(), f)
}

func (j *Jev) EvaluateContext(ctx context.Context, f features.Features) (Result, error) {
	state := map[string]any{
		"kind":     "service_observability_snapshot",
		"features": f,
		"notes": []string{
			"error_rate is a fraction of failed requests",
			"latency_seconds is the configured tail-latency signal",
			"traffic_rps is requests per second",
			"saturation is a 0..1 resource saturation signal when the PromQL query is configured that way",
			"*_score fields are deterministic local normalizations where 0 is healthy and 1 is strongly degraded",
		},
	}

	reqBody := jevRequest{
		Model: j.model,
		State: state,
		Questions: map[string]jevQuestion{
			"anomaly": {
				Type:         "noul",
				Instructions: "Is this telemetry snapshot meaningfully anomalous or degraded compared with a normally healthy service?",
				Criteria: map[string]string{
					"true":  "The signals collectively indicate an abnormal or degraded service state worth treating as an anomaly.",
					"false": "The signals are consistent with a healthy or ordinary service state and do not indicate a meaningful anomaly.",
				},
			},
			"user_impact": {
				Type:         "noul",
				Instructions: "Is this telemetry snapshot likely to correspond to real user-visible impact?",
				Criteria: map[string]string{
					"true":  "The observed error and latency behavior is likely to materially affect users or successful requests.",
					"false": "The observed state is unlikely to create material user-visible impact.",
				},
			},
			"investigate": {
				Type:         "noul",
				Instructions: "Should an SRE investigate this service state now, assuming conventional deterministic alerts and SLOs remain the primary safety mechanism?",
				Criteria: map[string]string{
					"true":  "The combination of signals is sufficiently suspicious or impactful that an SRE investigation is warranted now.",
					"false": "The combination of signals does not currently justify an SRE investigation.",
				},
			},
			"failure_domain": {
				Type:         "choice",
				Instructions: "Which failure domain best explains the observed service degradation?",
				Criteria: map[string]string{
					"resource": "Resource pressure or saturation is the dominant explanation.",
					"errors":   "Request or application errors are the dominant explanation.",
					"latency":  "High latency or slow responses are the dominant explanation.",
					"unknown":  "The telemetry does not support a clear dominant failure domain.",
				},
			},
		},
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return Result{}, fmt.Errorf("marshal Jev request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.url, bytes.NewReader(payload))
	if err != nil {
		return Result{}, fmt.Errorf("create Jev request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+j.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := j.http.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("call Jev: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return Result{}, fmt.Errorf("Jev returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var out jevResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Result{}, fmt.Errorf("decode Jev response: %w", err)
	}

	anomaly, ok := out.Answers["anomaly"]
	if !ok || anomaly.Type != "noul" {
		return Result{}, fmt.Errorf("Jev response missing noul answer %q", "anomaly")
	}
	impact, ok := out.Answers["user_impact"]
	if !ok || impact.Type != "noul" {
		return Result{}, fmt.Errorf("Jev response missing noul answer %q", "user_impact")
	}
	investigate, ok := out.Answers["investigate"]
	if !ok || investigate.Type != "noul" {
		return Result{}, fmt.Errorf("Jev response missing noul answer %q", "investigate")
	}
	domain, ok := out.Answers["failure_domain"]
	if !ok || domain.Type != "choice" {
		return Result{}, fmt.Errorf("Jev response missing choice answer %q", "failure_domain")
	}
	if domain.Choice != "resource" && domain.Choice != "errors" && domain.Choice != "latency" && domain.Choice != "unknown" {
		return Result{}, fmt.Errorf("Jev returned unsupported failure domain %q", domain.Choice)
	}

	return Result{
		AnomalyProbability:     clamp(anomaly.Noul),
		UserImpactProbability:  clamp(impact.Noul),
		InvestigateProbability: clamp(investigate.Noul),
		FailureDomain:          domain.Choice,
	}, nil
}
