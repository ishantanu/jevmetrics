package jevmetricsconnector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type jevClient struct {
	apiKey, url, model string
	http               *http.Client
}
type question struct {
	Type         string            `json:"type"`
	Instructions string            `json:"instructions"`
	Criteria     map[string]string `json:"criteria,omitempty"`
}
type request struct {
	Model     string              `json:"model"`
	State     any                 `json:"state"`
	Questions map[string]question `json:"questions"`
}
type answer struct {
	Type   string   `json:"type"`
	Noul   *float64 `json:"noul"`
	Choice string   `json:"choice"`
}
type response struct {
	Answers map[string]answer `json:"answers"`
}

func newJevClient(cfg *Config) (*jevClient, error) {
	t, err := time.ParseDuration(cfg.Timeout)
	if err != nil {
		return nil, err
	}
	return &jevClient{apiKey: string(cfg.APIKey), url: strings.TrimRight(cfg.BaseURL, "/") + "/v1/systemone", model: cfg.Model, http: &http.Client{Timeout: t}}, nil
}

func (j *jevClient) scoreMetric(ctx context.Context, s metricSummary) (metricScore, error) {
	body := request{Model: j.model, State: map[string]any{"kind": "otel_metric_storage_value_assessment", "metric": s}, Questions: map[string]question{
		"relevance":  {Type: "noul", Instructions: "Does this metric carry durable operational value for observing, debugging, capacity planning, or understanding this workload?", Criteria: map[string]string{"true": "High operational value.", "false": "Little durable operational value."}},
		"redundancy": {Type: "noul", Instructions: "Is this metric likely redundant with other standard telemetry or excessively noisy relative to its operational value?", Criteria: map[string]string{"true": "Likely redundant or noisy.", "false": "Distinct useful signal."}},
		"keep":       {Type: "noul", Instructions: "Should this metric be retained in the primary metrics backend when the goal is to reduce metrics volume without losing important operational signal?", Criteria: map[string]string{"true": "Retain in primary metrics storage.", "false": "Safe candidate for reduction or cheaper storage."}},
		"action":     {Type: "choice", Instructions: "Choose the safest storage action for this metric.", Criteria: map[string]string{"keep": "Keep at normal resolution.", "reduce": "Keep but reduce/downsample dimensions or resolution.", "drop": "Candidate to drop from primary metrics storage."}},
	}}
	payload, err := json.Marshal(body)
	if err != nil {
		return metricScore{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.url, bytes.NewReader(payload))
	if err != nil {
		return metricScore{}, err
	}
	req.Header.Set("Authorization", "Bearer "+j.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := j.http.Do(req)
	if err != nil {
		return metricScore{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return metricScore{}, fmt.Errorf("Jev returned %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var out response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return metricScore{}, err
	}
	r, rok := out.Answers["relevance"]
	d, dok := out.Answers["redundancy"]
	k, kok := out.Answers["keep"]
	a, aok := out.Answers["action"]
	if !(rok && dok && kok && aok) {
		return metricScore{}, fmt.Errorf("Jev response missing required answers")
	}
	for name, value := range map[string]answer{"relevance": r, "redundancy": d, "keep": k} {
		if value.Type != "noul" || value.Noul == nil || *value.Noul < 0 || *value.Noul > 1 {
			return metricScore{}, fmt.Errorf("invalid Jev noul answer %q", name)
		}
	}
	if a.Type != "choice" || (a.Choice != "keep" && a.Choice != "reduce" && a.Choice != "drop") {
		return metricScore{}, fmt.Errorf("invalid Jev action answer")
	}
	return metricScore{Relevance: *r.Noul, Redundancy: *d.Noul, Keep: *k.Noul, Action: a.Choice, ScoredAt: time.Now()}, nil
}
