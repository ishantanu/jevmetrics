package jevmetricsprocessor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestJevModelSelection(t *testing.T) {
	for _, model := range []string{"jev-1.14.0", "jev-1.13.0", "jev-latest", "jev-preview"} {
		t.Run(model, func(t *testing.T) {
			cfg := testConfig()
			cfg.Model = model
			client, err := newJevClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			client.http.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/systemone" {
					t.Fatalf("unexpected endpoint: %s %s", r.Method, r.URL)
				}
				var body request
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body.Model != model {
					t.Fatalf("model = %q, want %q", body.Model, model)
				}
				for _, name := range []string{"keep", "relevance", "redundancy"} {
					q := body.Questions[name]
					if q.Type != "noul" || q.Criteria["true"] == "" || q.Criteria["false"] == "" {
						t.Fatalf("invalid Noul question: %+v", q)
					}
				}
				if body.Questions["action"].Type != "choice" {
					t.Fatal("missing action Choice")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"model":"jev-1.14.0","answers":{"relevance":{"type":"noul","noul":0.9},"redundancy":{"type":"noul","noul":0.1},"keep":{"type":"noul","noul":0.8},"action":{"type":"choice","choice":"keep","probabilities":{"keep":0.8,"reduce":0.15,"drop":0.05},"confidence":0.7}},"usage":{"input_tokens":100,"output_tokens":20}}`))}, nil
			})
			score, err := client.scoreMetric(context.Background(), metricSummary{Name: "custom.requests"})
			if err != nil {
				t.Fatal(err)
			}
			if score.Keep != 0.8 || score.Relevance != 0.9 || score.Redundancy != 0.1 || score.Action != "keep" {
				t.Fatalf("unexpected score: %+v", score)
			}
		})
	}
}
