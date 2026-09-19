package evaluator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ishantanu/jevmetrics/internal/features"
)

func TestJevEvaluator(t *testing.T) {
	var got jevRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatalf("unexpected auth header %q", r.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model":"typesafe/jev-test",
			"answers":{
				"anomaly":{"type":"noul","noul":0.91},
				"user_impact":{"type":"noul","noul":0.74},
				"investigate":{"type":"noul","noul":0.88},
				"failure_domain":{"type":"choice","choice":"latency","probabilities":{"latency":0.8,"errors":0.1,"resource":0.05,"unknown":0.05},"confidence":0.77}
			},
			"usage":{"input_tokens":100,"output_tokens":20}
		}`))
	}))
	defer server.Close()

	j, err := NewJev(JevConfig{APIKey: "test-key", BaseURL: server.URL, Model: "jev-latest", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	res, err := j.Evaluate(features.Extract(features.Raw{ErrorRate: .04, Latency: .9, Traffic: 800, Saturation: .6}))
	if err != nil {
		t.Fatal(err)
	}
	if res.AnomalyProbability != .91 || res.UserImpactProbability != .74 || res.InvestigateProbability != .88 {
		t.Fatalf("unexpected probabilities: %+v", res)
	}
	if res.FailureDomain != "latency" {
		t.Fatalf("unexpected failure domain: %q", res.FailureDomain)
	}
	if got.Model != "jev-latest" || len(got.Questions) != 4 {
		t.Fatalf("unexpected request: %+v", got)
	}
}

func TestJevEvaluatorRejectsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()
	j, err := NewJev(JevConfig{APIKey: "test-key", BaseURL: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Evaluate(features.Features{}); err == nil {
		t.Fatal("expected error")
	}
}
