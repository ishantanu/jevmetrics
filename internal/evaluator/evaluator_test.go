package evaluator

import (
	"testing"

	"github.com/ishantanu/jevmetrics/internal/features"
)

func TestBaselineProducesBoundedProbabilities(t *testing.T) {
	got, err := NewBaseline().Evaluate(features.Features{
		ErrorScore:      1,
		LatencyScore:    0.5,
		TrafficScore:    0.25,
		SaturationScore: 0.75,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got.AnomalyProbability < 0 || got.AnomalyProbability > 1 {
		t.Fatalf("anomaly out of range: %v", got.AnomalyProbability)
	}
	if got.UserImpactProbability < 0 || got.UserImpactProbability > 1 {
		t.Fatalf("impact out of range: %v", got.UserImpactProbability)
	}
	if got.InvestigateProbability < 0 || got.InvestigateProbability > 1 {
		t.Fatalf("investigate out of range: %v", got.InvestigateProbability)
	}
}
