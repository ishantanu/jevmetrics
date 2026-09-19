package evaluator

import (
	"math"

	"github.com/ishantanu/jevmetrics/internal/features"
)

type Result struct {
	AnomalyProbability     float64
	UserImpactProbability  float64
	InvestigateProbability float64
	FailureDomain          string
}

type Evaluator interface {
	Evaluate(features.Features) (Result, error)
}

type Baseline struct{}

func NewBaseline() *Baseline { return &Baseline{} }

func clamp(v float64) float64 {
	return math.Max(0, math.Min(1, v))
}

func (b *Baseline) Evaluate(f features.Features) (Result, error) {
	anomaly := clamp(
		0.45*f.ErrorScore +
			0.30*f.LatencyScore +
			0.15*f.SaturationScore +
			0.10*f.TrafficScore,
	)
	impact := clamp(
		0.60*f.ErrorScore +
			0.30*f.LatencyScore +
			0.10*f.TrafficScore,
	)
	investigate := clamp(0.5*anomaly + 0.5*impact)

	domain := "unknown"
	switch {
	case f.SaturationScore >= f.ErrorScore && f.SaturationScore >= f.LatencyScore:
		domain = "resource"
	case f.ErrorScore >= f.LatencyScore:
		domain = "errors"
	default:
		domain = "latency"
	}

	return Result{
		AnomalyProbability:     anomaly,
		UserImpactProbability:  impact,
		InvestigateProbability: investigate,
		FailureDomain:          domain,
	}, nil
}
