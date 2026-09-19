package evaluator

import (
	"log"

	"github.com/ishantanu/jevmetrics/internal/features"
)

type Fallback struct {
	Primary  Evaluator
	Fallback Evaluator
}

func (f *Fallback) Evaluate(in features.Features) (Result, error) {
	res, err := f.Primary.Evaluate(in)
	if err == nil {
		return res, nil
	}
	if f.Fallback == nil {
		return Result{}, err
	}
	log.Printf("primary evaluator failed, using baseline fallback: %v", err)
	return f.Fallback.Evaluate(in)
}
