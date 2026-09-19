package features

import "testing"

func TestExtractClampsScores(t *testing.T) {
	f := Extract(Raw{
		ErrorRate:  0.2,
		Latency:    0.1,
		Traffic:    0,
		Saturation: 0.85,
	})

	if f.ErrorScore != 1 {
		t.Fatalf("expected error score 1, got %v", f.ErrorScore)
	}
	if f.LatencyScore != 0 {
		t.Fatalf("expected latency score 0, got %v", f.LatencyScore)
	}
	if f.SaturationScore <= 0 || f.SaturationScore >= 1 {
		t.Fatalf("expected intermediate saturation score, got %v", f.SaturationScore)
	}
}
