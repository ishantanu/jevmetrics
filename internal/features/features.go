package features

type Raw struct {
	ErrorRate  float64
	Latency    float64
	Traffic    float64
	Saturation float64
}

type Features struct {
	// Raw operational values retained for the Jev state.
	ErrorRate      float64 `json:"error_rate"`
	LatencySeconds float64 `json:"latency_seconds"`
	TrafficRPS     float64 `json:"traffic_rps"`
	Saturation     float64 `json:"saturation"`

	// Normalized 0..1 features. These are deterministic and computed locally.
	ErrorScore      float64 `json:"error_score"`
	LatencyScore    float64 `json:"latency_score"`
	TrafficScore    float64 `json:"traffic_score"`
	SaturationScore float64 `json:"saturation_score"`
}

func Extract(r Raw) Features {
	return Features{
		ErrorRate:       r.ErrorRate,
		LatencySeconds:  r.Latency,
		TrafficRPS:      r.Traffic,
		Saturation:      r.Saturation,
		ErrorScore:      normalize(r.ErrorRate, 0.01, 0.10),
		LatencyScore:    normalize(r.Latency, 0.25, 2.0),
		TrafficScore:    normalize(r.Traffic, 100, 5000),
		SaturationScore: normalize(r.Saturation, 0.70, 1.0),
	}
}

func normalize(v, healthy, bad float64) float64 {
	if v <= healthy {
		return 0
	}
	if v >= bad {
		return 1
	}
	return (v - healthy) / (bad - healthy)
}
