package jevmetricsprocessor

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestRateLimitValidation(t *testing.T) {
	for _, r := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		cfg := testConfig()
		cfg.RateLimit.RequestsPerSecond = r
		if cfg.Validate() == nil {
			t.Fatalf("accepted rate %v", r)
		}
	}
	for _, burst := range []int{0, -1} {
		cfg := testConfig()
		cfg.RateLimit.Burst = burst
		if cfg.Validate() == nil {
			t.Fatalf("accepted burst %d", burst)
		}
	}
	cfg := testConfig()
	cfg.RateLimit.RequestsPerSecond = 0.5
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRateLimitSharedAcrossWorkers(t *testing.T) {
	for _, burst := range []int{1, 3} {
		t.Run(fmt.Sprint(burst), func(t *testing.T) {
			cfg := testConfig()
			cfg.Workers = 6
			cfg.RateLimit = RateLimitConfig{RequestsPerSecond: 20, Burst: burst}
			p := testProcessor(t, cfg, func(context.Context, pmetric.Metrics) error { return nil })
			requests := make(chan time.Time, 6)
			p.client.http.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
				requests <- time.Now()
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"answers":{"relevance":{"type":"noul","noul":0.9},"redundancy":{"type":"noul","noul":0.1},"keep":{"type":"noul","noul":0.9},"action":{"type":"choice","choice":"keep"}}}`))}, nil
			})
			// Queue all work before starting workers to exercise concurrent admission.
			for i := 0; i < 6; i++ {
				p.enqueue(fmt.Sprint(i), metricSummary{Name: fmt.Sprint(i)})
			}
			started := time.Now()
			if err := p.Start(context.Background(), nil); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.Shutdown(context.Background()) })
			deadline := time.After(3 * time.Second)
			for i := 0; i < 6; i++ {
				select {
				case at := <-requests:
					minimum := time.Duration(i+1-burst)*50*time.Millisecond - 10*time.Millisecond
					if at.Sub(started) < minimum {
						t.Fatalf("request %d admitted after %v, minimum %v", i+1, at.Sub(started), minimum)
					}
				case <-deadline:
					t.Fatal("requests did not complete")
				}
			}
		})
	}
}

func TestRateLimitDoesNotBlockDeliveryOrShutdown(t *testing.T) {
	cfg := testConfig()
	cfg.RateLimit = RateLimitConfig{RequestsPerSecond: 0.001, Burst: 1}
	var calls atomic.Int32
	delivered := make(chan struct{}, 1)
	p := testProcessor(t, cfg, func(context.Context, pmetric.Metrics) error {
		delivered <- struct{}{}
		return nil
	})
	p.client.http.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, fmt.Errorf("unexpected request")
	})
	// Exhaust the bucket: the next admission would otherwise wait 1000 seconds.
	if !p.limiter.Allow() {
		t.Fatal("initial token missing")
	}
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	go func() { _ = p.ConsumeMetrics(context.Background(), metricInput()) }()
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("delivery blocked on inference admission")
	}
	awaitCondition(t, func() bool { return p.limiter.Tokens() < -0.5 })
	done := make(chan struct{})
	go func() { p.Shutdown(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel admission wait")
	}
	if calls.Load() != 0 {
		t.Fatal("request bypassed rate limit")
	}
}
