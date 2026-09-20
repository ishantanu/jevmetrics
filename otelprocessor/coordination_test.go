package jevmetricsprocessor

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func sharedConfig(address string) *Config {
	cfg := testConfig()
	cfg.Coordination.RedisURL = configopaque.String("redis://" + address)
	cfg.Coordination.Namespace = "test-" + rand.Text()
	return cfg
}

func testCoordinator(t *testing.T, cfg *Config) *coordinator {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	c, err := newCoordinator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.client.Close() })
	return c
}

func expectState(t *testing.T, c *coordinator, key, token, want string) []interface{} {
	t.Helper()
	result, err := c.acquire(context.Background(), key, token)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 || result[0] != want {
		t.Fatalf("admission = %v, want %s", result, want)
	}
	return result
}

func sharedPayload(t *testing.T, c *coordinator, keep float64) string {
	t.Helper()
	data, err := json.Marshal(sharedAssessment{Version: c.assessmentVersion, Relevance: &keep, Redundancy: &keep, Keep: &keep, Action: "keep"})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func awaitCondition(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}

func TestSharedReplicas(t *testing.T) {
	server := miniredis.RunT(t)
	runSharedReplicas(t, server.Addr())
}

func TestSharedConcurrentReplicas(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := sharedConfig(server.Addr())
	var calls atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	transport := transportFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"answers":{"relevance":{"type":"noul","noul":0.8},"redundancy":{"type":"noul","noul":0.1},"keep":{"type":"noul","noul":0.9},"action":{"type":"choice","choice":"keep"}}}`))}, nil
	})
	type replica struct {
		processor *metricsProcessor
		output    chan pmetric.Metrics
	}
	replicas := make([]replica, 4)
	for i := range replicas {
		output := make(chan pmetric.Metrics, 2)
		p := testProcessor(t, cfg, func(_ context.Context, md pmetric.Metrics) error { output <- md; return nil })
		p.client.http.Transport = transport
		if err := p.Start(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { p.Shutdown(context.Background()) })
		replicas[i] = replica{processor: p, output: output}
	}
	input := metricInput()
	var group sync.WaitGroup
	for _, r := range replicas {
		group.Add(1)
		go func(r replica) {
			defer group.Done()
			if err := r.processor.ConsumeMetrics(context.Background(), input); err != nil {
				t.Error(err)
			}
		}(r)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("inference not started")
	}
	group.Wait()
	for _, r := range replicas {
		if (<-r.output).MetricCount() != 1 {
			t.Fatal("cold replica did not retain input")
		}
	}
	close(release)
	key := inputKey(input)
	awaitCondition(t, func() bool {
		for _, r := range replicas {
			if _, ok := r.processor.cachedScore(key, time.Now()); !ok {
				return false
			}
		}
		return true
	})
	if calls.Load() != 1 {
		t.Fatalf("inference called %d times for one shared identity", calls.Load())
	}
}

// CI sets this to a real Redis service. No keys outside the random test namespace
// are accessed; all created keys expire without flushing an existing database.
func TestRedisIntegration(t *testing.T) {
	address := os.Getenv("JEVMETRICS_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("set JEVMETRICS_TEST_REDIS_ADDR for real Redis integration")
	}
	runSharedReplicas(t, address)
}

func runSharedReplicas(t *testing.T, address string) {
	t.Helper()
	cfg := sharedConfig(address)
	cfg.Mode = "reduce"
	var calls atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	transport := transportFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		var req struct {
			State struct {
				Metric metricSummary `json:"metric"`
			} `json:"state"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
		if req.State.Metric.DataPoints != 1 {
			t.Error("assessment did not use the first owner's batch")
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"answers":{"relevance":{"type":"noul","noul":0.1},"redundancy":{"type":"noul","noul":0.9},"keep":{"type":"noul","noul":0.1},"action":{"type":"choice","choice":"drop"}}}`))}, nil
	})
	makeReplica := func() (*metricsProcessor, chan pmetric.Metrics) {
		output := make(chan pmetric.Metrics, 16)
		c := testProcessor(t, cfg, func(_ context.Context, md pmetric.Metrics) error { output <- md; return nil })
		c.client.http.Transport = transport
		if err := c.Start(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Shutdown(context.Background()) })
		return c, output
	}
	a, aOut := makeReplica()
	b, bOut := makeReplica()
	input := metricInput()
	if err := a.ConsumeMetrics(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("inference not started")
	}
	otherBatch := pmetric.NewMetrics()
	input.CopyTo(otherBatch)
	otherBatch.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().AppendEmpty().SetIntValue(99)
	if err := b.ConsumeMetrics(context.Background(), otherBatch); err != nil {
		t.Fatal(err)
	}
	if (<-aOut).MetricCount() != 1 || (<-bOut).MetricCount() != 1 {
		t.Fatal("cold replicas did not retain input")
	}
	close(release)
	key := inputKey(input)
	awaitCondition(t, func() bool {
		_, aOK := a.cachedScore(key, time.Now())
		_, bOK := b.cachedScore(key, time.Now())
		return aOK && bOK
	})
	if calls.Load() != 1 {
		t.Fatalf("inference called %d times for one shared identity", calls.Load())
	}
	for _, replica := range []struct {
		c   *metricsProcessor
		out chan pmetric.Metrics
	}{{a, aOut}, {b, bOut}} {
		if err := replica.c.ConsumeMetrics(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		if (<-replica.out).MetricCount() != 0 {
			t.Fatal("replicas did not apply the shared drop assessment")
		}
	}
	// A replacement replica learns the existing score instead of spending another
	// inference request; the first batch is still retained during async lookup.
	d, dOut := makeReplica()
	d.ConsumeMetrics(context.Background(), input)
	if (<-dOut).MetricCount() != 1 {
		t.Fatal("replacement replica did not fail open")
	}
	awaitCondition(t, func() bool { _, ok := d.cachedScore(key, time.Now()); return ok })
	if calls.Load() != 1 {
		t.Fatal("replacement replica repeated inference")
	}
}

func TestSharedLeaseFencingAndRecovery(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Now()
	server.SetTime(now)
	cfg := sharedConfig(server.Addr())
	cfg.Coordination.MaxInFlight = 1
	c := testCoordinator(t, cfg)
	expectState(t, c, "metric", "old", "owner")
	expectState(t, c, "metric", "other", "wait")
	server.FastForward(11 * time.Second)
	server.SetTime(now.Add(11 * time.Second))
	expectState(t, c, "metric", "new", "owner")
	accepted, err := c.finish(context.Background(), "metric", "old", "success", sharedPayload(t, c, 0))
	if err != nil || accepted {
		t.Fatalf("stale owner publication = %v, %v", accepted, err)
	}
	accepted, err = c.finish(context.Background(), "metric", "new", "success", sharedPayload(t, c, 1))
	if err != nil || !accepted {
		t.Fatalf("new owner publication = %v, %v", accepted, err)
	}
	result := expectState(t, c, "metric", "reader", "hit")
	score, err := c.decode(result[1].(string), cfg.scoreTTL(), time.Now())
	if err != nil || score.Keep != 1 {
		t.Fatalf("new owner's decision lost: %v %v", score, err)
	}
}

func TestSharedBudgetsAndFailureCooldown(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Now().Truncate(time.Second)
	server.SetTime(now)
	cfg := sharedConfig(server.Addr())
	cfg.Coordination.MaxInFlight = 1
	cfg.Coordination.RequestsPerSecond = 1
	a := testCoordinator(t, cfg)
	b := testCoordinator(t, cfg)
	expectState(t, a, "one", "a", "owner")
	expectState(t, b, "two", "b", "wait")
	if ok, err := a.finish(context.Background(), "one", "a", "cancel", ""); err != nil || !ok {
		t.Fatal(ok, err)
	}
	// Releasing concurrency does not refund the start-rate budget.
	expectState(t, b, "two", "b", "wait")
	server.FastForward(time.Second)
	server.SetTime(now.Add(time.Second))
	expectState(t, b, "two", "b", "owner")
	if ok, err := b.finish(context.Background(), "two", "b", "failure", ""); err != nil || !ok {
		t.Fatal(ok, err)
	}
	// Move to the next rate window but remain within the shared cooldown.
	server.SetTime(now.Add(2 * time.Second))
	expectState(t, a, "three", "c", "wait")
	server.FastForward(time.Second)
	expectState(t, a, "three", "c", "owner")
	if ok, err := a.finish(context.Background(), "three", "c", "failure", ""); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ttl := server.TTL(a.prefix + "cooldown"); ttl != 2*time.Second {
		t.Fatalf("expected exponential cooldown, got %s", ttl)
	}
	otherCfg := *cfg
	otherCfg.Coordination.MaxInFlight = 2
	other := testCoordinator(t, &otherCfg)
	expectState(t, other, "other", "d", "mismatch")
}

func TestSharedVersionAndNamespaceIsolation(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := sharedConfig(server.Addr())
	a := testCoordinator(t, cfg)
	expectState(t, a, "metric", "a", "owner")
	if ok, err := a.finish(context.Background(), "metric", "a", "success", sharedPayload(t, a, 0)); err != nil || !ok {
		t.Fatal(ok, err)
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.Model = "different-model" },
		func(c *Config) { c.Coordination.Revision = "2" },
		func(c *Config) { c.Policy.DropThreshold = 0.2 },
		func(c *Config) { c.ContextAttributes = []string{"service.name"} },
		func(c *Config) { c.Coordination.Namespace = "other-tenant" },
	} {
		changed := *cfg
		change(&changed)
		b := testCoordinator(t, &changed)
		if b.prefix == a.prefix && b.assessmentVersion == a.assessmentVersion {
			t.Fatal("incompatible settings share assessments")
		}
		expectState(t, b, "metric", "b", "owner")
		b.finish(context.Background(), "metric", "b", "cancel", "")
	}
}

func TestSharedExpiryAndMalformedPayload(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := sharedConfig(server.Addr())
	c := testCoordinator(t, cfg)
	started := time.Now()
	score, err := c.decode(sharedPayload(t, c, 0.1), time.Second, started)
	if err != nil {
		t.Fatal(err)
	}
	cache := newScoreCache(1)
	cache.put("metric", score)
	if _, ok := cache.get("metric", started.Add(time.Second), cfg.scoreTTL()); ok {
		t.Fatal("shared TTL extended locally")
	}
	for _, payload := range []string{`{}`, `null`, `{"version":"wrong"}`, strings.Replace(sharedPayload(t, c, 0.1), `"keep":0.1`, `"keep":null`, 1)} {
		if _, err := c.decode(payload, time.Second, started); err == nil {
			t.Fatal("invalid shared assessment accepted")
		}
	}
	if _, err := c.decode(sharedPayload(t, c, 0.1), -1, started); err == nil {
		t.Fatal("non-expiring assessment accepted")
	}
}

func TestCoordinationOutageRetainsMetrics(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := sharedConfig(server.Addr())
	cfg.Mode = "reduce"
	cfg.Coordination.Timeout = "50ms"
	out := make(chan pmetric.Metrics, 4)
	c := testProcessor(t, cfg, func(_ context.Context, md pmetric.Metrics) error { out <- md; return nil })
	var calls atomic.Int32
	c.client.http.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("unexpected independent inference")
	})
	server.Close()
	if err := c.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())
	input := metricInput()
	if err := c.ConsumeMetrics(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if (<-out).MetricCount() != 1 {
		t.Fatal("Redis failure lost telemetry")
	}
	awaitCondition(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.failures > 0 })
	if calls.Load() != 0 {
		t.Fatal("Redis outage bypassed coordination")
	}
	if _, ok := c.cachedScore(inputKey(input), time.Now()); ok {
		t.Fatal("failed coordination cached a decision")
	}
}

func TestSharedAnnotationWriters(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := sharedConfig(server.Addr())
	input := metricInput()
	key := inputKey(input)
	var previous string
	for i := 0; i < 2; i++ {
		var output pmetric.Metrics
		c := testProcessor(t, cfg, func(_ context.Context, md pmetric.Metrics) error { output = md; return nil })
		defer c.Shutdown(context.Background())
		c.scores.put(key, metricScore{Keep: 1, Action: "keep", ScoredAt: time.Now()})
		if err := c.ConsumeMetrics(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		ms := output.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
		if ms.Len() != 5 {
			t.Fatal("missing companion metrics")
		}
		if _, ok := ms.At(0).Gauge().DataPoints().At(0).Attributes().Get("jev.collector.id"); ok {
			t.Fatal("original metric mutated")
		}
		for n := 1; n < ms.Len(); n++ {
			id, ok := ms.At(n).Gauge().DataPoints().At(0).Attributes().Get("jev.collector.id")
			if !ok || id.Str() != c.replicaID || id.Str() == previous {
				t.Fatal("companion writer identity collision")
			}
		}
		previous = c.replicaID
	}
}

func TestCoordinationConfig(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.Coordination.RedisURL = "not-a-url" },
		func(c *Config) { c.Coordination.LeaseTTL = "3s" },
		func(c *Config) { c.Coordination.Timeout = "0s" },
		func(c *Config) { c.Coordination.MaxInFlight = 0 },
		func(c *Config) { c.Coordination.RequestsPerSecond = 0 },
		func(c *Config) { c.Coordination.Revision = "" },
		func(c *Config) { c.Coordination.Namespace = "" },
	} {
		cfg := sharedConfig("localhost:6379")
		change(cfg)
		if cfg.Validate() == nil {
			t.Fatal("invalid coordination configuration accepted")
		}
	}
}

func TestSharedPublicationFailureDiscardsResult(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := sharedConfig(server.Addr())
	cfg.Coordination.Timeout = "50ms"
	c := testCoordinator(t, cfg)
	_, err := c.score(context.Background(), scoreJob{Key: "metric"}, func(context.Context, metricSummary) (metricScore, error) {
		server.Close()
		return metricScore{Keep: 0, Action: "drop"}, nil
	})
	if err == nil {
		t.Fatal("unpublished decision accepted")
	}
}

func TestSharedWaitCancellation(t *testing.T) {
	server := miniredis.RunT(t)
	c := testCoordinator(t, sharedConfig(server.Addr()))
	expectState(t, c, "metric", "other-owner", "owner")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.score(ctx, scoreJob{Key: "metric"}, func(context.Context, metricSummary) (metricScore, error) {
			t.Error("inference started without a lease")
			return metricScore{}, nil
		})
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not stop waiting worker")
	}
}

func TestSharedExpiredAssessmentRescores(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := sharedConfig(server.Addr())
	cfg.Policy.ScoreTTL = "1s"
	c := testCoordinator(t, cfg)
	var calls int
	infer := func(context.Context, metricSummary) (metricScore, error) {
		calls++
		return metricScore{Keep: 0.8, Action: "keep"}, nil
	}
	for i := 0; i < 2; i++ {
		if _, err := c.score(context.Background(), scoreJob{Key: "metric"}, infer); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatal("fresh assessment rescored")
	}
	server.FastForward(2 * time.Second)
	if _, err := c.score(context.Background(), scoreJob{Key: "metric"}, infer); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("expired assessment not rescored")
	}
}
