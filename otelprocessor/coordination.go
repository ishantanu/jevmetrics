package jevmetricsprocessor

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	randv2 "math/rand/v2"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// One primary executes these scripts atomically. All keys use the same hash tag.
// Lease tokens fence late writers; Redis time governs leases and rate windows.
// Redis failover with lost writes can still cause duplicate inference. This is
// coordination for advisory assessments, not a consensus or exactly-once system.
var acquireAssessment = redis.NewScript(`
local settings = redis.call('GET', KEYS[7])
if settings and settings ~= ARGV[5] then return {'mismatch'} end
redis.call('SET', KEYS[7], ARGV[5], 'PX', tonumber(ARGV[2]) * 2)
local score = redis.call('GET', KEYS[1])
if score then return {'hit', score, tostring(redis.call('PTTL', KEYS[1]))} end
if redis.call('EXISTS', KEYS[2]) == 1 or redis.call('EXISTS', KEYS[5]) == 1 then
  return {'wait'}
end
local tm = redis.call('TIME')
local now = tonumber(tm[1]) * 1000 + math.floor(tonumber(tm[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now)
if redis.call('ZCARD', KEYS[3]) >= tonumber(ARGV[3]) then return {'wait'} end
local window = redis.call('HGET', KEYS[4], 'window')
local count = tonumber(redis.call('HGET', KEYS[4], 'count') or '0')
if window ~= tm[1] then count = 0 end
if count >= tonumber(ARGV[4]) then return {'wait'} end
redis.call('HSET', KEYS[4], 'window', tm[1], 'count', count + 1)
redis.call('PEXPIRE', KEYS[4], 2000)
redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[2])
redis.call('ZADD', KEYS[3], now + tonumber(ARGV[2]), ARGV[1])
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[2]) * 2)
return {'owner'}
`)

var finishAssessment = redis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] then
  redis.call('ZREM', KEYS[3], ARGV[1])
  return 0
end
if ARGV[2] == 'success' then
  redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
  redis.call('DEL', KEYS[5], KEYS[6])
elseif ARGV[2] == 'failure' then
  local failures = math.min(tonumber(redis.call('GET', KEYS[6]) or '0') + 1, 6)
  redis.call('SET', KEYS[6], failures, 'PX', 300000)
  redis.call('SET', KEYS[5], '1', 'PX', 1000 * (2 ^ (failures - 1)))
end
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`)

type coordinator struct {
	client                         *redis.Client
	prefix                         string
	assessmentVersion              string
	timeout, leaseTTL, scoreTTL    time.Duration
	maxInFlight, requestsPerSecond int
}

func newCoordinator(cfg *Config) (*coordinator, error) {
	options, err := redis.ParseURL(string(cfg.Coordination.RedisURL))
	if err != nil {
		return nil, errors.New("invalid coordination.redis_url")
	}
	timeout, _ := time.ParseDuration(cfg.Coordination.Timeout)
	lease, _ := time.ParseDuration(cfg.Coordination.LeaseTTL)
	options.DialTimeout = timeout
	options.ReadTimeout = timeout
	options.WriteTimeout = timeout
	options.PoolTimeout = timeout
	options.ContextTimeoutEnabled = true
	// Admission and publication must not be retried after ambiguous I/O failures.
	options.MaxRetries = -1
	options.Protocol = 2
	options.DisableIdentity = true
	return &coordinator{
		client:            redis.NewClient(options),
		prefix:            fmt.Sprintf("jevmetrics:{%x}:", sha256.Sum256([]byte(cfg.Coordination.Namespace))),
		assessmentVersion: assessmentVersion(cfg),
		timeout:           timeout, leaseTTL: lease, scoreTTL: cfg.scoreTTL(),
		maxInFlight: cfg.Coordination.MaxInFlight, requestsPerSecond: cfg.Coordination.RequestsPerSecond,
	}, nil
}

// Credentials and replica-local settings must not enter shared keys. Revision
// provides explicit invalidation when a model alias changes underneath us.
func assessmentVersion(cfg *Config) string {
	attrs := append([]string(nil), cfg.ContextAttributes...)
	sort.Strings(attrs)
	policy := cfg.Policy
	policy.ProtectedMetrics = append([]string(nil), policy.ProtectedMetrics...)
	policy.ProtectedPrefixes = append([]string(nil), policy.ProtectedPrefixes...)
	sort.Strings(policy.ProtectedMetrics)
	sort.Strings(policy.ProtectedPrefixes)
	data, _ := json.Marshal([]any{"assessment-v1", cfg.Coordination.Revision, cfg.BaseURL, cfg.Model, metricQuestions(), attrs, policy})
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func (c *coordinator) keys(key string) []string {
	metric := c.prefix + c.assessmentVersion + ":" + key
	return []string{metric + ":score", metric + ":lease", c.prefix + "inflight", c.prefix + "rate", c.prefix + "cooldown", c.prefix + "failures", c.prefix + "settings"}
}

type sharedAssessment struct {
	Version    string   `json:"version"`
	Relevance  *float64 `json:"relevance"`
	Redundancy *float64 `json:"redundancy"`
	Keep       *float64 `json:"keep"`
	Action     string   `json:"action"`
}

func (c *coordinator) decode(payload string, remaining time.Duration, started time.Time) (metricScore, error) {
	var s sharedAssessment
	if err := json.Unmarshal([]byte(payload), &s); err != nil {
		return metricScore{}, errors.New("invalid shared assessment JSON")
	}
	if s.Version != c.assessmentVersion || remaining <= 0 || remaining > c.scoreTTL {
		return metricScore{}, errors.New("invalid shared assessment version or expiry")
	}
	for _, v := range []*float64{s.Relevance, s.Redundancy, s.Keep} {
		if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) || *v < 0 || *v > 1 {
			return metricScore{}, errors.New("invalid shared assessment probability")
		}
	}
	if s.Action != "keep" && s.Action != "reduce" && s.Action != "drop" {
		return metricScore{}, errors.New("invalid shared assessment action")
	}
	// Count request latency against freshness. Do not trust another host's clock
	// or extend TTL by starting a new lifetime in each replica's local cache.
	return metricScore{Relevance: *s.Relevance, Redundancy: *s.Redundancy, Keep: *s.Keep, Action: s.Action,
		ScoredAt: started.Add(remaining - c.scoreTTL)}, nil
}

func (c *coordinator) acquire(ctx context.Context, key, token string) ([]interface{}, error) {
	opCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	settings := fmt.Sprintf("%d/%d/%d", c.maxInFlight, c.requestsPerSecond, c.leaseTTL.Milliseconds())
	return acquireAssessment.Run(opCtx, c.client, c.keys(key), token, c.leaseTTL.Milliseconds(), c.maxInFlight, c.requestsPerSecond, settings).Slice()
}

func (c *coordinator) finish(ctx context.Context, key, token, outcome, payload string) (bool, error) {
	opCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	n, err := finishAssessment.Run(opCtx, c.client, c.keys(key), token, outcome, payload, c.scoreTTL.Milliseconds()).Int()
	return n == 1, err
}

// score is called only by background workers. The first lease holder's observed
// batch supplies the assessment for this identity until expiry. Other replicas
// reuse it; this is not aggregation of their observations or global cardinality.
func (c *coordinator) score(ctx context.Context, job scoreJob, infer func(context.Context, metricSummary) (metricScore, error)) (metricScore, error) {
	for {
		if err := ctx.Err(); err != nil {
			return metricScore{}, err
		}
		token := rand.Text()
		started := time.Now()
		result, err := c.acquire(ctx, job.Key, token)
		if err != nil {
			return metricScore{}, fmt.Errorf("shared assessment lookup: %w", err)
		}
		if len(result) == 0 {
			return metricScore{}, errors.New("empty coordination response")
		}
		switch result[0] {
		case "hit":
			if len(result) != 3 {
				return metricScore{}, errors.New("invalid coordination response")
			}
			payload, ok := result[1].(string)
			ttl, err := strconv.ParseInt(fmt.Sprint(result[2]), 10, 64)
			if !ok || err != nil {
				return metricScore{}, errors.New("invalid shared assessment expiry")
			}
			return c.decode(payload, time.Duration(ttl)*time.Millisecond, started)
		case "mismatch":
			return metricScore{}, errors.New("coordination namespace has incompatible budget or lease settings")
		case "owner":
			// Include Redis admission latency in the lease deadline. Late inference
			// results cannot overwrite an assessment published by a new owner.
			inferCtx, cancel := context.WithDeadline(ctx, started.Add(c.leaseTTL-c.timeout))
			score, inferErr := infer(inferCtx, job.Summary)
			cancel()
			outcome := "success"
			if inferErr != nil {
				outcome = "failure"
			}
			if ctx.Err() != nil {
				outcome = "cancel"
			}
			payload, err := json.Marshal(sharedAssessment{Version: c.assessmentVersion, Relevance: &score.Relevance, Redundancy: &score.Redundancy, Keep: &score.Keep, Action: score.Action})
			if err != nil {
				outcome = "failure"
				inferErr = err
			}
			publishedAt := time.Now()
			// Bounded cleanup is allowed after shutdown cancellation to release the
			// lease. A crash is handled by lease/slot expiry on the next request.
			accepted, finishErr := c.finish(context.WithoutCancel(ctx), job.Key, token, outcome, string(payload))
			if inferErr != nil {
				return metricScore{}, inferErr
			}
			if finishErr != nil {
				return metricScore{}, fmt.Errorf("shared assessment publication: %w", finishErr)
			}
			if !accepted || outcome != "success" {
				return metricScore{}, errors.New("assessment lease lost or cancelled")
			}
			score.ScoredAt = publishedAt
			return score, nil
		case "wait":
			// Jitter polling across replicas; it consumes worker slots, not the
			// telemetry delivery goroutine. Shutdown cancels the wait immediately.
			timer := time.NewTimer(100*time.Millisecond + time.Duration(randv2.IntN(150))*time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return metricScore{}, ctx.Err()
			case <-timer.C:
			}
		default:
			return metricScore{}, errors.New("unknown coordination response")
		}
	}
}
