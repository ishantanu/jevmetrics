package jevmetricsconnector

import (
	"context"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type connectorImp struct {
	cfg        *Config
	next       consumer.Metrics
	client     *jevClient
	logger     *zap.Logger
	mu         sync.RWMutex
	scores     *scoreCache
	retryUntil time.Time
	failures   int
	queued     map[string]bool
	jobs       chan scoreJob
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	now        func() time.Time
}

func newConnector(set connector.Settings, cfg *Config, next consumer.Metrics) (*connectorImp, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	normalized := *cfg
	normalized.Mode = strings.ToLower(cfg.Mode)
	cfg = &normalized
	client, err := newJevClient(cfg)
	if err != nil {
		return nil, err
	}
	return &connectorImp{cfg: cfg, next: next, client: client, logger: set.Logger, scores: newScoreCache(cfg.CacheSize), queued: map[string]bool{}, jobs: make(chan scoreJob, cfg.QueueSize), now: time.Now}, nil
}
func (c *connectorImp) Start(_ context.Context, _ component.Host) error {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.logger.Info(
		"jevmetrics connector started",
		zap.String("mode", c.cfg.Mode),
		zap.Int("workers", c.cfg.Workers),
		zap.Int("queue_size", c.cfg.QueueSize),
		zap.String("model", c.cfg.Model),
		zap.String("base_url", c.cfg.BaseURL),
	)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		interval := c.cfg.scoreTTL()
		if interval > time.Minute {
			interval = time.Minute
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.mu.Lock()
				c.scores.prune(c.now(), c.cfg.scoreTTL())
				c.mu.Unlock()
			}
		}
	}()
	for i := 0; i < c.cfg.Workers; i++ {
		c.wg.Add(1)
		go c.worker(ctx)
	}

	return nil
}
func (c *connectorImp) Shutdown(_ context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	return nil
}
func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *connectorImp) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// Copy because route/reduce may remove metrics. Raw telemetry can be fanned out
	// independently before this connector and remains untouched.
	out := pmetric.NewMetrics()
	md.CopyTo(out)
	now := c.now()
	for ri := 0; ri < out.ResourceMetrics().Len(); ri++ {
		rm := out.ResourceMetrics().At(ri)
		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)
			ms := sm.Metrics()
			scoresToEmit := make([]struct {
				name  string
				score metricScore
			}, 0)
			ms.RemoveIf(func(m pmetric.Metric) bool {
				if protectedMetric(m.Name(), c.cfg.Policy) {
					return false
				}
				key := scoreKey(rm.Resource().Attributes(), sm.Scope(), m, rm.SchemaUrl(), sm.SchemaUrl())
				if key == "" {
					return false
				}
				score, ok := c.cachedScore(key, now)
				if !ok {
					c.enqueue(
						key,
						summarizeMetric(
							m,
							sm.Scope(),
							rm.Resource().Attributes(),
							c.cfg.ContextAttributes,
						),
						rm.Resource(),
						sm.Scope(),
						rm.SchemaUrl(),
						sm.SchemaUrl(),
					)
					return false
				}
				scoresToEmit = append(scoresToEmit, struct {
					name  string
					score metricScore
				}{m.Name(), score})
				if c.cfg.Mode == "annotate" {
					return false
				}
				return !shouldKeep(score, c.cfg.Policy)
			})
			if c.cfg.Mode == "annotate" {
				appendScoreMetrics(sm.Metrics(), scoresToEmit, now, c.cfg.Model)
			}
		}
	}
	// Even if Jev is down, unknown/unscored metrics are kept. This is fail-open.
	if err := c.next.ConsumeMetrics(ctx, out); err != nil {
		return err
	}
	return nil
}

func (c *connectorImp) cachedScore(key string, now time.Time) (metricScore, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scores.get(key, now, c.cfg.scoreTTL())
}
func (c *connectorImp) enqueue(
	key string,
	summary metricSummary,
	resource pcommon.Resource,
	scope pcommon.InstrumentationScope,
	resourceURL string,
	scopeURL string,
) {
	c.mu.Lock()

	if c.queued[key] || c.now().Before(c.retryUntil) {
		c.mu.Unlock()
		return
	}

	c.queued[key] = true
	c.mu.Unlock()

	resourceCopy := pcommon.NewResource()
	resource.CopyTo(resourceCopy)

	scopeCopy := pcommon.NewInstrumentationScope()
	scope.CopyTo(scopeCopy)

	select {
	case c.jobs <- scoreJob{
		Key:         key,
		Summary:     summary,
		Resource:    resourceCopy,
		Scope:       scopeCopy,
		ResourceURL: resourceURL,
		ScopeURL:    scopeURL,
	}:
		c.logger.Info(
			"metric queued for Jev scoring",
			zap.String("metric", summary.Name),
		)

	default:
		c.mu.Lock()
		delete(c.queued, key)
		c.mu.Unlock()

		c.logger.Warn(
			"jevmetrics scoring queue full; keeping metric fail-open",
			zap.String("metric", summary.Name),
		)
	}
}
func (c *connectorImp) worker(ctx context.Context) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case job := <-c.jobs:
			// Drop pending work during an outage cooldown; future batches retry.
			c.mu.Lock()
			cooling := c.now().Before(c.retryUntil)
			if cooling {
				delete(c.queued, job.Key)
			}
			c.mu.Unlock()
			if cooling {
				continue
			}
			c.logger.Info(
				"sending metric to Jev",
				zap.String("metric", job.Summary.Name),
			)

			s, err := c.client.scoreMetric(ctx, job.Summary)

			c.mu.Lock()
			delete(c.queued, job.Key)

			if err == nil {
				s.ScoredAt = c.now()
				c.scores.put(job.Key, s)
				c.failures = 0
				c.retryUntil = time.Time{}
			} else {
				// Connector-wide backoff also bounds retries across new identities.
				if c.failures < 6 {
					c.failures++
				}
				delay := time.Second << (c.failures - 1)
				c.retryUntil = c.now().Add(delay)
			}

			c.mu.Unlock()

			if err != nil {
				c.logger.Warn(
					"Jev metric scoring failed; metric remains retained",
					zap.String("metric", job.Summary.Name),
					zap.Error(err),
				)
				continue
			}

			c.logger.Info(
				"Jev metric score received",
				zap.String("metric", job.Summary.Name),
				zap.Float64("relevance", s.Relevance),
				zap.Float64("redundancy", s.Redundancy),
				zap.Float64("keep_probability", s.Keep),
				zap.String("action", s.Action),
			)

			if c.cfg.Mode == "annotate" {
				if err := c.emitScoreMetrics(ctx, job, s); err != nil {
					c.logger.Warn(
						"failed to emit Jev score metrics",
						zap.String("metric", job.Summary.Name),
						zap.Error(err),
					)
				}
			}
		}
	}
}

func appendScoreMetrics(ms pmetric.MetricSlice, scored []struct {
	name  string
	score metricScore
}, now time.Time, model string) {
	for _, item := range scored {
		appendScoreGauge(ms, "jev.metric.relevance", "Jev estimate of durable operational value for an input metric.", item.name, item.score.Relevance, now, model)
		appendScoreGauge(ms, "jev.metric.redundancy", "Jev estimate that an input metric is redundant/noisy.", item.name, item.score.Redundancy, now, model)
		appendScoreGauge(ms, "jev.metric.keep_probability", "Jev estimate that an input metric should remain in primary metrics storage.", item.name, item.score.Keep, now, model)
		m := ms.AppendEmpty()
		m.SetName("jev.metric.recommended_action")
		m.SetDescription("Recommended storage action for an input metric.")
		g := m.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetDoubleValue(1)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
		dp.Attributes().PutStr("metric.name", item.name)
		dp.Attributes().PutStr("action", item.score.Action)
		dp.Attributes().PutStr("jev.model", model)
	}
}
func appendScoreGauge(ms pmetric.MetricSlice, name, desc, input string, v float64, now time.Time, model string) {
	m := ms.AppendEmpty()
	m.SetName(name)
	m.SetDescription(desc)
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetDoubleValue(v)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("metric.name", input)
	dp.Attributes().PutStr("jev.model", model)
}

func (c *connectorImp) emitScoreMetrics(
	ctx context.Context,
	job scoreJob,
	score metricScore,
) error {
	md := pmetric.NewMetrics()

	rm := md.ResourceMetrics().AppendEmpty()
	job.Resource.CopyTo(rm.Resource())
	rm.SetSchemaUrl(job.ResourceURL)

	sm := rm.ScopeMetrics().AppendEmpty()
	job.Scope.CopyTo(sm.Scope())
	sm.SetSchemaUrl(job.ScopeURL)

	scored := []struct {
		name  string
		score metricScore
	}{
		{
			name:  job.Summary.Name,
			score: score,
		},
	}

	appendScoreMetrics(
		sm.Metrics(),
		scored,
		c.now(),
		c.cfg.Model,
	)

	return c.next.ConsumeMetrics(ctx, md)
}
