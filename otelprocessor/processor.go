package jevmetricsprocessor

import (
	"context"
	"crypto/rand"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

type metricsProcessor struct {
	cfg         *Config
	next        consumer.Metrics
	client      *jevClient
	coordinator *coordinator
	replicaID   string
	logger      *zap.Logger
	mu          sync.RWMutex
	scores      *scoreCache
	retryUntil  time.Time
	failures    int
	queued      map[string]bool
	jobs        chan scoreJob
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	now         func() time.Time
	telemetry   processorTelemetry
}

type processorTelemetry struct {
	cacheHits     metric.Int64Counter
	cacheMisses   metric.Int64Counter
	queued        metric.Int64Counter
	queueRejected metric.Int64Counter
	scored        metric.Int64Counter
	scoreFailures metric.Int64Counter
	processed     metric.Int64Counter
	kept          metric.Int64Counter
	dropped       metric.Int64Counter
	annotated     metric.Int64Counter
	scoreLatency  metric.Float64Histogram
}

func newProcessorTelemetry(provider metric.MeterProvider) processorTelemetry {
	if provider == nil {
		provider = noop.NewMeterProvider()
	}
	meter := provider.Meter("github.com/ishantanu/jevmetrics/otelprocessor")
	newCounter := func(name, description string) metric.Int64Counter {
		counter, _ := meter.Int64Counter(name, metric.WithDescription(description))
		return counter
	}
	latency, _ := meter.Float64Histogram("jevmetrics.score.duration", metric.WithUnit("s"), metric.WithDescription("Time spent obtaining a Jev metric assessment."))
	return processorTelemetry{
		cacheHits:     newCounter("jevmetrics.cache.hits", "Metrics served by the local assessment cache."),
		cacheMisses:   newCounter("jevmetrics.cache.misses", "Metrics without a fresh local assessment."),
		queued:        newCounter("jevmetrics.queue.enqueued", "Assessment jobs accepted by the background queue."),
		queueRejected: newCounter("jevmetrics.queue.rejected", "Assessment jobs rejected because the queue was full or cooling down."),
		scored:        newCounter("jevmetrics.score.success", "Successful Jev metric assessments."),
		scoreFailures: newCounter("jevmetrics.score.failure", "Failed Jev metric assessments."),
		processed:     newCounter("jevmetrics.metrics.processed", "Input metrics inspected by the processor."),
		kept:          newCounter("jevmetrics.metrics.kept", "Input metrics retained by the processor, including annotation and fail-open decisions."),
		dropped:       newCounter("jevmetrics.metrics.dropped", "Input metrics removed by reduce policy."),
		annotated:     newCounter("jevmetrics.metrics.annotated", "Input metrics with cached assessments emitted in annotate mode."),
		scoreLatency:  latency,
	}
}

func newProcessor(set processor.Settings, cfg *Config, next consumer.Metrics) (*metricsProcessor, error) {
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
	c := &metricsProcessor{cfg: cfg, next: next, client: client, logger: set.Logger, scores: newScoreCache(cfg.CacheSize), queued: map[string]bool{}, jobs: make(chan scoreJob, cfg.QueueSize), now: time.Now, telemetry: newProcessorTelemetry(set.MeterProvider)}
	// Independent replicas also need distinct writers for generated assessments.
	c.replicaID = rand.Text()
	if cfg.Coordination.RedisURL != "" {
		c.coordinator, err = newCoordinator(cfg)
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}
func (c *metricsProcessor) Start(_ context.Context, _ component.Host) error {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.logger.Info(
		"jevmetrics processor started",
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
func (c *metricsProcessor) Shutdown(_ context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	if c.coordinator != nil {
		return c.coordinator.client.Close()
	}
	return nil
}
func (c *metricsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *metricsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// Copy because reduce may remove metrics. Raw telemetry can be fanned out
	// independently before this processor and remains untouched.
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
				c.telemetry.processed.Add(ctx, 1)
				if protectedMetric(m.Name(), c.cfg.Policy) {
					c.telemetry.kept.Add(ctx, 1)
					return false
				}
				key := scoreKey(rm.Resource().Attributes(), sm.Scope(), m, rm.SchemaUrl(), sm.SchemaUrl())
				if key == "" {
					c.telemetry.kept.Add(ctx, 1)
					return false
				}
				score, ok := c.cachedScore(key, now)
				if !ok {
					c.telemetry.cacheMisses.Add(ctx, 1)
					c.enqueue(
						key,
						summarizeMetric(
							m,
							sm.Scope(),
							rm.Resource().Attributes(),
							c.cfg.ContextAttributes,
						),
					)
					c.telemetry.kept.Add(ctx, 1)
					return false
				}
				c.telemetry.cacheHits.Add(ctx, 1)
				scoresToEmit = append(scoresToEmit, struct {
					name  string
					score metricScore
				}{m.Name(), score})
				if c.cfg.Mode == "annotate" {
					c.telemetry.annotated.Add(ctx, 1)
					c.telemetry.kept.Add(ctx, 1)
					return false
				}
				keep := shouldKeep(score, c.cfg.Policy)
				if keep {
					c.telemetry.kept.Add(ctx, 1)
				} else {
					c.telemetry.dropped.Add(ctx, 1)
				}
				return !keep
			})
			if c.cfg.Mode == "annotate" {
				appendScoreMetrics(sm.Metrics(), scoresToEmit, now, c.cfg.Model, c.replicaID)
			}
		}
	}
	// Even if Jev is down, unknown/unscored metrics are kept. This is fail-open.
	if err := c.next.ConsumeMetrics(ctx, out); err != nil {
		return err
	}
	return nil
}

func (c *metricsProcessor) cachedScore(key string, now time.Time) (metricScore, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scores.get(key, now, c.cfg.scoreTTL())
}
func (c *metricsProcessor) enqueue(
	key string,
	summary metricSummary,
) {
	c.mu.Lock()

	if c.queued[key] {
		c.mu.Unlock()
		return
	}
	if c.now().Before(c.retryUntil) {
		c.telemetry.queueRejected.Add(context.Background(), 1)
		c.mu.Unlock()
		return
	}

	c.queued[key] = true
	c.mu.Unlock()

	select {
	case c.jobs <- scoreJob{
		Key:     key,
		Summary: summary,
	}:
		c.telemetry.queued.Add(context.Background(), 1)
		c.logger.Info(
			"metric queued for Jev scoring",
			zap.String("metric", summary.Name),
		)

	default:
		c.telemetry.queueRejected.Add(context.Background(), 1)
		c.mu.Lock()
		delete(c.queued, key)
		c.mu.Unlock()

		c.logger.Warn(
			"jevmetrics scoring queue full; keeping metric fail-open",
			zap.String("metric", summary.Name),
		)
	}
}
func (c *metricsProcessor) worker(ctx context.Context) {
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
				"assessing metric",
				zap.String("metric", job.Summary.Name),
			)

			var s metricScore
			var err error
			started := time.Now()
			if c.coordinator != nil {
				s, err = c.coordinator.score(ctx, job, c.client.scoreMetric)
			} else {
				s, err = c.client.scoreMetric(ctx, job.Summary)
			}

			c.mu.Lock()
			delete(c.queued, job.Key)

			if err == nil {
				c.telemetry.scored.Add(ctx, 1)
				if c.coordinator == nil {
					s.ScoredAt = c.now()
				}
				c.scores.put(job.Key, s)
				c.failures = 0
				c.retryUntil = time.Time{}
			} else {
				c.telemetry.scoreFailures.Add(ctx, 1)
				// Processor-wide backoff also bounds retries across new identities.
				if c.failures < 6 {
					c.failures++
				}
				delay := time.Second << (c.failures - 1)
				c.retryUntil = c.now().Add(delay)
			}

			c.mu.Unlock()
			c.telemetry.scoreLatency.Record(ctx, time.Since(started).Seconds())

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

		}
	}
}

func appendScoreMetrics(ms pmetric.MetricSlice, scored []struct {
	name  string
	score metricScore
}, now time.Time, model, replicaID string) {
	start := ms.Len()
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
	if replicaID != "" {
		for i := start; i < ms.Len(); i++ {
			ms.At(i).Gauge().DataPoints().At(0).Attributes().PutStr("jev.collector.id", replicaID)
		}
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
