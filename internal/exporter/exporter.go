package exporter

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ishantanu/jevmetrics/internal/config"
	"github.com/ishantanu/jevmetrics/internal/evaluator"
	"github.com/ishantanu/jevmetrics/internal/features"
	promclient "github.com/ishantanu/jevmetrics/internal/prom"
)

type signalState struct {
	Anomaly       float64
	Impact        float64
	Investigate   float64
	FailureDomain string
	LastSuccess   int64
	QueryErrors   map[string]uint64
}

type Exporter struct {
	client *promclient.Client
	eval   evaluator.Evaluator
	cfg    config.Config
	mu     sync.RWMutex
	state  map[string]*signalState
}

func New(client *promclient.Client, eval evaluator.Evaluator, cfg config.Config) *Exporter {
	e := &Exporter{client: client, eval: eval, cfg: cfg, state: map[string]*signalState{}}
	for _, s := range cfg.Signals {
		e.state[s.Name] = &signalState{QueryErrors: map[string]uint64{}}
	}
	return e
}

func (e *Exporter) Run(ctx context.Context) {
	e.collect(ctx)
	t := time.NewTicker(e.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.collect(ctx)
		}
	}
}

func (e *Exporter) collect(ctx context.Context) {
	for _, s := range e.cfg.Signals {
		errorRate, ok := e.query(ctx, s.Name, "errors", s.ErrorQuery)
		if !ok {
			continue
		}
		latency, ok := e.query(ctx, s.Name, "latency", s.LatencyQuery)
		if !ok {
			continue
		}
		traffic, ok := e.query(ctx, s.Name, "traffic", s.TrafficQuery)
		if !ok {
			continue
		}
		saturation, ok := e.query(ctx, s.Name, "saturation", s.SaturationQuery)
		if !ok {
			continue
		}

		res, err := e.eval.Evaluate(features.Extract(features.Raw{
			ErrorRate: errorRate, Latency: latency, Traffic: traffic, Saturation: saturation,
		}))
		if err != nil {
			log.Printf("%s evaluate: %v", s.Name, err)
			continue
		}

		e.mu.Lock()
		st := e.state[s.Name]
		st.Anomaly = res.AnomalyProbability
		st.Impact = res.UserImpactProbability
		st.Investigate = res.InvestigateProbability
		st.FailureDomain = res.FailureDomain
		st.LastSuccess = time.Now().Unix()
		e.mu.Unlock()
	}
}

func (e *Exporter) query(ctx context.Context, signal, feature, q string) (float64, bool) {
	v, err := e.client.Query(ctx, q)
	if err != nil {
		e.mu.Lock()
		e.state[signal].QueryErrors[feature]++
		e.mu.Unlock()
		log.Printf("%s %s query: %v", signal, feature, err)
		return 0, false
	}
	return v, true
}

func (e *Exporter) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	e.mu.RLock()
	defer e.mu.RUnlock()

	names := make([]string, 0, len(e.state))
	for name := range e.state {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	b.WriteString("# HELP jev_anomaly_probability Estimated anomaly probability between 0 and 1.\n")
	b.WriteString("# TYPE jev_anomaly_probability gauge\n")
	b.WriteString("# HELP jev_user_impact_probability Estimated user-impact probability between 0 and 1.\n")
	b.WriteString("# TYPE jev_user_impact_probability gauge\n")
	b.WriteString("# HELP jev_investigate_probability Estimated probability that a signal warrants investigation.\n")
	b.WriteString("# TYPE jev_investigate_probability gauge\n")
	b.WriteString("# HELP jev_failure_domain One-hot failure-domain classification.\n")
	b.WriteString("# TYPE jev_failure_domain gauge\n")
	b.WriteString("# HELP jev_last_success_unixtime Unix timestamp of the last successful evaluation.\n")
	b.WriteString("# TYPE jev_last_success_unixtime gauge\n")
	b.WriteString("# HELP jev_source_query_errors_total Source query errors by signal and feature.\n")
	b.WriteString("# TYPE jev_source_query_errors_total counter\n")

	for _, name := range names {
		st := e.state[name]
		label := fmt.Sprintf("signal=%q", name)
		fmt.Fprintf(&b, "jev_anomaly_probability{%s} %g\n", label, st.Anomaly)
		fmt.Fprintf(&b, "jev_user_impact_probability{%s} %g\n", label, st.Impact)
		fmt.Fprintf(&b, "jev_investigate_probability{%s} %g\n", label, st.Investigate)
		fmt.Fprintf(&b, "jev_last_success_unixtime{%s} %d\n", label, st.LastSuccess)
		for _, domain := range []string{"resource", "errors", "latency", "unknown"} {
			v := 0
			if domain == st.FailureDomain {
				v = 1
			}
			fmt.Fprintf(&b, "jev_failure_domain{%s,domain=%q} %d\n", label, domain, v)
		}
		features := make([]string, 0, len(st.QueryErrors))
		for f := range st.QueryErrors {
			features = append(features, f)
		}
		sort.Strings(features)
		for _, f := range features {
			fmt.Fprintf(&b, "jev_source_query_errors_total{%s,feature=%q} %d\n", label, f, st.QueryErrors[f])
		}
	}
	_, _ = w.Write([]byte(b.String()))
}
