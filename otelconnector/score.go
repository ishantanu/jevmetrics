package jevmetricsconnector

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type metricSummary struct {
	Name            string            `json:"name"`
	Description     string            `json:"description,omitempty"`
	Unit            string            `json:"unit,omitempty"`
	Type            string            `json:"type"`
	DataPoints      int               `json:"data_points"`
	EstimatedSeries int               `json:"estimated_series"`
	AttributeKeys   []string          `json:"attribute_keys,omitempty"`
	ResourceContext map[string]string `json:"resource_context,omitempty"`
	ScopeName       string            `json:"scope_name,omitempty"`
	ScopeVersion    string            `json:"scope_version,omitempty"`
	Temporality     string            `json:"temporality,omitempty"`
	Monotonic       *bool             `json:"monotonic,omitempty"`
}

type metricScore struct {
	Relevance  float64
	Redundancy float64
	Keep       float64
	Action     string
	ScoredAt   time.Time
}

type scoreJob struct {
	Key         string
	Summary     metricSummary
	Resource    pcommon.Resource
	Scope       pcommon.InstrumentationScope
	ResourceURL string
	ScopeURL    string
}

func summarizeMetric(m pmetric.Metric, scope pcommon.InstrumentationScope, resource pcommon.Map, allowed []string) metricSummary {
	keys := map[string]struct{}{}
	fingerprints := map[string]struct{}{}
	datapoints := 0
	visit := func(attrs pcommon.Map) {
		datapoints++
		var parts []string
		attrs.Range(func(k string, v pcommon.Value) bool {
			keys[k] = struct{}{}
			parts = append(parts, k+"="+fmt.Sprint(v.AsRaw()))
			return true
		})
		sort.Strings(parts)
		fingerprints[strings.Join(parts, "\x00")] = struct{}{}
	}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			visit(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			visit(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			visit(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			visit(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			visit(dps.At(i).Attributes())
		}
	}
	attrKeys := make([]string, 0, len(keys))
	for k := range keys {
		attrKeys = append(attrKeys, k)
	}
	sort.Strings(attrKeys)
	summary := metricSummary{
		Name: m.Name(), Description: m.Description(), Unit: m.Unit(), Type: m.Type().String(),
		DataPoints: datapoints, EstimatedSeries: len(fingerprints), AttributeKeys: attrKeys,
		ResourceContext: copyAllowedContext(resource, allowed), ScopeName: scope.Name(), ScopeVersion: scope.Version(),
	}
	switch m.Type() {
	case pmetric.MetricTypeSum:
		t := m.Sum().AggregationTemporality().String()
		summary.Temporality = t
		mono := m.Sum().IsMonotonic()
		summary.Monotonic = &mono
	case pmetric.MetricTypeHistogram:
		summary.Temporality = m.Histogram().AggregationTemporality().String()
	case pmetric.MetricTypeExponentialHistogram:
		summary.Temporality = m.ExponentialHistogram().AggregationTemporality().String()
	}
	return summary
}

// JSON preserves value types and sorts map keys; hashing bounds key storage.
// Include complete resource/scope identity and metadata that affects inference.
func scoreKey(resource pcommon.Map, scope pcommon.InstrumentationScope, m pmetric.Metric, resourceURL, scopeURL string) string {
	summary := summarizeIdentity(m, scope)
	data, err := json.Marshal([]any{resource.AsRaw(), scope.Attributes().AsRaw(), resourceURL, scopeURL, summary})
	if err != nil {
		return ""
	} // Unsupported metadata must never share a cached decision.
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func summarizeIdentity(m pmetric.Metric, scope pcommon.InstrumentationScope) metricSummary {
	s := metricSummary{Name: m.Name(), Description: m.Description(), Unit: m.Unit(), Type: m.Type().String(), ScopeName: scope.Name(), ScopeVersion: scope.Version()}
	switch m.Type() {
	case pmetric.MetricTypeSum:
		s.Temporality = m.Sum().AggregationTemporality().String()
		mono := m.Sum().IsMonotonic()
		s.Monotonic = &mono
	case pmetric.MetricTypeHistogram:
		s.Temporality = m.Histogram().AggregationTemporality().String()
	case pmetric.MetricTypeExponentialHistogram:
		s.Temporality = m.ExponentialHistogram().AggregationTemporality().String()
	}
	return s
}

func protectedMetric(name string, p Policy) bool {
	for _, n := range p.ProtectedMetrics {
		if name == n {
			return true
		}
	}
	for _, prefix := range p.ProtectedPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func shouldKeep(score metricScore, p Policy) bool {
	if score.Keep >= p.KeepThreshold {
		return true
	}
	if score.Keep <= p.DropThreshold {
		return false
	}
	// Uncertain zone is fail-open.
	return true
}

func copyAllowedContext(resource pcommon.Map, allowed []string) map[string]string {
	if len(allowed) == 0 {
		return nil
	}
	out := make(map[string]string, len(allowed))
	for _, key := range allowed {
		v, ok := resource.Get(key)
		if !ok {
			continue
		}
		out[key] = fmt.Sprint(v.AsRaw())
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
