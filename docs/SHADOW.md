# Evaluate retention before filtering

Run `mode: annotate` to preserve original input while evaluating the exact policy
used by `reduce`. No additional inference is performed for the preview.

The processor emits these **Collector internal telemetry** counters:

| Instrument | Counts |
| --- | --- |
| `jevmetrics.policy.instruments` | Input instrument occurrences, once per batch occurrence |
| `jevmetrics.policy.datapoints` | Input datapoints belonging to those occurrences |
| `jevmetrics.inference.requests` | Inference attempts started, including failed attempts; excludes local and Redis cache hits |

Policy counters have only `mode`, `decision` (`keep` or `drop`), and `reason`
attributes. They never contain instrument names or resource values. Collector
instrumentation may supply its own component identity attributes.

| Reason | Decision |
| --- | --- |
| `protected` | Keep; explicit name/prefix protection takes precedence |
| `invalid_identity` | Keep; identity could not be encoded safely |
| `awaiting_assessment` | Keep; no fresh local assessment, including expiry, queue pressure, and failed inference |
| `cached_score` | Keep or drop according to thresholds |
| `uncertain` | Keep; score lies between thresholds |

In `annotate`, `decision=drop` means **would drop**. In `reduce`, it means the
processor removed the input instrument and its datapoints. Both use the same
decision function; this does not guarantee that separate replicas or runs obtain
identical model assessments. Fresh cached drop decisions still apply during API
outages until expiry. These are processing decisions, not confirmed delivery to
a backend. Generated companion gauges are excluded from these counts.

For a given processor in annotation mode, calculate prospective datapoint reduction
as the rate of policy datapoints with `decision=drop` divided by the rate of all
policy datapoints. Keep `mode=annotate` on both sides. Treat a zero denominator
as no observations. Show the breakdown by reason alongside this ratio so cold
caches and protected traffic remain visible.

These are observed datapoint volumes, **not unique time-series counts, global
cardinality, bytes saved, or guaranteed storage savings**. Repeated batches and
retries count again. A histogram datapoint counts as one regardless of its buckets.

Use `jevmetrics.inference.requests` alongside existing cache, queue, failure, and
`jevmetrics.score.duration` telemetry. The duration measures assessment retrieval,
including Redis coordination when enabled; it is not a pure API latency metric
and excludes local token waiting. Request counts do not measure tokens or cost.

Configure a Collector internal telemetry reader to export these instruments. The
application-metrics Prometheus exporter in `config-local.yaml` serves companion
gauges; it does not automatically expose these internal counters. Exported names
may be normalized by the telemetry reader/exporter.

For per-instrument inspection, temporarily set `service.telemetry.logs.level` to
`debug`. Each `metric retention decision` event includes metric name, mode,
decision, and reason. This can generate substantial logs and expose metric names;
it is disabled at the normal info level.

## Evaluation fixture

`otelprocessor/testdata/shadow_policy.json` contains synthetic policy cases.
`go test -run TestShadowMatchesFiltering ./...` from `otelprocessor` checks that
annotation previews and filtering agree, including protected, cold, expired,
uncertain, and threshold-boundary cases. The tests also verify input preservation
and original datapoint counts.

These are policy regression fixtures, not engineer-reviewed ground truth for Jev
quality. Before enabling filtering, collect representative metadata, have workload
owners label required instruments, and compare actual assessments against those
labels. Record model, prompt, policy, and observation context. Include renamed or
reworded equivalents to measure decision consistency. Report required instruments
selected for dropping separately from volume reduction.
