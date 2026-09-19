# jevmetrics

**Jev inference for metric assessment and retention in OpenTelemetry.**

`jevmetrics` is an experimental metrics-to-metrics OpenTelemetry Collector connector. It calls [TypeSafe’s Jev model](https://docs.typesafe.ai/api) to infer the likely operational value of metric instruments from their metadata, then applies deterministic policy to the returned probabilities.

Use it to assess unfamiliar instrumentation, review candidates for reduced retention, and selectively filter metrics before they reach a primary backend. Inference runs asynchronously, and cached assessments let subsequent batches use the same decision without another API call.

**Status: alpha.** Start with `annotate`, which preserves incoming metrics and exports the assessments for review. Filtering effectiveness and cost savings need evaluation on your own telemetry.

## Why inference belongs in the pipeline

A new service or library can introduce metrics nobody has classified yet. Names, descriptions, units, instrumentation scope, and attribute keys provide evidence about what those metrics mean—even before there is a history of dashboard or query usage.

`jevmetrics` uses that evidence to ask Jev whether a metric is likely useful enough to retain. Explicit protection rules and thresholds determine what the Collector actually does with the answer.

The defining feature is **model inference as a runtime step in telemetry retention policy**. The connector constructs typed questions, sends them to Jev, validates the answers, and turns one of those probabilities into a keep/drop decision.

This complements existing approaches:

| Approach | Decision input |
| --- | --- |
| [OTel filter processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/filterprocessor) | Explicit conditions written by an operator |
| [Grafana Adaptive Metrics](https://grafana.com/docs/learning-journeys/adaptive-metrics/how-it-works/) | Usage patterns and cardinality informing aggregation recommendations |
| `jevmetrics` | Jev inference over metric metadata, followed by explicit Collector policy |

The project explores a specific combination: semantic metric assessment through Jev, execution within an OTel Collector connector, and retention decisions before export to a chosen backend.

## How inference works

```text
OTLP metrics
    |
    v
jevmetrics connector ---- metric metadata ----> Jev API
    |                                             |
    |<---------- validated, cached answers -------+
    |
    +-- annotate: preserve metrics + emit assessments
    |
    +-- route/reduce: apply protection rules + keep-score policy
    |
    v
configured Collector exporter
```

For an unprotected metric without a fresh cached assessment, the connector:

1. Summarizes its metadata and the attribute sets observed in the current batch.
2. Keeps the incoming metric and queues an asynchronous inference request.
3. Calls `POST https://api.typesafe.ai/v1/systemone`, using `jev-latest` by default.
4. Validates and caches the typed answers.
5. Uses the assessment for subsequent batches until it expires or is evicted.

Each request contains four questions:

| Question | Jev primitive | Meaning | Used for filtering? |
| --- | --- | --- | --- |
| `relevance` | `noul` | Probability the metric carries durable operational value | No |
| `redundancy` | `noul` | Probability the metric is likely redundant or noisy | No |
| `keep` | `noul` | Probability the metric should remain in primary storage | **Yes** |
| `action` | `choice` | Recommendation: `keep`, `reduce`, or `drop` | No |

A `noul` expresses the probability of a yes/no judgment. The recommended action and the other probabilities are exposed for review; they do not override the keep-score policy. See the [TypeSafe API reference](https://docs.typesafe.ai/api) for the primitive definitions.

Inference happens remotely in Jev. The Collector handles metadata extraction, scheduling, caching, and policy execution locally. No model training takes place in the connector.

## Quickstart

Requirements:

- Go **1.26.0 or newer** for the connector and Collector build.
- Bash and Make for the build script.
- A TypeSafe API key and network access to the Jev endpoint.
- OpenTelemetry Collector Builder (OCB). The manifest pins Collector components to `v0.161.0` / `v1.67.0`.

Run from the repository root:

```bash
# Install the builder matching the pinned Collector release.
go install go.opentelemetry.io/collector/cmd/builder@v0.161.0

# Build the Collector core and launcher.
OCB_BIN="$(go env GOPATH)/bin/builder" make collector

export JEV_API_KEY='your-key'

./_build/otelcol-jevmetrics --version
./_build/otelcol-jevmetrics validate --config examples/otelcol/config.yaml
./_build/otelcol-jevmetrics --config examples/otelcol/config.yaml
```

The example receives OTLP/gRPC on port `4317` and OTLP/HTTP on port `4318`, and sends annotated metrics to the debug exporter. It also has independent logs and traces pipelines; the connector only processes metrics.

In another terminal, send a sample metric:

```bash
curl --fail-with-body http://localhost:4318/v1/metrics \
  -H 'Content-Type: application/json' \
  --data-binary @- <<EOF_METRIC
{
  "resourceMetrics": [{
    "resource": {
      "attributes": [{"key": "service.name", "value": {"stringValue": "demo"}}]
    },
    "scopeMetrics": [{
      "scope": {"name": "jevmetrics.quickstart"},
      "metrics": [{
        "name": "demo.work_queue.depth",
        "description": "Number of jobs waiting for a worker.",
        "unit": "{job}",
        "gauge": {
          "dataPoints": [{"asInt": "12", "timeUnixNano": "$(date +%s)000000000"}]
        }
      }]
    }]
  }]
}
EOF_METRIC
```

The original metric should appear in the Collector output. After successful inference, look for the `Jev metric score received` log and the four `jev.metric.*` companion metrics below. The batch processor may delay debug output. Scores are model judgments, so the quickstart does not prescribe numeric results.

If only the original metric appears, check for scoring errors, a valid API key, and access to the configured endpoint. A successful OTLP request confirms ingestion, not successful inference.

Keep both `_build/otelcol-jevmetrics` and `_build/otelcol-jevmetrics-core` together when moving the build. The launcher delegates Collector commands to the adjacent core binary. `VERSION` supplies the launcher version; `components` lists the compiled components.

## Modes

| Mode | Output | Example |
| --- | --- | --- |
| `annotate` (default) | Original metrics plus companion assessments | [config.yaml](examples/otelcol/config.yaml) |
| `route` | Metrics retained by policy; configure a separate raw archive branch | [config-route.yaml](examples/otelcol/config-route.yaml) |
| `reduce` | Metrics retained by policy, with no archive branch in the example | [config-reduce.yaml](examples/otelcol/config-reduce.yaml) |

`route` and `reduce` execute the same filtering logic. The archive fan-out is part of the Collector pipeline configuration; the connector does not create it automatically.

Filtering removes an entire metric and its datapoints for the assessed identity. **There is no downsampling, dimension reduction, or automatic aggregation.** A Jev recommendation of `reduce` is advisory and does not implement those operations.

In `annotate`, successful inference emits these gauges immediately through the downstream pipeline and includes them again with subsequent batches that use a cached score:

| Metric | Value |
| --- | --- |
| `jev.metric.relevance` | Probability in `[0, 1]` |
| `jev.metric.redundancy` | Probability in `[0, 1]` |
| `jev.metric.keep_probability` | Probability in `[0, 1]` |
| `jev.metric.recommended_action` | `1`, with an `action` attribute |

Companion datapoints carry `metric.name` and `jev.model`, under the original resource and instrumentation scope. Original datapoints are not modified. Annotation adds telemetry volume. Protected metrics bypass inference and do not receive companion assessments.

For a local Prometheus endpoint on port `9464`, use [config-local.yaml](examples/otelcol/config-local.yaml).

## Configuration and policy

The connector appears as an exporter in the input metrics pipeline and as a receiver in the output metrics pipeline. Complete working configurations are in [examples/otelcol](examples/otelcol).

```yaml
connectors:
  jevmetrics/annotate:
    api_key: ${env:JEV_API_KEY}
    mode: annotate
    base_url: https://api.typesafe.ai
    model: jev-latest
    timeout: 3s
    queue_size: 256
    workers: 2
    cache_size: 10000
    policy:
      score_ttl: 15m
      keep_threshold: 0.70
      drop_threshold: 0.30
      protected_prefixes: [otelcol_, up, scrape_, target_]
      protected_metrics:
        - http.server.request.duration
    context_attributes:
      - service.name
      - service.namespace
      - deployment.environment.name
      - cloud.region
      - k8s.namespace.name
```

All shown values are defaults except `api_key`, which is required, and `protected_metrics`, which is empty by default. The protected metric above is an example; explicitly list the metrics needed by your alerts and SLOs.

| Setting | Behavior |
| --- | --- |
| `mode` | `annotate`, `route`, or `reduce`; case-insensitive |
| `base_url`, `model` | Jev endpoint base URL and model identifier |
| `timeout` | Positive HTTP request timeout |
| `queue_size`, `workers` | Positive queue capacity and worker count |
| `cache_size` | Positive maximum number of cached assessments |
| `policy.score_ttl` | Positive lifetime of an assessment |
| `policy.protected_metrics` | Exact names that bypass inference and filtering |
| `policy.protected_prefixes` | Literal prefixes that bypass inference and filtering |
| `context_attributes` | Resource attributes included in external requests |

In `route` and `reduce`, the default thresholds mean:

```text
keep probability >= 0.70  -> keep
keep probability <= 0.30  -> drop
otherwise                -> keep
```

Thresholds must be in `[0, 1]`, with `drop_threshold <= keep_threshold`. If both thresholds are equal, a score at that boundary is kept. `annotate` preserves input regardless of scores.

## Fail-open behavior and caching

Unscored metrics are retained. This includes first-seen metrics, expired or evicted assessments, a full scoring queue, and unsuccessful inference attempts. Invalid answer types, missing probabilities, out-of-range probabilities, and unsupported actions are treated as failures.

After a scoring failure, the connector pauses new inference requests for one second, doubling after successive failures up to 32 seconds. Already-running requests may complete; a successful response resets the cooldown. Future incoming batches trigger retries after the cooldown.

Fresh cached decisions remain effective during an API outage, including decisions to drop metrics. When those scores expire, the metrics are kept until a fresh assessment arrives. Fail-open behavior concerns the inference path; it does not guarantee delivery through an unavailable downstream exporter.

The in-memory cache uses least-recently-used eviction at capacity. Expired scores are removed on lookup and during periodic cleanup. Cache identity includes the complete resource attributes, instrumentation scope name/version/attributes, schema URLs, and instrument name, description, unit, type, temporality, and monotonicity where applicable. Restarting the Collector clears its assessments.

Queue and cache limits bound entry counts, not metadata bytes or input batch size. Attribute keys and observed series counts are refreshed when rescoring occurs; changing those alone does not invalidate a fresh cached assessment.

## Data sent to Jev

The scoring state contains:

- Metric name, description, unit, and type.
- Instrumentation scope name and version.
- Temporality and sum monotonicity, where applicable.
- Datapoint count, distinct attribute-set count in the observed batch, and datapoint attribute keys.
- Resource values explicitly allowed by `context_attributes`.

Raw metric values, histogram buckets, exemplars, and datapoint attribute values are not included in the inference request. Datapoint attribute values are examined locally to estimate distinct series. Metric metadata and allowed resource values can still contain sensitive information; choose the allow-list accordingly.

The original resource is preserved in exported telemetry and companion metrics. The allow-list controls what goes to Jev, not what goes to your telemetry backend.

Gauge, Sum, Histogram, Exponential Histogram, and Summary are supported. Retained metrics keep their original payloads. See [SEMCONV.md](SEMCONV.md) for data-model details.

## Current limits and evaluation

Jev evaluates one metric summary at a time. It does not receive historical values, other metric inventories, query history, dashboard dependencies, alert rules, or SLO definitions. Redundancy is therefore a semantic estimate, not a measured correlation with another signal. A keep probability is not evidence that dropping a metric is operationally safe.

The project does not yet provide:

- Published measurements of assessment accuracy, signal loss, or net cost savings.
- Automatic discovery of metrics required by dashboards and alerts.
- Cross-metric comparison, time-series analysis, or downsampling.
- Dedicated inference spans or request latency, token-usage, and cost metrics.

Evaluate in `annotate` first: compare assessments with engineer-reviewed decisions, protect critical metrics, and measure inference overhead and added telemetry. Before enabling filtering, verify the prospective savings and missed-signal rate on representative workloads. An archive branch can preserve the full stream while you evaluate the filtered primary stream.

## Development

```bash
# Run both Go modules' tests.
make test

# Run the connector suite with race detection and coverage.
(cd otelconnector && go test -race -cover ./...)

# Build the Collector distribution.
make collector
```

The connector is implemented in [otelconnector](otelconnector). The [OCB manifest](examples/otelcol/builder-config.yaml) includes OTLP reception/export, debug and Prometheus exporters, memory limiting, batching, and the connector. It uses a local module replacement for development.

Tests cover response validation, mode normalization, retention policy, payload preservation across metric types, queue saturation, cache identity/expiry/eviction, and failure recovery. They use a mock HTTP transport; live Jev inference and production assessment quality require separate validation. See [BUILD_NOTES.md](BUILD_NOTES.md) for recorded verification.

This repository also contains an earlier standalone Prometheus polling experiment in [cmd/jevmetrics](cmd/jevmetrics). It assesses service anomalies and impact using a separate evaluator. The root `Dockerfile`, `make build`, and `make run` target that application; they do not build or run the OTel connector. The Collector path described above is the primary project direction.

## Contributing and CI

See [CONTRIBUTING.md](CONTRIBUTING.md) for development checks and pull request
guidance, and [SECURITY.md](SECURITY.md) for vulnerability reporting.

[GitHub Actions](.github/workflows/ci.yml) runs formatting, vet, and race tests for
both modules, then builds the Collector and validates all example configurations.
CI uses mock inference responses and a placeholder key; no Jev credentials are
required. Run `make check` locally and `make collector-validate` after a Collector
build. Dependabot checks GitHub Actions and connector dependencies weekly.

## License

Apache-2.0. See [LICENSE](LICENSE).
