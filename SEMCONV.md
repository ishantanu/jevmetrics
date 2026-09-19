# OpenTelemetry compatibility

`jevmetrics` operates on the OpenTelemetry metrics data model rather than hard-coding one semantic-convention domain.

## Supported metric data types

The scorer understands metadata for all current OTel metric data shapes used by pdata:

- Gauge
- Sum
- Histogram
- Exponential Histogram
- Summary

For sums and histograms it includes aggregation temporality when available, and for sums it includes monotonicity.

## Semantic conventions

Semantic-convention names and attributes are preserved because retained telemetry is forwarded as normal OTel metrics. The scorer also uses metric name, description, unit, instrumentation scope, resource context, and observed attribute keys when assessing storage value.

That means the same component can assess metrics from domains such as:

- HTTP
- RPC
- database
- messaging
- process/runtime
- host/system
- Kubernetes
- cloud
- custom application metrics

without requiring a dedicated feature extractor for every domain.

## Resource attributes sent to Jev

Only configured resource attributes are included in external scoring requests. The default allow-list is intentionally small:

```yaml
context_attributes:
  - service.name
  - service.namespace
  - deployment.environment.name
  - cloud.region
  - k8s.namespace.name
```

Datapoint attribute **keys** are included to help Jev reason about likely cardinality and semantics. Datapoint attribute values are not sent by the current scorer.

## Cardinality behaviour

`jevmetrics` estimates the number of distinct attribute sets observed for each instrument in the current Collector batch. This is a bounded local signal for deciding whether an instrument is likely expensive/noisy.

`annotate` mode emits separate companion score metrics instead of putting changing score values onto every original datapoint.

## Preservation

For metrics that are retained, the original pdata metric is copied through unchanged. The connector does not rename OTel semantic-convention metrics or rewrite their attributes.
