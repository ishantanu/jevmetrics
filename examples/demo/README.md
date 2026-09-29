# Jevmetrics shadow demo

Requirements: Docker Engine/Desktop with Compose v2 or later. Run from the
repository root:

```bash
make demo-up
```

Open [Grafana](http://localhost:3000/d/jevmetrics-demo) or
[Prometheus](http://localhost:9090). No login or API key is needed. The first build
fetches Go dependencies and images; allow about 30 seconds after startup for rate
charts. The default uses deterministic **synthetic mock assessments**, not live
Jev inference or evidence of model accuracy.

The stack contains the actual custom Collector, a synthetic OTLP/HTTP generator,
a mock inference endpoint, Prometheus, and a provisioned Grafana dashboard.
Prometheus scrapes both Collector internal telemetry and the application-metrics
exporter. The Collector remains in `annotate` mode and preserves all original
input.

| Synthetic instrument | Datapoints per batch | Mock assessment |
| --- | ---: | --- |
| `demo_checkout_latency` | 2 | Keep |
| `demo_debug_payload_size` | 20 | Would drop |
| `demo_worker_queue_depth` | 3 | Uncertain; keep |
| `up` | 1 | Protected; inference bypassed |

Every two seconds, the generator sends 26 datapoints across four instruments.
Once assessments are cached, 20 of 26 datapoints (about 77%) are drop candidates;
**zero are actually dropped**. Cold starts and the demo's 30-second assessment
expiry temporarily lower the preview ratio. Counts are observed datapoint volume,
not unique series, storage savings, or business value.

The dashboard shows prospective reduction, actual drops, decision reasons, cache
hit ratio, inference attempts, queue rejections, assessment retrieval latency,
per-instrument probabilities/model labels, and preserved original datapoints.
No Redis is required. The processor uses a local limit of two admissions per
second with a burst of one. These settings are deliberately small demo values.

## Verify and stop

```bash
make demo-check
make demo-down
```

The check waits up to 150 seconds for ingestion and scrapes, verifies that all
26 original datapoints are still exported, checks shadow reasons, and executes
every dashboard expression through Grafana's provisioned Prometheus datasource.
It is intended for **mock mode**; real model judgments are not fixed fixtures.
CI runs this same check. `demo-down` removes only this Compose project's
containers and volumes. Demo data is disposable; this is not a persistence setup.

Only Grafana and Prometheus are published, bound to localhost. Anonymous Grafana
access is read-only. OTLP, the mock, and Collector scrape ports stay inside the
Compose network. This convenience configuration is for a local demo, not a
production deployment. Override conflicting UI ports with, for example:

```bash
DEMO_GRAFANA_PORT=3300 DEMO_PROMETHEUS_PORT=9091 make demo-up
```

## Explicit live-Jev opt-in

```bash
export JEV_API_KEY='your-key'
make demo-live
```

The override refuses to start without a key, selects `https://api.typesafe.ai`
and `jev-latest`, and sends only the synthetic demo metadata to the real API.
This consumes API usage. No live requests are made by the normal demo or CI.
The mock container may still run but is no longer used by the Collector. Actual
Jev scores and prospective reduction will vary; inspect the model labels on the
dashboard. Never commit credentials or share resolved Compose configuration that
contains them. Return to mock mode with `make demo-down && make demo-up`.

## Container image

The root `Dockerfile` now builds the OTel Collector with the pinned OCB manifest.
Its entrypoint runs the Collector directly so it receives shutdown signals.
The default config expects `JEV_API_KEY`. Supply a mounted config for your own
pipeline. The older standalone Prometheus polling application is still buildable:

```bash
docker build -f Dockerfile.standalone -t jevmetrics-standalone .
```
