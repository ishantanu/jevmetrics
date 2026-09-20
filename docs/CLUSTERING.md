# Clustered Collectors

Redis is optional. The default processor maintains its own bounded in-memory
assessment cache and background inference workers. There are two deployment choices:

| Deployment | Assessment state | Inference budget |
| --- | --- | --- |
| Independent replicas, preferably with source affinity | Local to each replica | Per-instance workers and cooldown |
| Optional Redis coordination | Shared expiring assessments plus local caches | Shared admissions and cooldown per namespace |

## Without Redis: source ownership

Use the ordinary [processor configuration](../examples/otelcol/config.yaml) on
each replica, without a `coordination` section. Assign each source consistently
to one replica through your upstream routing, static source assignment, or
existing ingestion partitioning. No membership discovery or router is built into
Jevmetrics, and the example Collector does not ship a metric-affinity router.

The unit to keep together is the complete assessment identity: resource
attributes, instrumentation scope and its attributes, schema URLs, and instrument
metadata. Routing all metrics for a stable resource identity to one replica is
coarser but sufficient. Hashing individual datapoint label sets can split one
instrument's observations and is not equivalent. A trace-ID routing example
cannot simply be reused for metrics; verify your router's metrics support and
chosen routing keys before deploying it.

This follows the ownership principle used by [tail sampling](https://opentelemetry.io/docs/collector/scaling/):
state stays local and related input reaches one owner. Jevmetrics does not buffer
all observations before deciding or calculate global cardinality. Its summary
still describes the batch that triggered inference. Put an upstream batch stage
before Jevmetrics only if you intentionally want a different observation window.

With arbitrary load balancing, replicas still work independently, but may repeat
inference and disagree because their observed batches and assessment times differ.
Source affinity reduces this duplication; it does not make inference deterministic.
On restart or reassignment, a new owner's cold cache retains metrics until scoring
completes. Membership changes may temporarily leave two owners with different
cached decisions. No state handoff, exactly-once inference, or cluster-wide rate
limit is provided in local mode. Size the aggregate worker count for the API budget.

## Optional Redis coordination

Set `coordination.redis_url` only when shared assessments or a shared inference
admission budget are needed. See the complete [coordinated example](../examples/otelcol/config-cluster.yaml).
Redis does not replace ingestion ownership, deduplicate telemetry, or merge
observations across replicas.

```yaml
coordination:
  redis_url: ${env:JEV_REDIS_URL}
  namespace: jevmetrics-production
  revision: "1"
  timeout: 1s
  lease_ttl: 10s
  max_in_flight: 4
  requests_per_second: 10
```

All replicas in a coordination group must use the same Redis database, namespace,
revision, model, context allow-list, and retention policy. Use distinct namespaces
for tenants or independent API budgets. A namespace is a logical separation, not
an authentication boundary: configure Redis ACLs to restrict access.

## Request and decision lifecycle

1. The delivery goroutine checks the local cache. If it has no fresh score, it
   retains the metric and queues work. It performs no Redis or Jev network I/O.
2. A background worker looks for a shared assessment. A cache hit is validated
   and copied locally with its remaining lifetime, accounting conservatively
   for network latency. Replica wall clocks are not compared.
3. On a shared miss, an atomic Redis script admits a scoring owner only if no
   other owner holds the metric's lease and the namespace budget permits it.
4. That owner's observed batch supplies the metadata and shape summary for Jev.
   Other workers poll with jitter until a score appears or admission opens.
5. Publication checks the lease token before saving the assessment. An expired
   owner's result cannot overwrite a newer owner's result. Only successfully
   published assessments enter the owner's local cache.
6. Subsequent batches use the cached score and local retention policy. Redis
   expires the shared score; local caches never extend its remaining lifetime.

The first owner's batch is the evidence for the shared assessment's lifetime.
The processor does not merge observations from different replicas. Cardinality
is still a batch-local estimate, not a cluster-wide series count. The complete
resource/scope/instrument identity remains in the cache key, so distinct source
service instances are not automatically pooled into one assessment.

## Shared budgets and recovery

`max_in_flight` bounds active inference leases per namespace. Crashed owners'
slots become reusable after their leases expire. `requests_per_second` bounds
admissions in each Redis-clock second, including attempts that fail or are
cancelled; fixed-window boundaries can admit bursts. These settings limit API
starts, not token usage or monetary spend.

`lease_ttl` must exceed the Jev request `timeout` plus twice the Redis operation
`timeout`. There is no lease renewal: a bounded request either publishes while
it owns its lease or discards its result. Cancellation attempts bounded cleanup;
crashes rely on expiration. The remote API may continue a request after client
cancellation, so this cannot guarantee a hard bound on remote computation.

Jev failures establish a namespace-wide cooldown from 1 second up to 32 seconds,
doubling on consecutive failures. A successful publication resets it. Redis
failures also activate local backoff. The processor does not fall back to
independent inference when Redis is unavailable, avoiding uncoordinated API
bursts. Fresh local decisions remain in force during an outage; unscored or
expired metrics are retained.

Budget and lease settings must match within a namespace. Mismatches reject new
lookups with an error. To change those settings, quiesce all replicas for more
than twice the old lease TTL before restarting with the new settings, or use a
new namespace and account for both namespaces' budgets during overlap.

## Versions and rollouts

Shared assessment keys hash the actual question definitions, endpoint, model,
context allow-list, retention policy, and `revision`. Changes to these inputs
produce separate assessments. The `revision` is an operator-controlled string:
bump it when the behavior behind a mutable model alias changes or when you want
fresh assessments. Old entries expire naturally.

A new replica initially keeps metrics while it fetches existing assessments.
Replicas can briefly disagree during cold starts, expiry, partitions, or rolling
configuration changes. This is eventual assessment sharing, not an atomic
cluster-wide retention switch. Drain old configurations when policy consistency
is required; keep the raw archive branch during evaluation.

## Companion metric writers

In both local and coordinated modes, companion score datapoints include `jev.collector.id`, a
random ID created per processor instance. Original telemetry is unchanged.
This separates assessment writers across replicas and restarts; it adds series
proportional to participating replicas and their observed instruments. Select or
compare replica assessments rather than summing probability gauges. Do not
remove the writer attribute before exporting to a backend that requires distinct
time-series writers.

The processor does not deduplicate replicated input telemetry, coordinate
Prometheus scrapers, or provide delivery guarantees for downstream exporters.
Those remain responsibilities of the overall Collector topology.

## Redis operation

This implementation uses a single writable Redis primary endpoint with optional
TLS (`rediss://`) and ACL authentication in the URL. Native Redis Cluster and
Sentinel discovery are not implemented. A managed endpoint can handle primary
selection, but failover that loses recent writes can repeat inference or produce
temporarily divergent scores. Exactly-once inference is not guaranteed.

Use a dedicated database/service, bounded memory, and `maxmemory-policy noeviction`.
Evicting active leases or budget keys would undermine coordination. At capacity,
write errors make new assessments fail open. `cache_size` limits each process's
local cache only; Redis storage grows with distinct identities during the score
TTL and must be capacity-planned separately. Assessments, coordination keys, and
failure state have expirations. Redis stores hashed identities and numeric
assessments, not the raw metadata summary or API credentials.

Redis users need the commands used by the client's connection handshake and the
Lua scripts, including `EVALSHA`, `EVAL`, `GET`, `SET`, `DEL`, `EXISTS`, `PTTL`,
`PEXPIRE`, `TIME`, `HGET`, `HSET`, `ZADD`, `ZCARD`, `ZREM`, and
`ZREMRANGEBYSCORE`. Restrict the account to this processor's keys. Avoid logging
the credential-bearing Redis URL.

## Verification

`make check` includes multi-replica tests with a local Redis protocol test server.
CI also runs the processor-to-Redis integration test against Redis 7.4. To run
that integration locally against an existing test Redis server:

```bash
cd otelprocessor
JEVMETRICS_TEST_REDIS_ADDR=localhost:6379 go test -race -run TestRedisIntegration ./...
```

Tests use synthetic metadata and mocked Jev responses. They check shared scoring,
replacement replicas, lease fencing, expiry, request budgets, failure cooldown,
version separation, Redis outage behavior, and distinct annotation writers. They
do not establish distributed consensus, live Jev accuracy, or performance at
production scale.
