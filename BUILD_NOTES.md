# Build notes

Validation on 2026-09-20 with Go 1.26.5:

- `make check`: formatting, vet, and race tests passed for both modules.
- `make collector` and `make collector-validate`: full build and all five example configurations passed, including coordinated mode.
- GitHub Actions workflow passed actionlint v1.7.7; hosted CI has not run yet.
- Both Go modules passed `go mod tidy -diff`.
- Root standalone module: `go test ./...` passed.
- Processor module: race tests passed with 85.9% statement coverage, including
  the real-Redis integration test against a temporary Redis 7.4 container.
- Regression tests cover malformed inference answers, mode normalization, policy,
  payload preservation, queue saturation, cache identity/expiry/eviction, and
  inference failure cooldown/recovery. Cluster tests cover shared assessments
  across two replicas, a replacement replica, stale-owner fencing, shared
  budgets/cooldown, version separation, publication failure, expiry, Redis
  outages, cancellation, and companion metric writer identity.

The processor module requires Go 1.26.0 or newer and pins Collector processor
v0.161.0 and component/consumer/pdata v1.67.0. Root-module tests alone do not
include the nested processor module; `make test` runs both.

Tests use a mock HTTP transport. Passing them does not establish the accuracy
of Jev assessments on production telemetry or validate live API credentials.
