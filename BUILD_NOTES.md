# Build notes

Validation on 2026-09-19 with Go 1.26.5:

- `make check`: formatting, vet, and race tests passed for both modules.
- `make collector` and `make collector-validate`: full build and all four example configurations passed.
- GitHub Actions workflow passed actionlint v1.7.7; hosted CI has not run yet.
- Both Go modules passed `go mod tidy -diff`.
- Root standalone module: `go test ./...` passed.
- Connector module: `go test -race -cover ./...` passed (86.5% statement coverage).
- Regression tests cover malformed inference answers, mode normalization, policy,
  payload preservation, queue saturation, cache identity/expiry/eviction, and
  inference failure cooldown/recovery.

The connector module requires Go 1.26.0 or newer and pins Collector connector
v0.161.0 and component/consumer/pdata v1.67.0. Root-module tests alone do not
include the nested connector module; `make test` runs both.

Tests use a mock HTTP transport. Passing them does not establish the accuracy
of Jev assessments on production telemetry or validate live API credentials.
