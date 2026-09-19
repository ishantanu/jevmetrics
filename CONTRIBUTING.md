# Contributing

The primary project is the experimental OpenTelemetry connector in `otelconnector/`.
The root module also contains an older standalone Prometheus application. Changes
should make clear which application they affect.

## Local checks

Use Go 1.26.0 or newer, Bash, and Make. From the repository root:

```bash
make check
```

This checks formatting, runs `go vet`, and runs tests with race detection for both
Go modules. Tests use mock inference responses and do not need a Jev API key.
Run `make fmt` to format Go files and `make tidy` after dependency changes.

For Collector integration changes:

```bash
go install go.opentelemetry.io/collector/cmd/builder@v0.161.0
OCB_BIN="$(go env GOPATH)/bin/builder" make collector
make collector-validate
```

Validation uses a placeholder key and does not call Jev. Building regenerates
`_build/`, which is ignored by Git. Keep the Collector versions in the OCB
manifest and connector module compatible when updating dependencies. Dependabot
updates to Go modules do not update the OCB manifest or builder pin automatically.

## Pull requests

Explain the problem, resulting behavior, and how you verified the change. Add
regression tests for changes to retention decisions, inference response parsing,
cache identity, retries, or telemetry preservation. Keep unrelated changes in
separate pull requests.

Do not include API keys, private telemetry, or local credentials. Use synthetic
fixtures. Distinguish measured model performance from expected benefits; mock
tests verify code behavior, not assessment accuracy.

Contributions are made under the repository's Apache-2.0 license. The nested
`otelconnector` module carries a copy of the same license for module distribution.
