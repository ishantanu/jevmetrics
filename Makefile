.PHONY: test test-race vet fmt fmt-check check build run tidy connector-test collector collector-validate collector-version clean

tidy:
	go mod tidy
	cd otelconnector && go mod tidy

test:
	go test -mod=readonly ./...
	cd otelconnector && go test -mod=readonly ./...

test-race:
	go test -mod=readonly -race -cover ./...
	cd otelconnector && go test -mod=readonly -race -cover ./...

vet:
	go vet -mod=readonly ./...
	cd otelconnector && go vet -mod=readonly ./...

fmt:
	gofmt -w cmd internal otelconnector

fmt-check:
	@unformatted="$$(gofmt -l cmd internal otelconnector)"; \
	if [ -n "$$unformatted" ]; then \
		printf 'Run make fmt for these files:\n%s\n' "$$unformatted"; exit 1; \
	fi

check: fmt-check vet test-race

build:
	go build -mod=readonly ./cmd/jevmetrics

run:
	JEVMETRICS_CONFIG=config.example.json go run ./cmd/jevmetrics

connector-test:
	cd otelconnector && go test -mod=readonly ./...

collector:
	./scripts/build-collector.sh

collector-validate:
	@for config in examples/otelcol/config*.yaml; do \
		echo "Validating $$config"; \
		JEV_API_KEY=validation-placeholder ./_build/otelcol-jevmetrics validate --config "$$config" || exit 1; \
	done

collector-version:
	./_build/otelcol-jevmetrics --version

clean:
	rm -rf _build
