# syntax=docker/dockerfile:1
FROM golang:1.26.5-bookworm AS build
WORKDIR /src
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.161.0
COPY otelprocessor/ ./otelprocessor/
COPY examples/otelcol/builder-config.yaml ./examples/otelcol/builder-config.yaml
ENV CGO_ENABLED=0
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    builder --config examples/otelcol/builder-config.yaml

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/_build/otelcol-jevmetrics-core /otelcol-jevmetrics
COPY examples/otelcol/config.yaml /etc/otelcol/config.yaml
EXPOSE 4317 4318
ENTRYPOINT ["/otelcol-jevmetrics"]
CMD ["--config", "/etc/otelcol/config.yaml"]
