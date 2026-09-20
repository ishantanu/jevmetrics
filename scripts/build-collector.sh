#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/_build"
BUILDER_CONFIG="${ROOT_DIR}/examples/otelcol/builder-config.yaml"
VERSION_FILE="${ROOT_DIR}/VERSION"
WRAPPER_DIR="${ROOT_DIR}/cmd/otelcol-jevmetrics-wrapper"

if [[ ! -f "${BUILDER_CONFIG}" ]]; then
  echo "Builder config not found:"
  echo "  ${BUILDER_CONFIG}"
  exit 1
fi

if [[ ! -f "${VERSION_FILE}" ]]; then
  echo "VERSION file not found:"
  echo "  ${VERSION_FILE}"
  exit 1
fi

VERSION="$(tr -d '[:space:]' < "${VERSION_FILE}")"

if [[ -z "${VERSION}" ]]; then
  echo "VERSION file is empty."
  exit 1
fi

if [[ -n "${OCB_BIN:-}" ]]; then
  if ! command -v "${OCB_BIN}" >/dev/null 2>&1; then
    echo "Configured OCB_BIN is not executable: ${OCB_BIN}" >&2
    exit 1
  fi
elif command -v ocb >/dev/null 2>&1; then
  OCB_BIN="$(command -v ocb)"
elif command -v builder >/dev/null 2>&1; then
  OCB_BIN="$(command -v builder)"
else
  echo "OpenTelemetry Collector Builder not found."
  echo
  echo "Expected either:"
  echo "  ocb"
  echo "or:"
  echo "  builder"
  echo
  echo "If installed with:"
  echo "  go install go.opentelemetry.io/collector/cmd/builder@v0.161.0"
  echo
  echo "the executable is typically named 'builder'."
  exit 1
fi

echo "Building jevmetrics OpenTelemetry Collector"
echo "  version: ${VERSION}"
echo "  builder: ${OCB_BIN}"
echo "  config:  ${BUILDER_CONFIG}"
echo "  output:  ${BUILD_DIR}"
echo

cd "${ROOT_DIR}"

echo "Preparing processor Go module..."
(
  cd otelprocessor
  go test -mod=readonly ./...
)

echo
echo "Removing previous build..."
rm -rf "${BUILD_DIR}"

echo
echo "Building Collector with OCB..."
"${OCB_BIN}" --config "${BUILDER_CONFIG}"

CORE_BINARY="${BUILD_DIR}/otelcol-jevmetrics-core"

if [[ ! -x "${CORE_BINARY}" ]]; then
  echo
  echo "Expected Collector core binary was not produced:"
  echo "  ${CORE_BINARY}"
  echo
  echo "Check examples/otelcol/builder-config.yaml and ensure:"
  echo "  dist.name: otelcol-jevmetrics-core"
  echo "  dist.output_path: ./_build"
  exit 1
fi

echo
echo "Building user-facing jevmetrics launcher..."

mkdir -p "${BUILD_DIR}"

go build \
  -trimpath \
  -ldflags "-s -w -X main.version=${VERSION}" \
  -o "${BUILD_DIR}/otelcol-jevmetrics" \
  "${WRAPPER_DIR}"

if [[ ! -x "${BUILD_DIR}/otelcol-jevmetrics" ]]; then
  echo "Failed to create launcher:"
  echo "  ${BUILD_DIR}/otelcol-jevmetrics"
  exit 1
fi

echo
echo "Build complete."
echo
echo "Binaries:"
echo "  ${BUILD_DIR}/otelcol-jevmetrics"
echo "  ${BUILD_DIR}/otelcol-jevmetrics-core"
echo
echo "Verify:"
echo "  ./_build/otelcol-jevmetrics --version"
echo "  ./_build/otelcol-jevmetrics components"
echo
echo "Run:"
echo "  export JEV_API_KEY='your-key'"
echo "  ./_build/otelcol-jevmetrics --config examples/otelcol/config.yaml"
