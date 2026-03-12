#!/usr/bin/env bash

set -euo pipefail

COLLECTOR_OTLP_ENDPOINT="${COLLECTOR_OTLP_ENDPOINT:-localhost:4317}"
TELEMETRYGEN_BIN="${TELEMETRYGEN_BIN:-telemetrygen}"
ITERATIONS="${ITERATIONS:-25}"
SLEEP_SECONDS="${SLEEP_SECONDS:-1}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

rand_token() {
  LC_ALL=C tr -dc 'a-z0-9' </dev/urandom | head -c "${1:-8}"
}

need "$TELEMETRYGEN_BIN"
need tr
need head
need sleep

for _ in $(seq 1 "$ITERATIONS"); do
  signal_selector=$(( $(od -An -N1 -tu1 /dev/urandom) % 3 ))
  service="fidelity-$(rand_token 6)"
  attr_value="$(rand_token 10)"
  common_args=(
    --otlp-insecure
    --otlp-endpoint "$COLLECTOR_OTLP_ENDPOINT"
    --resource-attributes "service.name=$service,fidelity.correlation_id=$service,fidelity.random=$attr_value"
  )

  case "$signal_selector" in
    0)
      "$TELEMETRYGEN_BIN" traces "${common_args[@]}" --traces 1 --duration 1s --workers 1
      ;;
    1)
      "$TELEMETRYGEN_BIN" metrics "${common_args[@]}" --metrics 1 --duration 1s --workers 1
      ;;
    2)
      "$TELEMETRYGEN_BIN" logs "${common_args[@]}" --logs 1 --duration 1s --workers 1
      ;;
  esac

  sleep "$SLEEP_SECONDS"
done
