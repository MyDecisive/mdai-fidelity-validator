#!/usr/bin/env bash

set -euo pipefail

VALIDATOR_ADMIN_URL="${VALIDATOR_ADMIN_URL:-http://localhost:8080}"
VALIDATOR_METRICS_URL="${VALIDATOR_METRICS_URL:-http://localhost:8888}"
VALIDATOR_INGRESS_URL="${VALIDATOR_INGRESS_URL:-http://localhost:8081}"
DDMLTGEN_BIN="${DDMLTGEN_BIN:-go run ./cmd/ddmltgen}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-20}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-1}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need curl
need sed
need awk
need grep

run_signal() {
  local signal="$1"
  local output
  local correlation_id
  local result_path
  local deadline

  echo "== signal: ${signal}"
  output="$(eval "$DDMLTGEN_BIN --endpoint '$VALIDATOR_INGRESS_URL' --signal '$signal' --encoding json --count 1")"
  echo "$output"

  correlation_id="$(printf '%s\n' "$output" | sed -n 's/.*correlation_id=\([^ ]*\).*/\1/p' | head -n1)"
  if [[ -z "$correlation_id" ]]; then
    echo "failed to extract correlation_id from ddmltgen output" >&2
    exit 1
  fi

  result_path="${VALIDATOR_ADMIN_URL}/results/${signal}:${correlation_id}"
  deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))

  while (( $(date +%s) <= deadline )); do
    if result_json="$(curl -fsS "$result_path" 2>/dev/null)"; then
      echo "$result_json"
      if printf '%s' "$result_json" | grep -q '"passed":true'; then
        echo "result=${signal} pass"
      else
        echo "result=${signal} fail"
      fi
      return 0
    fi

    sleep "$POLL_INTERVAL_SECONDS"
  done

  echo "timed out waiting for ${signal} comparison at ${result_path}" >&2
  echo "current metrics snapshot:" >&2
  curl -fsS "${VALIDATOR_METRICS_URL}/metrics" | grep 'mdai_fidelity' >&2 || true
  return 1
}

echo "smoke test starting against ingress=${VALIDATOR_INGRESS_URL} admin=${VALIDATOR_ADMIN_URL} metrics=${VALIDATOR_METRICS_URL}"
run_signal traces
run_signal metrics
run_signal logs

echo "== prometheus metrics"
curl -fsS "${VALIDATOR_METRICS_URL}/metrics" | grep 'mdai_fidelity'
