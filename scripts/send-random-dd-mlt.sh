#!/usr/bin/env bash

set -euo pipefail

ENDPOINT="${ENDPOINT:-http://localhost:8081}"
COUNT="${COUNT:-25}"
SLEEP_SECONDS="${SLEEP_SECONDS:-1}"

for _ in $(seq 1 "$COUNT"); do
  signal_selector=$(( $(od -An -N1 -tu1 /dev/urandom) % 3 ))
  encoding_selector=$(( $(od -An -N1 -tu1 /dev/urandom) % 2 ))
  gzip_selector=$(( $(od -An -N1 -tu1 /dev/urandom) % 2 ))

  case "$signal_selector" in
    0) signal="traces" ;;
    1) signal="metrics" ;;
    2) signal="logs" ;;
  esac

  if [[ "$signal" == "metrics" ]]; then
    encoding="json"
  else
    case "$encoding_selector" in
      0) encoding="json" ;;
      1) encoding="msgpack" ;;
    esac
  fi

  gzip_flag=()
  if [[ "$gzip_selector" -eq 1 ]]; then
    gzip_flag=(--gzip)
  fi

  go run ./cmd/ddmltgen \
    --endpoint "$ENDPOINT" \
    --signal "$signal" \
    --encoding "$encoding" \
    --count 1 \
    "${gzip_flag[@]}"

  sleep "$SLEEP_SECONDS"
done
