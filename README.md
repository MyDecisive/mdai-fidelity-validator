# mdai-fidelity-validator

`mdai-fidelity-validator` is a small Go service that captures Datadog wire-format payloads and checks whether attributes are preserved from initial ingress to final egress.

## What it does

- Exposes an admin API on `:8080`, a Datadog receiver ingest endpoint on `:8126`, and an exporter/API endpoint on `:8081` (HTTP).
- Captures raw Datadog receiver-side requests on `:8126` and raw Datadog exporter-side requests on `:8081` Datadog signal endpoints.
- Decodes JSON, gzipped JSON, and MessagePack request bodies so it can inspect raw Datadog-style payloads from a receiver proxy and a Datadog exporter.
- Flattens each decoded payload into `attribute.path -> value` form so every incoming field is explicitly denoted in the response.
- Correlates payload pairs using `X-Correlation-ID` when present, otherwise preferring `correlation_id` and `fidelity.correlation_id` across nested Datadog-style paths before falling back to a derived fingerprint.
- Compares all flattened attributes and exports Prometheus metrics for both per-attribute and per-signal pass/fail results.

## Prometheus metrics

- `mdai_fidelity_payloads_received_total{source,signal}`
- `mdai_fidelity_attribute_checks_total{signal,attribute,result}`
- `mdai_fidelity_signal_checks_total{signal,result}`
- `mdai_fidelity_required_attribute_checks_total{signal,attribute,result}`
- `mdai_fidelity_required_signal_checks_total{signal,result}`
- `mdai_fidelity_pending_payloads`

## Run locally

```bash
go run ./cmd/mdai-fidelity-validator
```

## Deploy with Helm

Render manifests:

```bash
helm template mdai-fidelity-validator ./deployment -n mdai
```

Install or upgrade:

```bash
helm upgrade --install mdai-fidelity-validator ./deployment -n mdai --create-namespace
```

The chart defaults match the current listener model:

- admin on `:8080`
- receiver ingest on `:8126`
- exporter/API endpoint on `:8081` (Service `8081 -> 8081`)
- optional `busybox` debug sidecar enabled by default

Generate raw Datadog receiver-side traffic against `:8126`:

```bash
go run ./cmd/ddmltgen --endpoint http://localhost:8126 --signal random --encoding random --count 10
```

Endpoints:

- Raw Datadog receiver-side ingest: any Datadog intake path on `:8126`
- Raw Datadog exporter-side ingest: Datadog signal paths on `:8081`
- Datadog API validation endpoint: `:8081/api/v1/validate`
- `GET /results/{correlation_id}`
- `GET /metrics`

The included `telemetrygen` script still targets OTLP on the collector. That is useful for generating traffic through processors and into the Datadog exporter, but it does not validate raw Datadog ingress fidelity because it bypasses the receiver-side capture path.

For a fully local demo with no Datadog network calls, the Kubernetes manifest runs the validator by itself and exposes `:8126` as receiver ingest plus Service `8081 -> :8081` for exporter/API.

The demo pod also includes a tiny `busybox` sidecar named `debug` so you can verify the pod-local host aliases and listener reachability:

```bash
kubectl -n mdai-fidelity-demo exec deploy/mdai-fidelity-stack -c debug -- cat /etc/hosts
kubectl -n mdai-fidelity-demo exec deploy/mdai-fidelity-stack -c debug -- wget -qO- http://mdai-fidelity-validator.mdai.svc.cluster.local:8081/api/v1/validate
```

## Example flow

Send a receiver-side raw Datadog-style observation to `:8126`:

```bash
curl -sS localhost:8126/v0.4/traces \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: demo-1' \
  -d '{"trace_id":"123","span_id":"abc","resource":{"service.name":"checkout"}}'
```

Send an exporter-side raw Datadog-style observation to `:8081`:

```bash
curl -sS localhost:8081/api/v0.2/traces \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: demo-1' \
  -d '{"trace_id":"123","span_id":"abc","resource":{"service.name":"checkout"}}'
```

Inspect the comparison:

```bash
curl -sS localhost:8080/results/demo-1 | jq
```

## Demo assets

- Helm chart: `deployment/`
- Fidelity policy: [fidelity-policy.yaml](fidelity-policy.yaml)
- Random telemetry generator: [scripts/send-random-mlt.sh](scripts/send-random-mlt.sh)
- Raw Datadog MLT generator CLI: [cmd/ddmltgen/main.go](cmd/ddmltgen/main.go)

## Important limitation

This validator is now shaped around raw Datadog payload capture on both sides of a receiver/exporter boundary, but it still compares normalized attribute/value presence after decoding rather than byte-for-byte equivalence. For reliable pairing across ingress and egress observations, inject a stable identifier such as `correlation_id` or `fidelity.correlation_id` before export.

Policy-based fidelity is driven by [fidelity-policy.yaml](fidelity-policy.yaml). `passed` in comparison results and `mdai_fidelity_required_*` Prometheus metrics are based on those required attributes. `full_payload_passed` remains available for strict flattened payload debugging.

For the demo collector config, the Datadog exporter still validates API key syntax even if you only point it at the local validator proxy. Use a 32-character hex placeholder like `00000000000000000000000000000000` or a real key via `DD_API_KEY`.
