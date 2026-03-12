# mdai-dd-fidelity-validator

`mdai-dd-fidelity-validator` is a small Go service that sits inline on both sides of a Datadog pipeline and checks whether attributes are preserved from initial ingress to final egress.

## What it does

- Exposes an admin API on `:8080`, a receiver-side raw Datadog proxy on `:8081`, and an exporter-side raw Datadog sink/proxy on `:8082`.
- Captures raw Datadog requests before the collector's Datadog receiver and again when the collector's Datadog exporter emits its final wire-format payload.
- Decodes JSON, gzipped JSON, and MessagePack request bodies so it can inspect raw Datadog-style payloads from a receiver proxy and a Datadog exporter.
- Flattens each decoded payload into `attribute.path -> value` form so every incoming field is explicitly denoted in the response.
- Correlates payload pairs using `X-Correlation-ID` when present, otherwise preferring `correlation_id` and `fidelity.correlation_id` across nested Datadog-style paths before falling back to a derived fingerprint.
- Compares all flattened attributes and exports Prometheus metrics for both per-attribute and per-signal pass/fail results.

## Prometheus metrics

- `mdai_dd_fidelity_payloads_received_total{source,signal}`
- `mdai_dd_fidelity_attribute_checks_total{signal,attribute,result}`
- `mdai_dd_fidelity_signal_checks_total{signal,result}`
- `mdai_dd_fidelity_required_attribute_checks_total{signal,attribute,result}`
- `mdai_dd_fidelity_required_signal_checks_total{signal,result}`
- `mdai_dd_fidelity_pending_payloads`

## Run locally

```bash
go run ./cmd/mdai-dd-fidelity-validator
```

Generate raw Datadog traffic against the ingress proxy:

```bash
go run ./cmd/ddmltgen --endpoint http://localhost:8081 --signal random --encoding random --count 10
```

Endpoints:

- Raw Datadog receiver-side capture: any Datadog intake path on `:8081`
- Raw Datadog exporter-side capture: any Datadog intake path on `:8082`
- `GET /results/{correlation_id}`
- `GET /metrics`

The included `telemetrygen` script still targets OTLP on the collector. That is useful for generating traffic through processors and into the Datadog exporter, but it does not validate raw Datadog ingress fidelity because it bypasses the receiver-side Datadog proxy.

For a fully local demo with no Datadog network calls, the Kubernetes manifest runs the validator and collector in the same pod. The validator also emulates the Datadog API validation endpoint on `:8443`, and the collector resolves `api.datadoghq.local` to `127.0.0.1` with `fail_on_invalid_key: false` and `api.tls.insecure_skip_verify: true`.

## Example flow

Send a receiver-side raw Datadog-style observation to the ingress proxy:

```bash
curl -sS localhost:8081/v0.4/traces \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: demo-1' \
  -d '{"trace_id":"123","span_id":"abc","resource":{"service.name":"checkout"}}'
```

Send the exporter-side raw Datadog-style observation to the egress proxy:

```bash
curl -sS localhost:8082/v0.4/traces \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: demo-1' \
  -d '{"trace_id":"123","span_id":"abc","resource":{"service.name":"checkout"}}'
```

Inspect the comparison:

```bash
curl -sS localhost:8080/results/demo-1 | jq
```

## Demo assets

- Collector example: [examples/otel/collector.yaml](/Users/justin/work/mdai-dd-fidelity-validator/examples/otel/collector.yaml)
- Fidelity policy: [fidelity-policy.yaml](/Users/justin/work/mdai-dd-fidelity-validator/fidelity-policy.yaml)
- Random telemetry generator: [scripts/send-random-mlt.sh](/Users/justin/work/mdai-dd-fidelity-validator/scripts/send-random-mlt.sh)
- Raw Datadog MLT generator CLI: [cmd/ddmltgen/main.go](/Users/justin/work/mdai-dd-fidelity-validator/cmd/ddmltgen/main.go)
- Random raw Datadog generator script: [scripts/send-random-dd-mlt.sh](/Users/justin/work/mdai-dd-fidelity-validator/scripts/send-random-dd-mlt.sh)
- Kubernetes demo: [deploy/k8s/demo.yaml](/Users/justin/work/mdai-dd-fidelity-validator/deploy/k8s/demo.yaml)

## Important limitation

This validator is now shaped around raw Datadog payload capture on both sides of `dd receiver (proxy) -> processors -> dd exporter`, but it still compares normalized attribute/value presence after decoding rather than byte-for-byte equivalence. For reliable pairing across ingress and egress observations, inject a stable identifier such as `correlation_id` or `fidelity.correlation_id` before export.

Policy-based fidelity is driven by [fidelity-policy.yaml](/Users/justin/work/mdai-dd-fidelity-validator/fidelity-policy.yaml). `passed` in comparison results and `mdai_dd_fidelity_required_*` Prometheus metrics are based on those required attributes. `full_payload_passed` remains available for strict flattened payload debugging.

For the demo collector config, the Datadog exporter still validates API key syntax even if you only point it at the local validator proxy. Use a 32-character hex placeholder like `00000000000000000000000000000000` or a real key via `DD_API_KEY`.
