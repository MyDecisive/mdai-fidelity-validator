# mdai-fidelity-validator

`mdai-fidelity-validator` is a small Go service that captures receiver/exporter wire-format payloads and checks whether attributes are preserved from initial ingress to final egress.

## What it does

- Exposes an admin API on `:8080`, a Datadog receiver ingest endpoint on `:8126`, and an exporter/API endpoint on `:18081` (HTTP).
- Captures raw Datadog receiver-side requests on `:8126` and raw exporter-side requests on `:18081`.
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
- exporter/API endpoint on `:18081` (Service `18081 -> 18081`)
- optional `busybox` debug sidecar enabled by default

Generate raw Datadog receiver-side traffic against `:8126`:

```bash
go run ./cmd/ddmltgen --endpoint http://localhost:8126 --signal random --encoding random --count 10
```

Endpoints:

- Raw Datadog receiver-side ingest: any Datadog intake path on `:8126`
- Raw exporter-side ingest: `:18081/exporter/{exporter_name}/...` (for example `/exporter/datadog/api/v2/logs`)
- Datadog API validation endpoint: `:18081/api/v1/validate`
- `GET /results/{correlation_id}`
- `GET /metrics`

### Config-driven Field Mapping (No Plugin Needed)

`datadog_raw` now maps decoded payload fields to canonical attributes via config.

Environment variable:

- `MDAI_FIELD_MAPPING_PATH`: optional YAML path to override default mapping (`internal/validator/field-mapping.yaml`).
- `MDAI_FIDELITY_POLICY_PATH`: optional policy path override.
- `MDAI_CONFIG_RELOAD_INTERVAL`: optional hot-reload interval for both files (default `15s`).

Mapping format:

```yaml
signals:
  logs:
    message:
      - message
      - "[0].message|json:message"
      - "[0].message"
    correlation_id:
      - correlation_id
      - "[0].ddtags|tag:correlation_id"
exporters:
  datadog:
    signals:
      logs:
        message:
          - "[0].message|json:message"
```

When path is prefixed with `/exporter/{name}/...`, exporter-specific mappings are resolved from `exporters.{name}.signals` first, then fallback to top-level `signals`.

Supported source expressions:

- direct key: `"[0].message"`
- JSON extraction: `"[0].message|json:service"`
- tag extraction from comma-separated tags: `"[0].ddtags|tag:correlation_id"`
- suffix match: `"suffix:.trace_id"`
- contains match: `"contains:.tags[|tag:env"`

Current limitation:

- Protobuf Datadog payloads are not decoded in this mode.

Both files are hot-reloaded from disk, so ConfigMap volume updates are picked up without restarting the pod.

The included `telemetrygen` script still targets OTLP on the collector. That is useful for generating traffic through processors and into the Datadog exporter, but it does not validate raw Datadog ingress fidelity because it bypasses the receiver-side capture path.

For a fully local demo with no Datadog network calls, the Kubernetes manifest runs the validator by itself and exposes `:8126` as receiver ingest plus Service `18081 -> :18081` for exporter/API.

The demo pod also includes a tiny `busybox` sidecar named `debug` so you can verify the pod-local host aliases and listener reachability:

```bash
kubectl -n mdai-fidelity-demo exec deploy/mdai-fidelity-stack -c debug -- cat /etc/hosts
kubectl -n mdai-fidelity-demo exec deploy/mdai-fidelity-stack -c debug -- wget -qO- http://mdai-fidelity-validator.mdai.svc.cluster.local:18081/api/v1/validate
```

## Example flow

Send a receiver-side raw Datadog-style observation to `:8126`:

```bash
curl -sS localhost:8126/v0.4/traces \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: demo-1' \
  -d '{"trace_id":"123","span_id":"abc","resource":{"service.name":"checkout"}}'
```

Send an exporter-side raw Datadog-style observation to `:18081`:

```bash
curl -sS localhost:18081/exporter/datadog/api/v0.2/traces \
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
- Fidelity policy example: [internal/validator/fidelity-policy.yaml](/Users/justin/work/mdai-fidelity-validator/internal/validator/fidelity-policy.yaml)
- Random telemetry generator: [scripts/send-random-mlt.sh](scripts/send-random-mlt.sh)
- Raw Datadog MLT generator CLI: [cmd/ddmltgen/main.go](cmd/ddmltgen/main.go)

## Important limitation

This validator is now shaped around raw Datadog payload capture on both sides of a receiver/exporter boundary, but it still compares normalized attribute/value presence after decoding rather than byte-for-byte equivalence. For reliable pairing across ingress and egress observations, inject a stable identifier such as `correlation_id` or `fidelity.correlation_id` before export.

Policy-based fidelity is driven by the file set via `MDAI_FIDELITY_POLICY_PATH` (example: [internal/validator/fidelity-policy.yaml](/Users/justin/work/mdai-fidelity-validator/internal/validator/fidelity-policy.yaml)). `passed` in comparison results and `mdai_fidelity_required_*` Prometheus metrics are based on those required attributes. `full_payload_passed` remains available for strict flattened payload debugging.

For the demo collector config, the Datadog exporter still validates API key syntax even if you only point it at the local validator proxy. Use a 32-character hex placeholder like `00000000000000000000000000000000` or a real key via `DD_API_KEY`.
