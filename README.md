# mdai-fidelity-validator

`mdai-fidelity-validator` is a Go service that checks whether telemetry attributes survive a receiver-to-exporter pipeline.

It captures Datadog-style payloads on both sides, decodes and maps them to canonical attributes, correlates matching observations, and exposes comparison results through an admin API and Prometheus metrics.

## Documentation

- [How the validator works](docs/architecture.md)
- [How metrics are derived and used](docs/metrics.md)
- [Documentation index](docs/README.md)
- [Structurizr C4 architecture](docs/architecture.dsl)
- [C4 system-context view](docs/architecture-c4.svg)
- [Mermaid processing flow](docs/architecture.mmd)
- [Mermaid request sequence](docs/request-sequence.mmd)
- [Mermaid metric derivation](docs/metric-derivation.mmd)

## Run locally

The validator needs field-mapping and policy configuration to perform meaningful comparisons:

```bash
MDAI_FIDELITY_FIELD_MAPPING_PATH=internal/validator/field-mapping.yaml \
MDAI_FIDELITY_RULES_PATH=internal/validator/fidelity-policy.yaml \
go run ./cmd/mdai-fidelity-validator
```

Default listeners:

| Address | Purpose |
|---|---|
| `:8080` | Admin, results, diagnostics, and synthetic observations |
| `:8888` | Prometheus metrics |
| `:8126` | Receiver-side Datadog ingest |
| `:18081` | Exporter-side Datadog ingest and API validation |

## Try a comparison

Send the same correlated observation to both sides:

```bash
curl -sS localhost:8126/v0.4/traces \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: demo-1' \
  -d '{"trace_id":"123","span_id":"abc","service":"checkout"}'

curl -sS localhost:18081/exporter/datadog/api/v0.2/traces \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: demo-1' \
  -d '{"trace_id":"123","span_id":"abc","service":"checkout"}'
```

Inspect the result and metrics:

```bash
curl -sS localhost:8080/results/traces:123 | jq
curl -sS localhost:8888/metrics | grep mdai_fidelity
```

For more varied traffic, use the included generator:

```bash
go run ./cmd/ddmltgen \
  --endpoint http://localhost:8126 \
  --mirror-exporter-endpoint http://localhost:18081/exporter/datadog \
  --signal random \
  --encoding random \
  --count 10
```

## Deploy with Helm

Render the chart:

```bash
helm template mdai-fidelity-validator ./deployment -n mdai
```

Install it:

```bash
helm upgrade --install mdai-fidelity-validator ./deployment \
  -n mdai \
  --create-namespace
```

Enable Prometheus Operator discovery when needed:

```bash
helm upgrade --install mdai-fidelity-validator ./deployment \
  -n mdai \
  --create-namespace \
  --set serviceMonitor.enabled=true
```

The chart mounts its packaged
[field mapping](deployment/files/field-mapping.yaml) and
[fidelity policy](deployment/files/fidelity-policy.yaml) through a ConfigMap. Review their canonical attribute names together before relying on policy metrics; the current alignment findings are documented in the [metrics guide](docs/metrics.md#configuration-alignment-the-main-correctness-dependency).

## Development

```bash
go test ./...
```

Key locations:

- `cmd/mdai-fidelity-validator/` — service entry point
- `cmd/ddmltgen/` and `internal/ddgen/` — synthetic traffic generator
- `internal/validator/` — decoding, mapping, correlation, comparison, and metrics
- `deployment/` — Helm chart and packaged configuration
- `scripts/` — smoke and traffic-generation helpers
