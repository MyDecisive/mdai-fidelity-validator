# mdai-fidelity-validator

`mdai-fidelity-validator` is a small Go service that captures receiver/exporter wire-format payloads and checks whether attributes are preserved from initial ingress to final egress.

## What it does

- Exposes an admin API on `:8080`, a Prometheus metrics endpoint on `:8888`, a Datadog receiver ingest endpoint on `:8126`, and an exporter/API endpoint on `:18081` (HTTP).
- Captures raw Datadog receiver-side requests on `:8126` and raw exporter-side requests on `:18081`.
- Decodes JSON, gzipped JSON, MessagePack, Datadog metric protobuf payloads on `/series`, and Datadog trace protobuf payloads on `/traces` so it can inspect raw Datadog-style payloads from a receiver proxy and a Datadog exporter.
- Flattens each decoded payload into `attribute.path -> value` form so every incoming field is explicitly denoted in the response.
- Correlates payload pairs using `X-Correlation-ID` when present. Trace payloads prefer `span_id`, then `trace_id`, then `correlation_id`; other signals prefer `correlation_id` across nested Datadog-style paths before falling back to a derived fingerprint.
- Compares all flattened attributes and exports Prometheus metrics for both per-attribute and per-signal pass/fail results.

## Prometheus metrics

The validator exposes two conceptually distinct comparison modes, each with its own set of metrics.

### Background: exhaustive vs. policy-based comparison

When a receiver payload and its correlated exporter payload arrive, the validator runs two independent comparisons:

**Exhaustive comparison** — every flattened attribute from both payloads is diffed. A key can be matched (same value on both sides), mismatched (different values), or missing on one side. This catches any drift between the receiver and exporter, regardless of whether you declared that attribute important.

**Policy-based comparison** — only the attributes listed in the fidelity policy (`MDAI_FIDELITY_RULES_PATH`) are checked. Each required attribute can be checked for value equality (default) or mere presence (`compare: presence_only`). This is the intentional contract: it answers "did the fields my pipeline is supposed to preserve actually make it through?"

The `passed` field in comparison results and the `mdai_fidelity_required_*` metrics reflect the policy-based result. The `full_payload_passed` field and `mdai_fidelity_signal_checks_total` / `mdai_fidelity_attribute_checks_total` reflect the exhaustive result.

### Common labels

| Label | Values | Meaning |
|---|---|---|
| `mdai_connection` | string | Identifies the validator instance, set via `MDAI_CONNECTION_NAME` env var (default: `"default"`) |
| `signal` | `traces`, `metrics`, `logs` | The telemetry signal type of the compared payload pair |
| `attribute` | string | The specific flattened attribute key being evaluated |
| `result` | `pass`, `fail` | Whether the check passed or failed |
| `source` | `receiver`, `exporter` | Which side of the pipeline sent the payload |

### Metrics

#### `mdai_fidelity_payloads_received_total`

**Type:** Counter  
**Labels:** `mdai_connection`, `source`, `signal`

Incremented for every payload the validator receives, before any pairing or comparison. Use this to confirm traffic is flowing from both sides of the pipeline.

---

#### `mdai_fidelity_pending_payloads`

**Type:** Gauge  
**Labels:** `mdai_connection`

Number of payloads that have arrived from one side (receiver or exporter) but whose correlated counterpart has not yet been seen. A persistently non-zero value indicates payloads are not being paired — either the other side isn't sending, correlation IDs don't match, or the retention window is too short.

---

#### `mdai_fidelity_signal_checks_total`

**Type:** Counter  
**Labels:** `mdai_connection`, `signal`, `result`

Incremented once per compared payload pair. Result is `pass` only if **every** flattened attribute matched between receiver and exporter (i.e., `full_payload_passed = true`). A single differing or missing field causes a `fail`. This is the strict exhaustive signal — useful for detecting any drift, including fields not covered by the policy.

---

#### `mdai_fidelity_attribute_checks_total`

**Type:** Counter  
**Labels:** `mdai_connection`, `signal`, `attribute`, `result`

Incremented once per attribute per compared pair, covering all attributes found across both payloads. Result is `pass` if the attribute was present on both sides with the same value, `fail` if it was mismatched or missing on one side. Use this to identify which specific attributes are drifting, even for attributes not in the policy.

---

#### `mdai_fidelity_required_signal_checks_total`

**Type:** Counter  
**Labels:** `mdai_connection`, `signal`, `result`

Incremented once per compared payload pair, but scoped to the fidelity policy. Result is `pass` only if every required attribute defined in the policy passed its check. This is the primary signal for alerting — if your policy captures the attributes your pipeline must preserve, this metric tells you whether those guarantees are holding.

---

#### `mdai_fidelity_required_attribute_checks_total`

**Type:** Counter  
**Labels:** `mdai_connection`, `signal`, `attribute`, `result`

Incremented once per required attribute per compared pair (only for attributes declared in the fidelity policy). Result is `pass` if the attribute passed its configured check (value equality or presence-only). Use this to identify which specific required attribute is failing when `mdai_fidelity_required_signal_checks_total` shows failures.

### Example queries

```promql
# Policy pass rate for logs over the last hour
rate(mdai_fidelity_required_signal_checks_total{signal="logs", result="pass"}[1h])
/
rate(mdai_fidelity_required_signal_checks_total{signal="logs"}[1h])

# Which required attributes are failing?
rate(mdai_fidelity_required_attribute_checks_total{result="fail"}[5m]) > 0

# Is traffic flowing from both sides?
rate(mdai_fidelity_payloads_received_total[5m])

# How many payloads are stuck waiting for a pair?
mdai_fidelity_pending_payloads
```

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

Enable Prometheus Operator scraping with a `ServiceMonitor`:

```bash
helm upgrade --install mdai-fidelity-validator ./deployment -n mdai \
  --create-namespace \
  --set serviceMonitor.enabled=true
```

If your Prometheus Operator selects `ServiceMonitor` objects by label, set those labels in `serviceMonitor.labels`.
`ServiceMonitor` is the better fit here than `PodMonitor` because the chart already exposes a stable `metrics` Service on port `8888`.

The chart defaults match the current listener model:

- admin on `:8080`
- metrics on `:8888`
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
- Synthetic admin observe endpoints: `:8080/observe/{receiver|exporter}/{signal}`
- Datadog API validation endpoint: `:18081/api/v1/validate`
- `GET :8080/results/{correlation_id}`
- `GET :8888/metrics`

### Config-driven Field Mapping (No Plugin Needed)

`datadog_raw` now maps decoded payload fields to canonical attributes via config.

Environment variable:

- `MDAI_FIDELITY_FIELD_MAPPING_PATH`: optional YAML path to field mapping config.
- `MDAI_FIDELITY_RULES_PATH`: optional YAML path to fidelity rules config.
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

When path is prefixed with `/exporter/{name}/...` or the synthetic admin observe path resolves an exporter name, exporter-specific mappings are merged on top of top-level `signals`, and exporter-specific canonical keys win when both define the same attribute.

Supported source expressions:

- direct key: `"[0].message"`
- JSON extraction: `"[0].message|json:service"`
- tag extraction from comma-separated tags: `"[0].ddtags|tag:correlation_id"`
- suffix match: `"suffix:.trace_id"`
- contains match: `"contains:.tags[|tag:env"`

Current limitation:

- Datadog protobuf decoding in this mode is intentionally narrow: `/series` and `/traces` are supported, but the validator still does not aim to be a complete Datadog protobuf decoder for every intake path.

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
curl -sS localhost:8080/results/traces:demo-1 | jq
```

## Demo assets

- Helm chart: `deployment/`
- Fidelity policy example: [internal/validator/fidelity-policy.yaml](/Users/justin/work/mdai-fidelity-validator/internal/validator/fidelity-policy.yaml)
- Random telemetry generator: [scripts/send-random-mlt.sh](scripts/send-random-mlt.sh)
- Raw Datadog MLT generator CLI: [cmd/ddmltgen/main.go](cmd/ddmltgen/main.go)

## Important limitation

This validator is now shaped around raw Datadog payload capture on both sides of a receiver/exporter boundary, but it still compares normalized attribute/value presence after decoding rather than byte-for-byte equivalence. For reliable pairing across ingress and egress observations, inject a stable identifier such as `correlation_id` before export.

Policy-based fidelity is driven by the file set via `MDAI_FIDELITY_RULES_PATH` (example: [internal/validator/fidelity-policy.yaml](/Users/justin/work/mdai-fidelity-validator/internal/validator/fidelity-policy.yaml)). `passed` in comparison results and `mdai_fidelity_required_*` Prometheus metrics are based on those required attributes. `full_payload_passed` remains available for strict flattened payload debugging.

`ddmltgen` now emits trace `status` on synthetic spans (`"Ok"` by default) so trace status can be mapped and compared when the exporter preserves it.

For the demo collector config, the Datadog exporter still validates API key syntax even if you only point it at the local validator proxy. Use a 32-character hex placeholder like `00000000000000000000000000000000` or a real key via `DD_API_KEY`.
