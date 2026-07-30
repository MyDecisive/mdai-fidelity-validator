# Agent guidance

This repository validates telemetry fidelity by comparing receiver-side and
exporter-side observations. Preserve the distinction between what the code
actually measures and what an operator may assume it measures.

## Read before changing behavior or documentation

Read the files relevant to the task:

- `docs/architecture.md` for runtime boundaries, data flow, correlation,
  canonicalization, retention, and package ownership.
- `docs/metrics.md` for metric events, labels, PromQL, and operational
  interpretation.
- `docs/architecture.dsl` for the complete C4 model.
- `deployment/files/field-mapping.yaml` and
  `deployment/files/fidelity-policy.yaml` together when changing canonical
  fields or required checks.

Treat implementation and tests as authoritative when documentation disagrees.
Update the documentation in the same change when behavior changes.

## Architectural invariants

- The validator observes mirrored payloads; it is not a telemetry forwarding
  proxy.
- One process owns four HTTP listeners: admin `:8080`, metrics `:8888`, receiver
  capture `:8126`, and exporter capture/API `:18081`.
- Comparison and pending state are process-local memory. Do not describe replicas
  as sharing state.
- `mdai_fidelity_payloads_received_total` counts captured HTTP requests.
  Comparison counters count completed decoded item pairs. Batch splitting means
  these counts need not match.
- The top-level full-payload comparison operates on canonical attributes emitted
  by field mapping, not every raw decoded wire field.
- Trace span comparison can fail the full signal result without emitting a
  corresponding top-level attribute failure.
- `ComparisonResult.passed` and `mdai_fidelity_required_*` represent policy
  evaluation. `ComparisonResult.full_payload_passed` and the non-required check
  counters represent the full canonical comparison.
- Decode or empty-mapping errors are retained for diagnostics but do not enter
  pairing or comparison counters.
- Correlation keys are signal-prefixed. For traces, intrinsic `trace_id` is
  preferred before `span_id` and `correlation_id`; other signals may use mapped
  IDs, headers, fields, or fingerprints.
- An absent policy makes a completed pair pass required-signal evaluation while
  emitting no required-attribute checks. Do not present that as proof of a useful
  fidelity contract.

## Mapping and policy changes

Treat mapping and policy as one versioned contract:

1. Verify every policy attribute is emitted under the same canonical name by
   receiver and exporter mappings.
2. Use mapping transforms for intentional representation normalization.
3. Reserve `presence_only` for fields whose values may legitimately change.
4. Add tests for both a passing pair and an intentional failure.
5. Update the alignment table in `docs/metrics.md`.

The packaged Helm files currently have known name gaps documented in
`docs/metrics.md`. Do not silently claim they are aligned. If fixing them, change
tests and documentation with the configuration.

## Documentation boundaries

Keep the root `README.md` as a short landing page containing purpose, quick start,
deployment, and links. Do not duplicate the architecture or metrics guides there.

Use:

- `docs/architecture.md` for design and runtime explanations.
- `docs/metrics.md` for metric contracts, queries, alerts, and troubleshooting.
- `docs/README.md` only as the documentation index.

Prefer precise terms:

- Say **full canonical comparison**, not “every raw field” or “byte-for-byte.”
- Say **required-policy comparison** for the configured contract.
- Distinguish request counts, decoded items, completed pairs, and pending items.

## Diagram maintenance

The embedded primary diagram is the fixed-layout C4 system-context SVG at
`docs/architecture-c4.svg`. It intentionally avoids Mermaid C4 auto-layout,
which produced overlapping arrows and labels.

- Keep the system-context view small: operator, telemetry pipeline, validator,
  and Prometheus.
- Use orthogonal connectors and dedicated whitespace for relationship labels.
- Do not replace it with an auto-laid-out Mermaid C4 diagram without visually
  verifying that no text or arrows overlap.
- Update `docs/architecture.dsl` when systems, containers, components, or
  relationships change.
- Use `docs/architecture.mmd`, `docs/request-sequence.mmd`, and
  `docs/metric-derivation.mmd` for detailed flows that do not fit cleanly in a
  C4 context view.
- Keep embedded diagrams and linked source artifacts synchronized.

Visually inspect diagram output after changing layout; syntax validation alone is
not sufficient.

## Validation

Run checks proportional to the change:

```bash
git diff --check
go test ./...
helm template mdai-fidelity-validator ./deployment -n mdai
```

Also verify changed Markdown links resolve. When diagrams change, render or
preview them and inspect readability at normal documentation width.
