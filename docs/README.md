# Repository documentation

This documentation is organized around the questions an operator or contributor is
most likely to ask:

- [How the validator works](architecture.md) — runtime components, request flow,
  decoding, mapping, correlation, comparison, retention, and deployment.
- [How the metrics work](metrics.md) — the exact event behind every metric,
  PromQL examples, configuration dependencies, and debugging workflow.
- [C4 system-context view](architecture-c4.svg) — the fixed-layout runtime
  architecture embedded in the architecture guide.
- [Mermaid architecture source](architecture.mmd) — end-to-end processing flow.
- [Mermaid request sequence](request-sequence.mmd) — receiver/exporter pairing
  lifecycle.
- [Mermaid metric derivation](metric-derivation.mmd) — exact comparison-to-metric
  branches.
- [Structurizr DSL architecture source](architecture.dsl) — C4 system context,
  container, component, and dynamic views.

The diagrams are source artifacts rather than exported images so they can evolve
with the code and remain reviewable in pull requests.
