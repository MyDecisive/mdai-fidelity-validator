# Metrics: semantics and use

## Mental model

Metrics are produced at three different lifecycle points:

```text
HTTP request accepted
  -> payloads_received_total
  -> decoded item waits for counterpart
       -> pending_payloads
       -> counterpart matches
            -> canonical full-payload counters
            -> required-policy counters
```

There is no periodic score calculation. The service exposes cumulative counters;
Prometheus queries turn them into rates, ratios, and alerts.

## Metric contract

| Metric | Type | Increment/update event | Labels |
|---|---|---|---|
| `mdai_fidelity_payloads_received_total` | Counter | Once per captured HTTP request after decoding yields at least one item; ignored paths do not count | `mdai_connection`, `source`, `signal` |
| `mdai_fidelity_pending_payloads` | Gauge | Set to the total unmatched item count after insert, match, expiry, or GC | `mdai_connection` |
| `mdai_fidelity_attribute_checks_total` | Counter | Once for each top-level canonical key classified by a completed full-payload comparison | `mdai_connection`, `signal`, `attribute`, `result` |
| `mdai_fidelity_signal_checks_total` | Counter | Once per completed item pair; pass iff the top-level canonical diff and trace span diff both pass | `mdai_connection`, `signal`, `result` |
| `mdai_fidelity_required_attribute_checks_total` | Counter | Once per policy-required attribute on a completed pair | `mdai_connection`, `signal`, `attribute`, `result` |
| `mdai_fidelity_required_signal_checks_total` | Counter | Once per completed pair; pass iff all configured required attributes pass | `mdai_connection`, `signal`, `result` |

`payloads_received_total` counts HTTP requests, while the four check counters
count decoded item pairs. One batched request can split into several trace groups
or metric series, so received traffic and completed checks are not expected to
have a one-to-one relationship.

The pending gauge has no `source` or `signal` label. Use `/debug/pending` to
identify the contents when it rises.

## Exactly what “attribute” means

For `mdai_fidelity_attribute_checks_total`, `attribute` is a key from the
canonical mapped union after comparison normalization. It is not necessarily a
raw Datadog path. A missing key produces one `fail` increment for that key, as
does a value mismatch.

For `mdai_fidelity_required_attribute_checks_total`, `attribute` is the name
written in the fidelity policy. Policy lookup first seeks that exact canonical
key. Legacy deep-path matching exists for non-log signals when an exact key is
absent, but canonical mapping and aligned policy names are the intended model.

Span-level attributes can make `mdai_fidelity_signal_checks_total` fail without a
corresponding failing `mdai_fidelity_attribute_checks_total` series because span
deltas are stored in `ComparisonResult.spans`, not recorded in the top-level
attribute counter loop. Inspect the result API when those signals disagree.

## How the metrics are leveraged

Use them as a diagnostic funnel:

1. **Traffic:** compare receiver/exporter request rates.
2. **Pairing:** watch pending growth and completion rates.
3. **Contract SLI:** calculate pass fraction from required-signal checks.
4. **Root cause:** rank failing required attributes.
5. **Unexpected drift:** compare full-payload results against policy results.
6. **Forensics:** inspect `/results/{signal}:{id}` or `/debug/results`.

Policy success ratio over a window:

```promql
sum by (mdai_connection, signal) (
  increase(mdai_fidelity_required_signal_checks_total{result="pass"}[1h])
)
/
sum by (mdai_connection, signal) (
  increase(mdai_fidelity_required_signal_checks_total[1h])
)
```

Guard a dashboard against a zero denominator:

```promql
(
  sum by (mdai_connection, signal) (
    increase(mdai_fidelity_required_signal_checks_total{result="pass"}[1h])
  )
/
  sum by (mdai_connection, signal) (
    increase(mdai_fidelity_required_signal_checks_total[1h])
  )
)
and
sum by (mdai_connection, signal) (
  increase(mdai_fidelity_required_signal_checks_total[1h])
) > 0
```

Receiver/exporter request-rate imbalance:

```promql
sum by (mdai_connection, signal, source) (
  rate(mdai_fidelity_payloads_received_total[5m])
)
```

Required failures by attribute:

```promql
sum by (mdai_connection, signal, attribute) (
  increase(mdai_fidelity_required_attribute_checks_total{result="fail"}[15m])
)
```

Full comparison fails while the declared policy passes:

```promql
sum by (mdai_connection, signal) (
  increase(mdai_fidelity_signal_checks_total{result="fail"}[15m])
)
and
sum by (mdai_connection, signal) (
  increase(mdai_fidelity_required_signal_checks_total{result="fail"}[15m])
) == 0
```

This last query is directional evidence, not pairwise proof: separate cumulative
counter families cannot be joined by correlation ID.

## Recommended alerts

Traffic and check volumes should be gated so “no data” is not mistaken for
perfect fidelity.

```promql
# At least one policy failure and enough comparison volume to be meaningful.
sum by (mdai_connection, signal) (
  increase(mdai_fidelity_required_signal_checks_total{result="fail"}[15m])
) > 0
and
sum by (mdai_connection, signal) (
  increase(mdai_fidelity_required_signal_checks_total[15m])
) >= 5
```

```promql
# Pairing backlog sustained for 10 minutes (set `for: 10m` in the alert rule).
mdai_fidelity_pending_payloads > 0
```

Tune the backlog threshold to batch volume and retention. A transient positive
gauge is normal between the arrival of the two sides.

## Configuration alignment: the main correctness dependency

The policy names need to exist in the canonical output of the mapping for the
same signal. The packaged Helm files currently contain these notable differences:

| Signal | Policy name | Canonical mapping status |
|---|---|---|
| Metrics | `host`, `metric`, `tags`, `type` | Mapping instead defines `metric_name`, `point_timestamp`, `point_value`, `service`, and `env`; these policy names are not mapped |
| Traces | `trace_id`, `span_count`, `service` | `trace_id` and `service` are mapped; `span_count` is not |
| Logs | `ddsource`, `ddtags`, `hostname`, `service`, `message`, `status`, `timestamp` | All except `ddtags` are mapped |

As shipped, this can make required checks fail for absent canonical attributes
even when the wire payload otherwise looks healthy. Treat the mapping and policy
as one versioned contract and test them together whenever either changes.

A useful review checklist:

- every policy attribute is emitted by receiver mapping;
- the exporter mapping emits the same canonical name;
- value-transform operations normalize expected representation differences;
- only volatile fields use `presence_only`;
- test payloads exercise both pass and intentional failure cases.

## Interpreting disagreements

| Required result | Full result | Interpretation |
|---|---|---|
| pass | pass | Declared contract and all mapped comparable fields survived |
| pass | fail | Required contract survived, but another mapped field or a trace span drifted |
| fail | pass | Usually a policy/mapping semantic issue; investigate exact vs legacy lookup and presence-only rules |
| fail | fail | Required contract failed and broader drift exists |

With an empty policy, every completed pair increments required-signal `pass` and
emits no required-attribute series. This is mechanically valid but not a useful
fidelity contract.

## Cardinality and identity

`mdai_connection` comes from `MDAI_CONNECTION_NAME`, not Kubernetes metadata.
Give each independently meaningful pipeline connection a stable, bounded value.
The `attribute` label is configuration/data-derived, so avoid mapping arbitrary
dynamic path names into canonical keys. Values are never labels.

The default Prometheus registry is process-global. Prometheus should scrape the
`:8888/metrics` Service endpoint; the Helm `ServiceMonitor` is optional and
disabled by default.

## Debugging a bad metric

1. Confirm both `source` values increase in `payloads_received_total`.
2. Check `pending_payloads`; inspect `GET :8080/debug/pending` if non-zero.
3. Inspect `GET :8080/debug/last-signal/{source}/{signal}` to verify decode format,
   canonical attributes, correlation strategy in logs, and decode errors.
4. Inspect `GET :8080/results/{signal}:{correlation-value}` or
   `GET :8080/debug/results`.
5. Compare `passed`, `full_payload_passed`, `required_checks`, top-level deltas,
   and trace `spans`.
6. Verify the mounted mapping and policy use the same canonical names.
7. Remember that config reload is polling-based and keeps the last known good
   config after an invalid update.
