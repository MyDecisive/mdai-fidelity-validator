package validator

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func TestComparePairPassesWhenFieldsMatch(t *testing.T) {
	receiver := &observedPayload{
		source:      "receiver",
		signal:      "traces",
		correlation: "abc",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"trace_id": "123",
			"span_id":  "456",
		},
	}
	exporter := &observedPayload{
		source:      "exporter",
		signal:      "traces",
		correlation: "abc",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"trace_id": "123",
			"span_id":  "456",
		},
	}

	result := comparePair(receiver, exporter, Policy{})
	if !result.Passed {
		t.Fatalf("expected pass, got %#v", result)
	}
	if len(result.Matched) != 2 {
		t.Fatalf("expected 2 matched fields, got %d", len(result.Matched))
	}
}

func TestComparePairFailsWhenFieldsDiffer(t *testing.T) {
	receiver := &observedPayload{
		source:      "receiver",
		signal:      "metrics",
		correlation: "abc",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"series[0].metric": "demo.metric",
			"series[0].value":  "10",
		},
	}
	exporter := &observedPayload{
		source:      "exporter",
		signal:      "metrics",
		correlation: "abc",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"series[0].metric": "demo.metric",
			"series[0].value":  "11",
			"series[0].host":   "node-a",
		},
	}

	result := comparePair(receiver, exporter, Policy{})
	if result.FullPayloadPassed {
		t.Fatalf("expected failure, got %#v", result)
	}
	if len(result.Mismatched) != 1 {
		t.Fatalf("expected 1 mismatched field, got %d", len(result.Mismatched))
	}
	if len(result.MissingIn) != 1 {
		t.Fatalf("expected 1 missing field, got %d", len(result.MissingIn))
	}
}

func TestFlattenValueMap(t *testing.T) {
	var payload any
	if err := json.Unmarshal([]byte(`{"resource":{"attributes":[{"key":"service.name","value":"demo"}]},"value":1,"ok":true}`), &payload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	fields := flattenValueMap(payload)

	if fields["resource.attributes[0].key"] != "service.name" {
		t.Fatalf("unexpected key field: %#v", fields)
	}
	if fields["value"] != "1" {
		t.Fatalf("unexpected numeric field: %#v", fields)
	}
	if fields["ok"] != "true" {
		t.Fatalf("unexpected bool field: %#v", fields)
	}
}

func TestDecodeBodyJSONGzip(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte(`{"correlation_id":"demo-1","value":1}`)); err != nil {
		t.Fatalf("write gzip: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}

	payload, format, err := decodeBody(compressed.Bytes(), "", "gzip", "application/json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if format != "json" {
		t.Fatalf("expected json format, got %q", format)
	}

	fields := flattenValueMap(payload)
	if fields["correlation_id"] != "demo-1" {
		t.Fatalf("unexpected fields: %#v", fields)
	}
}

func TestDecodeBodyMsgpack(t *testing.T) {
	body, err := msgpack.Marshal(map[string]any{
		"resource": map[string]any{
			"correlation_id": "raw-dd-1",
		},
	})
	if err != nil {
		t.Fatalf("marshal msgpack: %v", err)
	}

	payload, format, err := decodeBody(body, "", "", "application/msgpack")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if format != "msgpack" {
		t.Fatalf("expected msgpack format, got %q", format)
	}

	fields := flattenValueMap(payload)
	if fields["resource.correlation_id"] != "raw-dd-1" {
		t.Fatalf("unexpected fields: %#v", fields)
	}
}

func TestCorrelationCandidatesPreferCorrelationIDPaths(t *testing.T) {
	fields := map[string]string{
		"spans[0].meta.correlation_id":  "dd-span-1",
		"resource.correlation_id":       "resource-1",
		"fidelity.correlation_id":       "fidelity-1",
		"resource.attributes.trace_id":  "trace-1",
		"series[0].metric":              "metric-name",
		"logs[0].attributes.service":    "checkout",
		"logs[0].attributes.trace_id":   "trace-2",
		"logs[0].attributes.otherthing": "value",
	}

	candidates := correlationCandidates(fields)
	if len(candidates) == 0 {
		t.Fatal("expected non-empty candidates")
	}
	if candidates[0] != "fidelity.correlation_id" && candidates[0] != "resource.correlation_id" && candidates[0] != "correlation_id" {
		t.Fatalf("expected correlation_id path to be preferred, got %q", candidates[0])
	}
}

func TestInferSignalFromDatadogPath(t *testing.T) {
	cases := map[string]string{
		"/v0.4/traces":                "traces",
		"/api/v1/series":              "metrics",
		"/api/v2/logs":                "logs",
		"/something/else":             "unknown",
		"/api/v1/distribution_points": "metrics",
	}

	for path, want := range cases {
		if got := inferSignalFromDatadogPath(path); got != want {
			t.Fatalf("inferSignalFromDatadogPath(%q)=%q want %q", path, got, want)
		}
	}
}

func TestSelectedHeaders(t *testing.T) {
	header := http.Header{}
	header.Set("Content-Type", "application/msgpack")
	header.Set("DD-API-KEY", "secret")
	header.Set("X-Unused", "ignored")

	got := selectedHeaders(header)
	if got["Content-Type"] != "application/msgpack" {
		t.Fatalf("unexpected content type: %#v", got)
	}
	if got["DD-API-KEY"] != "secret" {
		t.Fatalf("unexpected dd api key: %#v", got)
	}
	if _, ok := got["X-Unused"]; ok {
		t.Fatalf("unexpected x-unused in %#v", got)
	}
}

func TestDeriveCorrelationFromMetricTagsBeforeMetricName(t *testing.T) {
	fields := map[string]string{
		"series[0].metric":  "ddgen.checkout.duration",
		"series[0].tags[0]": "service:checkout",
		"series[0].tags[1]": "correlation_id:corr-123",
	}

	got := deriveCorrelationFromFields("metrics", fields)
	if got != "metrics:corr-123" {
		t.Fatalf("deriveCorrelationFromFields()=%q want %q", got, "metrics:corr-123")
	}
}

func TestDeriveCorrelationFromLogDDTags(t *testing.T) {
	fields := map[string]string{
		"[0].ddtags": "env:dev,correlation_id:corr-log-1,fidelity.correlation_id:corr-log-1",
	}

	got := deriveCorrelationFromFields("logs", fields)
	if got != "logs:corr-log-1" {
		t.Fatalf("deriveCorrelationFromFields()=%q want %q", got, "logs:corr-log-1")
	}
}
