package validator

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
	cases := map[string]Signal{
		"/v0.4/traces":                SignalTraces,
		"/api/v1/series":              SignalMetrics,
		"/api/v2/logs":                SignalLogs,
		"/something/else":             SignalUnknown,
		"/api/v1/distribution_points": SignalMetrics,
	}

	for path, want := range cases {
		if got := inferSignalFromDatadogPath(path); got != want {
			t.Fatalf("inferSignalFromDatadogPath(%q)=%q want %q", path, got, want)
		}
	}
}

func TestParseExporterPath(t *testing.T) {
	cases := []struct {
		path           string
		wantExporter   string
		wantNormalized string
	}{
		{path: "/api/v2/logs", wantExporter: "", wantNormalized: "/api/v2/logs"},
		{path: "/exporter/datadog/api/v2/logs", wantExporter: "datadog", wantNormalized: "/api/v2/logs"},
		{path: "/exporter/datadog", wantExporter: "datadog", wantNormalized: "/"},
		{path: "/splunk/services/collector/event", wantExporter: "splunk", wantNormalized: "/services/collector/event"},
	}

	for _, tc := range cases {
		gotExporter, gotNormalized := parseExporterPath(tc.path)
		if gotExporter != tc.wantExporter || gotNormalized != tc.wantNormalized {
			t.Fatalf("parseExporterPath(%q)=(%q,%q) want (%q,%q)", tc.path, gotExporter, gotNormalized, tc.wantExporter, tc.wantNormalized)
		}
	}
}

func TestSelectedHeaders(t *testing.T) {
	header := http.Header{}
	header.Set("Content-Type", "application/msgpack")
	header.Set("Dd-Api-Key", "secret")
	header.Set("X-Fidelity-Id", "fid-1")
	header.Set("X-Request-ID", "req-1")
	header.Set("X-Unused", "ignored")

	got := selectedHeaders(header)
	if got["Content-Type"] != "application/msgpack" {
		t.Fatalf("unexpected content type: %#v", got)
	}
	if got["DD-API-KEY"] != "secret" {
		t.Fatalf("unexpected dd api key: %#v", got)
	}
	if got["X-Fidelity-ID"] != "fid-1" {
		t.Fatalf("unexpected x-fidelity-id: %#v", got)
	}
	if got["X-Request-ID"] != "req-1" {
		t.Fatalf("unexpected x-request-id: %#v", got)
	}
	if _, ok := got["X-Unused"]; ok {
		t.Fatalf("unexpected x-unused in %#v", got)
	}
}

func TestResolveCorrelationIDFromHeaderFallbacks(t *testing.T) {
	fields := map[string]string{}
	body := []byte(`{"message":"hello"}`)

	t.Run("x-fidelity-id", func(t *testing.T) {
		headers := http.Header{}
		headers.Set("X-Fidelity-Id", "fid-123")
		got := resolveCorrelationID(SignalLogs, fields, headers, body)
		if got.CorrelationID != "logs:fid-123" || got.Strategy != "header" || got.Field != "X-Fidelity-ID" {
			t.Fatalf("unexpected decision: %+v", got)
		}
	})

	t.Run("x-request-id", func(t *testing.T) {
		headers := http.Header{}
		headers.Set("X-Request-ID", "req-123")
		got := resolveCorrelationID(SignalLogs, fields, headers, body)
		if got.CorrelationID != "logs:req-123" || got.Strategy != "header" || got.Field != "X-Request-ID" {
			t.Fatalf("unexpected decision: %+v", got)
		}
	})
}

func TestResolveCorrelationIDPrefersHeaderOverField(t *testing.T) {
	fields := map[string]string{
		"correlation_id": "from-field",
	}
	headers := http.Header{}
	headers.Set("X-Correlation-ID", "from-header")

	got := resolveCorrelationID(SignalLogs, fields, headers, []byte(`{"message":"hello"}`))
	if got.CorrelationID != "logs:from-header" || got.Strategy != "header" || got.Field != "X-Correlation-ID" {
		t.Fatalf("unexpected decision: %+v", got)
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

func TestComparePairNormalizesSingleLogArrayPrefix(t *testing.T) {
	receiver := &observedPayload{
		source:      "receiver",
		signal:      "logs",
		correlation: "logs:corr-1",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"message": "ddgen synthetic log event",
			"ddtags":  "env:dev,correlation_id:corr-1",
		},
	}
	exporter := &observedPayload{
		source:      "exporter",
		signal:      "logs",
		correlation: "logs:corr-1",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"[0].message": "ddgen synthetic log event",
			"[0].ddtags":  "env:dev,correlation_id:corr-1",
		},
	}

	result := comparePair(receiver, exporter, Policy{})
	if !result.FullPayloadPassed {
		t.Fatalf("expected full payload pass, got %#v", result)
	}
	if len(result.Matched) != 2 {
		t.Fatalf("expected 2 matched fields, got %d", len(result.Matched))
	}
}

func TestComparePairStripsCorrelationFromLogDDTags(t *testing.T) {
	receiver := &observedPayload{
		source:      "receiver",
		signal:      "logs",
		correlation: "logs:corr-1",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"ddtags": "env:dev,correlation_id:corr-a,fidelity.correlation_id:corr-a,otel_source:datadog_exporter",
		},
	}
	exporter := &observedPayload{
		source:      "exporter",
		signal:      "logs",
		correlation: "logs:corr-1",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"[0].ddtags": "env:dev,correlation_id:corr-b,fidelity.correlation_id:corr-b,otel_source:datadog_exporter",
		},
	}

	result := comparePair(receiver, exporter, Policy{})
	if !result.FullPayloadPassed {
		t.Fatalf("expected full payload pass, got %#v", result)
	}
}

func TestComparePairStripsCorrelationFromLogMessageJSON(t *testing.T) {
	receiver := &observedPayload{
		source:      "receiver",
		signal:      "logs",
		correlation: "logs:corr-1",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"message": `{"message":"ddgen synthetic log event","service":"ddgen-svc","correlation_id":"corr-a"}`,
		},
	}
	exporter := &observedPayload{
		source:      "exporter",
		signal:      "logs",
		correlation: "logs:corr-1",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"[0].message": `{"service":"ddgen-svc","correlation_id":"corr-b","message":"ddgen synthetic log event"}`,
		},
	}

	result := comparePair(receiver, exporter, Policy{})
	if !result.FullPayloadPassed {
		t.Fatalf("expected full payload pass, got %#v", result)
	}
}

func TestResolvePairForRequest(t *testing.T) {
	svc := &Service{
		defaultPair: defaultPairID,
		pairs: map[string]configuredPair{
			defaultPairID: {
				PairConfig: PairConfig{
					ID:                 defaultPairID,
					ReceiverTranslator: defaultTranslatorID,
					ExporterTranslator: defaultTranslatorID,
				},
			},
			"shadow-a": {
				PairConfig: PairConfig{
					ID:                 "shadow-a",
					ReceiverTranslator: defaultTranslatorID,
					ExporterTranslator: defaultTranslatorID,
					ReceiverPorts:      []string{"18126"},
				},
			},
		},
		receiverPairByPort: map[string]string{"18126": "shadow-a"},
		exporterPairByPort: map[string]string{},
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v0.4/traces", http.NoBody)
	gotDefault := svc.resolvePairForRequest(req, "receiver", ":8126")
	if gotDefault.ID != defaultPairID {
		t.Fatalf("expected default pair %q, got %q", defaultPairID, gotDefault.ID)
	}

	req.Header.Set("X-Forwarded-Port", "18126")
	gotPortMapped := svc.resolvePairForRequest(req, "receiver", ":8126")
	if gotPortMapped.ID != "shadow-a" {
		t.Fatalf("expected port-mapped pair shadow-a, got %q", gotPortMapped.ID)
	}

	req.Header.Set(pairHeaderKey, "shadow-a")
	gotNamed := svc.resolvePairForRequest(req, "receiver", ":8126")
	if gotNamed.ID != "shadow-a" {
		t.Fatalf("expected pair shadow-a, got %q", gotNamed.ID)
	}
}

func TestHandleAdminPairs(t *testing.T) {
	svc := &Service{
		defaultPair: defaultPairID,
		translators: map[string]PayloadTranslator{
			defaultTranslatorID: datadogRawTranslator{mapping: newMappingStore(defaultFieldMapping())},
		},
		pairs: map[string]configuredPair{
			defaultPairID: {
				PairConfig: PairConfig{
					ID:                 defaultPairID,
					ReceiverTranslator: defaultTranslatorID,
					ExporterTranslator: defaultTranslatorID,
				},
			},
		},
	}

	body := `{"id":"shadow-b","receiver_translator":"datadog_raw","exporter_translator":"datadog_raw","receiver_upstream":"http://receiver.example:8126","exporter_upstream":"http://exporter.example:8081","default":true}`
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/pairs", strings.NewReader(body))
	rec := httptest.NewRecorder()

	svc.handleAdminPairs(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	pair, ok := svc.pairs["shadow-b"]
	if !ok {
		t.Fatal("expected pair shadow-b to be saved")
	}
	if pair.receiverUpstream == nil || pair.receiverUpstream.String() != "http://receiver.example:8126" {
		t.Fatalf("unexpected receiver upstream: %+v", pair.receiverUpstream)
	}
	if pair.exporterUpstream == nil || pair.exporterUpstream.String() != "http://exporter.example:8081" {
		t.Fatalf("unexpected exporter upstream: %+v", pair.exporterUpstream)
	}
	if svc.defaultPair != "shadow-b" {
		t.Fatalf("expected default pair shadow-b, got %q", svc.defaultPair)
	}

	body = `{"id":"shadow-c","receiver_translator":"datadog_raw","exporter_translator":"datadog_raw","receiver_ports":["18126"],"exporter_ports":["18081"]}`
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/pairs", strings.NewReader(body))
	rec = httptest.NewRecorder()
	svc.handleAdminPairs(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, rec.Code, rec.Body.String())
	}
	if got := svc.receiverPairByPort["18126"]; got != "shadow-c" {
		t.Fatalf("expected receiver port mapping to shadow-c, got %q", got)
	}
	if got := svc.exporterPairByPort["18081"]; got != "shadow-c" {
		t.Fatalf("expected exporter port mapping to shadow-c, got %q", got)
	}
}
