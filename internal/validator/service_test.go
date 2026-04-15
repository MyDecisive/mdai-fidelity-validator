package validator

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

type staticTranslator struct {
	name    string
	decoded DecodedPayload
}

func (t staticTranslator) Name() string { return t.name }

func (t staticTranslator) Decode(_ Signal, _, _, _ string, _ []byte) DecodedPayload {
	return t.decoded
}

func newMetricsTestService(connection string) (*Service, *prometheus.Registry) {
	registry := prometheus.NewRegistry()

	receivedTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mdai_fidelity_payloads_received_total",
		Help: "Number of payloads received by connection, source, and signal.",
	}, []string{"mdai_connection", "source", "signal"})
	attributeEval := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mdai_fidelity_attribute_checks_total",
		Help: "Number of attribute comparisons by connection, signal, attribute, and result.",
	}, []string{"mdai_connection", "signal", "attribute", "result"})
	signalEval := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mdai_fidelity_signal_checks_total",
		Help: "Number of whole-signal comparisons by connection, signal, and result.",
	}, []string{"mdai_connection", "signal", "result"})
	requiredEval := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mdai_fidelity_required_attribute_checks_total",
		Help: "Number of required attribute comparisons by connection, signal, attribute, and result.",
	}, []string{"mdai_connection", "signal", "attribute", "result"})
	requiredSig := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mdai_fidelity_required_signal_checks_total",
		Help: "Number of policy-based whole-signal comparisons by connection, signal, and result.",
	}, []string{"mdai_connection", "signal", "result"})
	pendingGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mdai_fidelity_pending_payloads",
		Help: "Number of payloads waiting for their correlated counterpart by connection.",
	}, []string{"mdai_connection"})

	registry.MustRegister(receivedTotal, attributeEval, signalEval, requiredEval, requiredSig, pendingGauge)

	svc := &Service{
		logger:        zap.NewNop(),
		shards:        make([]*shard, numShards),
		connection:    connection,
		lastBySource:  map[string]*observedPayload{},
		lastByKey:     map[string]*observedPayload{},
		translators:   map[string]PayloadTranslator{},
		receivedTotal: receivedTotal,
		attributeEval: attributeEval,
		signalEval:    signalEval,
		requiredEval:  requiredEval,
		requiredSig:   requiredSig,
		pendingGauge:  pendingGauge,
	}

	for i := range numShards {
		svc.shards[i] = &shard{
			pending:    map[string]*observedPayload{},
			lastResult: map[string]ComparisonResult{},
		}
	}

	return svc, registry
}

func requireMetricHasConnectionLabel(t *testing.T, registry *prometheus.Registry, metricName, connection string) {
	t.Helper()

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	for _, family := range families {
		if family.GetName() != metricName {
			continue
		}
		if len(family.Metric) == 0 {
			t.Fatalf("metric %s had no series", metricName)
		}
		for _, metric := range family.Metric {
			if !hasLabel(metric, "mdai_connection", connection) {
				t.Fatalf("metric %s missing mdai_connection=%q label: %+v", metricName, connection, metric.GetLabel())
			}
		}
		return
	}

	t.Fatalf("metric family %s not found", metricName)
}

func hasLabel(metric *dto.Metric, name, value string) bool {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name && label.GetValue() == value {
			return true
		}
	}
	return false
}

func counterValue(t *testing.T, metric prometheus.Metric) float64 {
	t.Helper()

	dtoMetric := &dto.Metric{}
	if err := metric.Write(dtoMetric); err != nil {
		t.Fatalf("write counter metric: %v", err)
	}

	return dtoMetric.GetCounter().GetValue()
}

func gaugeValue(t *testing.T, metric prometheus.Metric) float64 {
	t.Helper()

	dtoMetric := &dto.Metric{}
	if err := metric.Write(dtoMetric); err != nil {
		t.Fatalf("write gauge metric: %v", err)
	}

	return dtoMetric.GetGauge().GetValue()
}

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

func TestRecordMetricsUsesConnectionLabel(t *testing.T) {
	svc, registry := newMetricsTestService("shadow-a")

	svc.recordMetrics(ComparisonResult{
		Signal:            SignalTraces,
		Matched:           []string{"trace_id"},
		Mismatched:        []AttributeDelta{{Attribute: "span_id"}},
		MissingIn:         []MissingField{{Attribute: "service.name"}},
		FullPayloadPassed: false,
		Passed:            true,
		RequiredChecks: []RequiredAttributeCheck{
			{Attribute: "trace_id", Passed: true},
			{Attribute: "service.name", Passed: false},
		},
	})

	if got := counterValue(t, svc.attributeEval.WithLabelValues("shadow-a", "traces", "trace_id", "pass")); got != 1 {
		t.Fatalf("attribute pass count=%v want 1", got)
	}
	if got := counterValue(t, svc.attributeEval.WithLabelValues("shadow-a", "traces", "span_id", "fail")); got != 1 {
		t.Fatalf("attribute fail count=%v want 1", got)
	}
	if got := counterValue(t, svc.attributeEval.WithLabelValues("shadow-a", "traces", "service.name", "fail")); got != 1 {
		t.Fatalf("missing-field fail count=%v want 1", got)
	}
	if got := counterValue(t, svc.signalEval.WithLabelValues("shadow-a", "traces", "fail")); got != 1 {
		t.Fatalf("signal fail count=%v want 1", got)
	}
	if got := counterValue(t, svc.requiredSig.WithLabelValues("shadow-a", "traces", "pass")); got != 1 {
		t.Fatalf("required signal pass count=%v want 1", got)
	}
	if got := counterValue(t, svc.requiredEval.WithLabelValues("shadow-a", "traces", "trace_id", "pass")); got != 1 {
		t.Fatalf("required attribute pass count=%v want 1", got)
	}
	if got := counterValue(t, svc.requiredEval.WithLabelValues("shadow-a", "traces", "service.name", "fail")); got != 1 {
		t.Fatalf("required attribute fail count=%v want 1", got)
	}

	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_attribute_checks_total", "shadow-a")
	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_signal_checks_total", "shadow-a")
	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_required_attribute_checks_total", "shadow-a")
	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_required_signal_checks_total", "shadow-a")
}

func TestCaptureRequestUsesConnectionLabelForReceivedMetric(t *testing.T) {
	svc, registry := newMetricsTestService("shadow-b")
	svc.translators[defaultTranslatorID] = staticTranslator{
		name: defaultTranslatorID,
		decoded: DecodedPayload{
			Signal:      SignalTraces,
			Attributes:  map[string]string{},
			Format:      "json",
			DecodeError: "decode failed",
		},
	}

	req := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"/v0.4/traces",
		strings.NewReader(`{"trace_id":"123"}`),
	)

	observed, result, matched, err := svc.captureRequest(
		defaultPairID,
		defaultTranslatorID,
		"receiver",
		SignalTraces,
		":8126",
		"/v0.4/traces",
		req,
	)
	if err != nil {
		t.Fatalf("captureRequest() error = %v", err)
	}
	if observed == nil {
		t.Fatal("expected observed payload")
	}
	if result != nil || matched {
		t.Fatalf("expected no comparison for decode error, got result=%v matched=%v", result, matched)
	}
	if got := counterValue(t, svc.receivedTotal.WithLabelValues("shadow-b", "receiver", "traces")); got != 1 {
		t.Fatalf("received count=%v want 1", got)
	}

	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_payloads_received_total", "shadow-b")
}

func TestUpdatePendingGaugeUsesConnectionLabel(t *testing.T) {
	svc, registry := newMetricsTestService("shadow-c")
	svc.shards[0].pending["traces:a"] = &observedPayload{}
	svc.shards[1].pending["metrics:b"] = &observedPayload{}

	svc.updatePendingGauge()

	if got := gaugeValue(t, svc.pendingGauge.WithLabelValues("shadow-c")); got != 2 {
		t.Fatalf("pending gauge=%v want 2", got)
	}

	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_pending_payloads", "shadow-c")
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
		logger:      zap.NewNop(),
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
		logger:      zap.NewNop(),
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

	body = `{"id":"shadow-d","receiver_translator":"datadog_raw","exporter_translator":"datadog_raw","exporter_ignore_paths":["/api/beta/sketches","api/v2/sketches"]}`
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/pairs", strings.NewReader(body))
	rec = httptest.NewRecorder()
	svc.handleAdminPairs(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, rec.Code, rec.Body.String())
	}
	if got := svc.pairs["shadow-d"].ExporterIgnorePaths; !reflect.DeepEqual(got, []string{"/api/beta/sketches", "/api/v2/sketches"}) {
		t.Fatalf("unexpected exporter ignore paths: %#v", got)
	}
}

func TestNormalizePathPatternList(t *testing.T) {
	got, err := normalizePathPatternList([]string{" api/beta/sketches ", "/api/beta/*", "/api/beta/sketches"})
	if err != nil {
		t.Fatalf("normalizePathPatternList() error = %v", err)
	}
	want := []string{"/api/beta/*", "/api/beta/sketches"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalizePathPatternList() = %#v want %#v", got, want)
	}
}

func TestConfiguredPairShouldIgnorePath(t *testing.T) {
	pair := configuredPair{
		PairConfig: PairConfig{
			ID:                  "shadow-e",
			ExporterIgnorePaths: []string{"/api/beta/sketches", "/api/v2/*"},
			ReceiverIgnorePaths: []string{"/v0.4/traces"},
		},
	}

	if !pair.shouldIgnorePath("exporter", "/exporter/datadog/api/beta/sketches") {
		t.Fatal("expected exporter path to be ignored")
	}
	if !pair.shouldIgnorePath("exporter", "/api/v2/series") {
		t.Fatal("expected exporter wildcard path to be ignored")
	}
	if !pair.shouldIgnorePath("receiver", "/v0.4/traces") {
		t.Fatal("expected receiver path to be ignored")
	}
	if pair.shouldIgnorePath("exporter", "/exporter/datadog/api/v1/validate") {
		t.Fatal("did not expect validate path to be ignored")
	}
}

func TestHandleExporterAPIIgnoresConfiguredPath(t *testing.T) {
	svc := &Service{
		logger:      zap.NewNop(),
		defaultPair: defaultPairID,
		pairs: map[string]configuredPair{
			defaultPairID: {
				PairConfig: PairConfig{
					ID:                  defaultPairID,
					ReceiverTranslator:  defaultTranslatorID,
					ExporterTranslator:  defaultTranslatorID,
					ExporterIgnorePaths: []string{"/api/beta/sketches"},
				},
			},
		},
		translators: map[string]PayloadTranslator{
			defaultTranslatorID: datadogRawTranslator{mapping: newMappingStore(defaultFieldMapping())},
		},
	}

	req := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"/exporter/datadog/api/beta/sketches",
		strings.NewReader(`{"ignored":true}`),
	)
	rec := httptest.NewRecorder()

	svc.handleExporterAPI(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if _, ok := svc.lastBySource["exporter"]; ok {
		t.Fatal("did not expect ignored exporter payload to be captured")
	}
}
