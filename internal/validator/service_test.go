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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

type staticTranslator struct {
	name    string
	decoded DecodedPayload
	batch   []DecodedPayload // when set, DecodeAll returns this slice; implements BatchDecoder
}

func (t staticTranslator) Name() string { return t.name }

func (t staticTranslator) Decode(_ Signal, _, _, _ string, _ []byte) DecodedPayload {
	return t.decoded
}

func (t staticTranslator) DecodeAll(_ Signal, _, _, _ string, _ []byte) []DecodedPayload {
	if len(t.batch) > 0 {
		return t.batch
	}
	return []DecodedPayload{t.decoded}
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
		connection:    connection,
		pending:       map[string]*observedPayload{},
		lastResults:   map[string]ComparisonResult{},
		translators:   map[string]PayloadTranslator{},
		receivedTotal: receivedTotal,
		attributeEval: attributeEval,
		signalEval:    signalEval,
		requiredEval:  requiredEval,
		requiredSig:   requiredSig,
		pendingGauge:  pendingGauge,
	}

	return svc, registry
}

func requireMetricHasConnectionLabel(t *testing.T, registry *prometheus.Registry, metricName, connection string) {
	t.Helper()

	families, err := registry.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != metricName {
			continue
		}
		require.NotEmpty(t, family.GetMetric(), "metric %s had no series", metricName)
		for _, metric := range family.GetMetric() {
			assert.True(t, hasLabel(metric, "mdai_connection", connection), "metric %s missing mdai_connection=%q label: %+v", metricName, connection, metric.GetLabel())
		}
		return
	}

	require.Failf(t, "metric family missing", "metric family %s not found", metricName)
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
	require.NoError(t, metric.Write(dtoMetric))

	return dtoMetric.GetCounter().GetValue()
}

func gaugeValue(t *testing.T, metric prometheus.Metric) float64 {
	t.Helper()

	dtoMetric := &dto.Metric{}
	require.NoError(t, metric.Write(dtoMetric))

	return dtoMetric.GetGauge().GetValue()
}

func TestComparePairPassesWhenFieldsMatch(t *testing.T) {
	t.Parallel()

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
	assert.True(t, result.Passed, "expected pass, got %#v", result)
	assert.Len(t, result.Matched, 2)
}

func TestComparePairFailsWhenFieldsDiffer(t *testing.T) {
	t.Parallel()

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
	assert.False(t, result.FullPayloadPassed, "expected failure, got %#v", result)
	assert.Len(t, result.Mismatched, 1)
	assert.Len(t, result.MissingIn, 1)
}

func TestFlattenValueMap(t *testing.T) {
	t.Parallel()

	var payload any
	require.NoError(t, json.Unmarshal([]byte(`{"resource":{"attributes":[{"key":"service.name","value":"demo"}]},"value":1,"ok":true}`), &payload))

	fields := flattenValueMap(payload)

	assert.Equal(t, "service.name", fields["resource.attributes[0].key"])
	assert.Equal(t, "1", fields["value"])
	assert.Equal(t, "true", fields["ok"])
}

func TestDecodeBodyJSONGzip(t *testing.T) {
	t.Parallel()

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write([]byte(`{"correlation_id":"demo-1","value":1}`))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	payload, format, err := decodeBody(compressed.Bytes(), "", "gzip", "application/json")
	require.NoError(t, err)
	assert.Equal(t, "json", format)

	fields := flattenValueMap(payload)
	assert.Equal(t, "demo-1", fields["correlation_id"])
}

func TestDecodeBodyJSONPreservesLargeTraceIDs(t *testing.T) {
	t.Parallel()

	payload, format, err := decodeBody(
		[]byte(`[[{"trace_id":218523465776977553,"span_id":6933185253666401645}]]`),
		"/v0.4/traces",
		"",
		"application/json",
	)
	require.NoError(t, err)
	assert.Equal(t, "json", format)

	fields := flattenValueMap(payload)
	assert.Equal(t, "218523465776977553", fields["[0][0].trace_id"])
	assert.Equal(t, "6933185253666401645", fields["[0][0].span_id"])
}

func TestDecodeBodyMsgpack(t *testing.T) {
	t.Parallel()

	body, err := msgpack.Marshal(map[string]any{
		"resource": map[string]any{
			"correlation_id": "raw-dd-1",
		},
	})
	require.NoError(t, err)

	payload, format, err := decodeBody(body, "", "", "application/msgpack")
	require.NoError(t, err)
	assert.Equal(t, "msgpack", format)

	fields := flattenValueMap(payload)
	assert.Equal(t, "raw-dd-1", fields["resource.correlation_id"])
}

func TestCorrelationCandidatesPreferCorrelationIDPaths(t *testing.T) {
	t.Parallel()

	fields := map[string]string{
		"spans[0].meta.correlation_id":  "dd-span-1",
		"resource.correlation_id":       "resource-1",
		"resource.attributes.trace_id":  "trace-1",
		"series[0].metric":              "metric-name",
		"logs[0].attributes.service":    "checkout",
		"logs[0].attributes.trace_id":   "trace-2",
		"logs[0].attributes.otherthing": "value",
	}

	candidates := correlationCandidates(SignalUnknown, fields)
	require.NotEmpty(t, candidates)
	assert.Contains(t, []string{"resource.correlation_id", "correlation_id"}, candidates[0])
}

func TestCorrelationCandidatesTracesPreferTraceIDOverSpanIDOverCorrelationID(t *testing.T) {
	t.Parallel()

	fields := map[string]string{
		"span_id":        "span-111",
		"trace_id":       "trace-222",
		"correlation_id": "corr-333",
	}

	candidates := correlationCandidates(SignalTraces, fields)
	require.GreaterOrEqual(t, len(candidates), 3)
	assert.Equal(t, "trace_id", candidates[0])
	assert.Equal(t, "span_id", candidates[1])
	assert.Equal(t, "correlation_id", candidates[2])
}

func TestCorrelationCandidatesTracesUseSuffixFallback(t *testing.T) {
	t.Parallel()

	// When canonical keys are absent (no field mapping applied), suffix scans should
	// prefer trace_id over span_id: all spans share the same trace_id so it is
	// stable regardless of span ordering within the chunk.
	fields := map[string]string{
		"[0][0].span_id":             "span-111",
		"[0][0].trace_id":            "trace-222",
		"[0][0].meta.correlation_id": "corr-333",
	}

	candidates := correlationCandidates(SignalTraces, fields)
	require.GreaterOrEqual(t, len(candidates), 3)
	assert.Equal(t, "[0][0].trace_id", candidates[0])
	assert.Equal(t, "[0][0].span_id", candidates[1])
	assert.Equal(t, "[0][0].meta.correlation_id", candidates[2])
}

func TestResolveCorrelationIDFromDecodedTracesPreferTraceID(t *testing.T) {
	t.Parallel()

	fields := map[string]string{
		"span_id":        "span-abc",
		"trace_id":       "trace-def",
		"correlation_id": "corr-xyz",
	}

	// Even when the translator extracted a correlation_id, traces should prefer trace_id
	// because it is stable across span reordering within a trace group.
	res := resolveCorrelationIDFromDecoded(SignalTraces, "corr-xyz", fields, http.Header{}, nil)
	assert.Equal(t, "traces:trace-def", res.CorrelationID)
	assert.Equal(t, "field", res.Strategy)
	assert.Equal(t, "trace_id", res.Field)
}

func TestInferSignalFromDatadogPath(t *testing.T) {
	t.Parallel()

	cases := map[string]Signal{
		"/v0.4/traces":                SignalTraces,
		"/api/v1/series":              SignalMetrics,
		"/api/v2/logs":                SignalLogs,
		"/something/else":             SignalUnknown,
		"/api/v1/distribution_points": SignalMetrics,
	}

	for requestPath, want := range cases {
		assert.Equal(t, want, inferSignalFromDatadogPath(requestPath), "path=%s", requestPath)
	}
}

func TestParseExporterPath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		path           string
		wantExporter   string
		wantNormalized string
	}{
		{path: "/api/v2/logs", wantExporter: "", wantNormalized: "/api/v2/logs"},
		{path: "/exporter/datadog/api/v2/logs", wantExporter: "datadog", wantNormalized: "/api/v2/logs"},
		{path: "/exporter/datadog", wantExporter: "datadog", wantNormalized: "/"},
		{path: "/observe/exporter/mdai/sample/gateway/datadog/api/v0.2/traces", wantExporter: "datadog", wantNormalized: "/api/v0.2/traces"},
		{path: "/intake/exporter/mdai/sample/gateway/datadog/api/v0.2/traces", wantExporter: "intake", wantNormalized: "/exporter/mdai/sample/gateway/datadog/api/v0.2/traces"},
		{path: "/splunk/services/collector/event", wantExporter: "splunk", wantNormalized: "/services/collector/event"},
	}

	for _, tc := range cases {
		gotExporter, gotNormalized := parseExporterPath(tc.path)
		assert.Equal(t, tc.wantExporter, gotExporter, "path=%s", tc.path)
		assert.Equal(t, tc.wantNormalized, gotNormalized, "path=%s", tc.path)
	}
}

func TestTrimSyntheticSourcePath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		path       string
		source     string
		wantSignal string
		wantOK     bool
	}{
		{path: "/observe/exporter/traces", source: "exporter", wantSignal: "traces", wantOK: true},
		{path: "/observe/receiver/logs", source: "receiver", wantSignal: "logs", wantOK: true},
		{path: "/intake/exporter/metrics", source: "exporter", wantSignal: "", wantOK: false},
		{path: "/intake/receiver/traces", source: "receiver", wantSignal: "", wantOK: false},
		{path: "/api/v2/logs", source: "exporter", wantSignal: "", wantOK: false},
	}

	for _, tc := range cases {
		gotSignal, gotOK := trimSyntheticSourcePath(tc.path, tc.source)
		assert.Equal(t, tc.wantSignal, gotSignal, "path=%s", tc.path)
		assert.Equal(t, tc.wantOK, gotOK, "path=%s", tc.path)
	}
}

func TestSelectedHeaders(t *testing.T) {
	t.Parallel()

	header := http.Header{}
	header.Set("Content-Type", "application/msgpack")
	header.Set("Dd-Api-Key", "secret")
	header.Set("X-Request-ID", "req-1")
	header.Set("X-Unused", "ignored")

	got := selectedHeaders(header)
	assert.Equal(t, "application/msgpack", got["Content-Type"])
	assert.Equal(t, "secret", got["DD-API-KEY"])
	assert.Equal(t, "req-1", got["X-Request-ID"])
	_, ok := got["X-Unused"]
	assert.False(t, ok, "unexpected x-unused in %#v", got)
}

func TestSanitizedDebugPayloadRedactsDatadogAPIKey(t *testing.T) {
	t.Parallel()

	payload := &observedPayload{
		request: RequestSnapshot{
			Headers: map[string]string{
				"Content-Type": "application/json",
				"DD-API-KEY":   "secret",
				"DD_API_KEY":   "secret",
			},
		},
		flattened: map[string]string{
			"message":               "ok",
			"attributes.DD_API_KEY": "secret",
		},
	}

	got := sanitizedDebugPayloadFromObserved(payload)
	assert.Equal(t, "application/json", got.Request.Headers["Content-Type"])
	assert.Equal(t, "[REDACTED]", got.Request.Headers["DD-API-KEY"])
	assert.Equal(t, "[REDACTED]", got.Request.Headers["DD_API_KEY"])
	assert.Equal(t, "ok", got.Attributes["message"])
	assert.Equal(t, "[REDACTED]", got.Attributes["attributes.DD_API_KEY"])
}

func TestSanitizedDebugPayloadIncludesRawBodyOnlyForDecodeError(t *testing.T) {
	t.Parallel()

	decoded := &observedPayload{
		body:        []byte(`{"message":"ok"}`),
		decodeError: "",
	}
	assert.Empty(t, sanitizedDebugPayloadFromObserved(decoded).RawBody)

	mappingError := &observedPayload{
		body:        []byte(`{"message":"ok"}`),
		decodeError: "field mapping produced no canonical attributes",
	}
	assert.Empty(t, sanitizedDebugPayloadFromObserved(mappingError).RawBody)

	decodeError := &observedPayload{
		body:        []byte(`{"message":"ok","DD_API_KEY":"secret"}`),
		decodeError: "failed to decode payload: unsupported payload encoding",
	}
	assert.JSONEq(t, `{"message":"ok","DD_API_KEY":"[REDACTED]"}`, sanitizedDebugPayloadFromObserved(decodeError).RawBody)
}

func TestSanitizedComparisonResultRedactsSensitiveFields(t *testing.T) {
	t.Parallel()

	result := ComparisonResult{
		ReceiverFields:    map[string]string{"DD_API_KEY": "receiver-secret", "message": "ok"},
		ExporterFields:    map[string]string{"attributes.DD_API_KEY": "exporter-secret"},
		ReceiverRawFields: map[string]string{"resource[0].DD-API-KEY": "receiver-secret"},
		ExporterRawFields: map[string]string{"message": "ok"},
		Mismatched: []AttributeDelta{
			{Attribute: "DD_API_KEY", Receiver: "receiver-secret", Exporter: "exporter-secret"},
		},
		MissingIn: []MissingField{
			{Attribute: "DD_API_KEY", Side: "exporter", Value: "receiver-secret"},
		},
		ReceiverWire: RequestSnapshot{
			Headers: map[string]string{"DD-API-KEY": "receiver-secret", "Content-Type": "application/json"},
		},
		ExporterWire: RequestSnapshot{
			Headers: map[string]string{"DD_API_KEY": "exporter-secret", "Content-Type": "application/json"},
		},
	}

	got := sanitizedComparisonResult(result)
	assert.Equal(t, "[REDACTED]", got.ReceiverFields["DD_API_KEY"])
	assert.Equal(t, "[REDACTED]", got.ExporterFields["attributes.DD_API_KEY"])
	assert.Equal(t, "[REDACTED]", got.ReceiverRawFields["resource[0].DD-API-KEY"])
	assert.Equal(t, "ok", got.ExporterRawFields["message"])
	assert.Equal(t, "[REDACTED]", got.Mismatched[0].Receiver)
	assert.Equal(t, "[REDACTED]", got.Mismatched[0].Exporter)
	assert.Equal(t, "[REDACTED]", got.MissingIn[0].Value)
	assert.Equal(t, "[REDACTED]", got.ReceiverWire.Headers["DD-API-KEY"])
	assert.Equal(t, "[REDACTED]", got.ExporterWire.Headers["DD_API_KEY"])
	assert.Equal(t, "application/json", got.ReceiverWire.Headers["Content-Type"])
	assert.Equal(t, "application/json", got.ExporterWire.Headers["Content-Type"])
}

func TestIsSensitiveFieldName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		field     string
		sensitive bool
	}{
		{"DD_API_KEY", true},
		{"dd_api_key", true},
		{"DD-API-KEY", true},
		{"dd-api-key", true},
		{"Dd-Api-Key", true},
		{"attributes.DD_API_KEY", true},
		{"attributes.DD-API-KEY", true},
		{"resource[0].DD_API_KEY", true},
		{"resource[0].DD-API-KEY", true},
		{"Content-Type", false},
		{"message", false},
		{"X-Request-ID", false},
		{"service.name", false},
		{"DD_API_KEY_EXTRA", false},
		{"MY_DD_API_KEY", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.sensitive, isSensitiveFieldName(tt.field))
		})
	}
}

func TestSanitizedHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input map[string]string
		want  map[string]string
	}{
		{
			name:  "nil map returned as-is",
			input: nil,
			want:  nil,
		},
		{
			name:  "empty map returned as-is",
			input: map[string]string{},
			want:  map[string]string{},
		},
		{
			name:  "non-sensitive headers pass through unchanged",
			input: map[string]string{"Content-Type": "application/json", "X-Request-ID": "abc"},
			want:  map[string]string{"Content-Type": "application/json", "X-Request-ID": "abc"},
		},
		{
			name:  "sensitive header value masked, key preserved",
			input: map[string]string{"DD-API-KEY": "secret", "Content-Type": "application/json"},
			want:  map[string]string{"DD-API-KEY": "[REDACTED]", "Content-Type": "application/json"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, sanitizedHeaders(tt.input))
		})
	}
}

func TestSanitizedAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input map[string]string
		want  map[string]string
	}{
		{
			name:  "nil map returned as-is",
			input: nil,
			want:  nil,
		},
		{
			name:  "empty map returned as-is",
			input: map[string]string{},
			want:  map[string]string{},
		},
		{
			name:  "non-sensitive attributes pass through unchanged",
			input: map[string]string{"message": "hello", "service.name": "svc"},
			want:  map[string]string{"message": "hello", "service.name": "svc"},
		},
		{
			name:  "sensitive attribute value masked, key preserved",
			input: map[string]string{"attributes.DD_API_KEY": "secret", "message": "hello"},
			want:  map[string]string{"attributes.DD_API_KEY": "[REDACTED]", "message": "hello"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, sanitizedAttributes(tt.input))
		})
	}
}

func TestSanitizedAttributeDeltas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input []AttributeDelta
		want  []AttributeDelta
	}{
		{
			name:  "nil slice returned as-is",
			input: nil,
			want:  nil,
		},
		{
			name:  "empty slice returned as-is",
			input: []AttributeDelta{},
			want:  []AttributeDelta{},
		},
		{
			name:  "non-sensitive delta passes through unchanged",
			input: []AttributeDelta{{Attribute: "message", Receiver: "hello", Exporter: "world"}},
			want:  []AttributeDelta{{Attribute: "message", Receiver: "hello", Exporter: "world"}},
		},
		{
			name:  "sensitive delta values masked, attribute name preserved",
			input: []AttributeDelta{{Attribute: "DD_API_KEY", Receiver: "r-secret", Exporter: "e-secret"}},
			want:  []AttributeDelta{{Attribute: "DD_API_KEY", Receiver: "[REDACTED]", Exporter: "[REDACTED]"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, sanitizedAttributeDeltas(tt.input))
		})
	}
}

func TestSanitizedMissingFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input []MissingField
		want  []MissingField
	}{
		{
			name:  "nil slice returned as-is",
			input: nil,
			want:  nil,
		},
		{
			name:  "empty slice returned as-is",
			input: []MissingField{},
			want:  []MissingField{},
		},
		{
			name:  "non-sensitive field passes through unchanged",
			input: []MissingField{{Attribute: "message", Side: "exporter", Value: "hello"}},
			want:  []MissingField{{Attribute: "message", Side: "exporter", Value: "hello"}},
		},
		{
			name:  "sensitive field value masked, attribute and side preserved",
			input: []MissingField{{Attribute: "DD_API_KEY", Side: "receiver", Value: "secret"}},
			want:  []MissingField{{Attribute: "DD_API_KEY", Side: "receiver", Value: "[REDACTED]"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, sanitizedMissingFields(tt.input))
		})
	}
}

func TestSanitizedRawPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
		json  bool
	}{
		{
			name:  "non-JSON body returned as-is",
			input: "not json at all",
			want:  "[unparseable payload redacted]",
		},
		{
			name:  "JSON without sensitive fields passes through unchanged",
			input: `{"message":"ok","service":"svc"}`,
			want:  `{"message":"ok","service":"svc"}`,
			json:  true,
		},
		{
			name:  "top-level sensitive field masked",
			input: `{"message":"ok","DD_API_KEY":"secret"}`,
			want:  `{"message":"ok","DD_API_KEY":"[REDACTED]"}`,
			json:  true,
		},
		{
			name:  "nested sensitive field masked",
			input: `{"meta":{"DD_API_KEY":"secret"},"message":"ok"}`,
			want:  `{"meta":{"DD_API_KEY":"[REDACTED]"},"message":"ok"}`,
			json:  true,
		},
		{
			name:  "array of objects with sensitive field masked",
			input: `[{"DD_API_KEY":"secret","message":"ok"}]`,
			want:  `[{"DD_API_KEY":"[REDACTED]","message":"ok"}]`,
			json:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := sanitizedRawPayload(tt.input)
			if tt.json {
				assert.JSONEq(t, tt.want, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestShouldIncludeRawPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		decodeError string
		want        bool
	}{
		{"failed to decode payload", true},
		{"decode error occurred", true},
		{"DECODE: unsupported format", true},
		{"", false},
		{"field mapping produced no attributes", false},
		{"unsupported payload encoding", false},
	}
	for _, tt := range tests {
		t.Run(tt.decodeError, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, shouldIncludeRawPayload(&observedPayload{decodeError: tt.decodeError}))
		})
	}
}

func TestRecordMetricsUsesConnectionLabel(t *testing.T) {
	t.Parallel()

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

	assert.InDelta(t, float64(1), counterValue(t, svc.attributeEval.WithLabelValues("shadow-a", "traces", "trace_id", "pass")), 0.000001)
	assert.InDelta(t, float64(1), counterValue(t, svc.attributeEval.WithLabelValues("shadow-a", "traces", "span_id", "fail")), 0.000001)
	assert.InDelta(t, float64(1), counterValue(t, svc.attributeEval.WithLabelValues("shadow-a", "traces", "service.name", "fail")), 0.000001)
	assert.InDelta(t, float64(1), counterValue(t, svc.signalEval.WithLabelValues("shadow-a", "traces", "fail")), 0.000001)
	assert.InDelta(t, float64(1), counterValue(t, svc.requiredSig.WithLabelValues("shadow-a", "traces", "pass")), 0.000001)
	assert.InDelta(t, float64(1), counterValue(t, svc.requiredEval.WithLabelValues("shadow-a", "traces", "trace_id", "pass")), 0.000001)
	assert.InDelta(t, float64(1), counterValue(t, svc.requiredEval.WithLabelValues("shadow-a", "traces", "service.name", "fail")), 0.000001)

	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_attribute_checks_total", "shadow-a")
	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_signal_checks_total", "shadow-a")
	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_required_attribute_checks_total", "shadow-a")
	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_required_signal_checks_total", "shadow-a")
}

func TestCaptureRequestUsesConnectionLabelForReceivedMetric(t *testing.T) {
	t.Parallel()

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

	payloads, results, matched, err := svc.captureRequests(
		defaultPairID,
		defaultTranslatorID,
		"receiver",
		SignalTraces,
		":8126",
		"/v0.4/traces",
		req,
	)
	require.NoError(t, err)
	require.NotEmpty(t, payloads)
	assert.Nil(t, results[0])
	assert.False(t, matched)
	assert.InDelta(t, float64(1), counterValue(t, svc.receivedTotal.WithLabelValues("shadow-b", "receiver", "traces")), 0.000001)

	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_payloads_received_total", "shadow-b")
}

func TestAdjustPendingTotalUsesConnectionLabel(t *testing.T) {
	t.Parallel()

	svc, registry := newMetricsTestService("shadow-c")
	svc.stateMu.Lock()
	svc.adjustPendingTotal(2)
	svc.stateMu.Unlock()

	assert.InDelta(t, float64(2), gaugeValue(t, svc.pendingGauge.WithLabelValues("shadow-c")), 0.000001)

	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_pending_payloads", "shadow-c")
}

func TestObserveDoesNotOverwriteNewerResult(t *testing.T) {
	t.Parallel()

	svc, _ := newMetricsTestService("shadow-results")
	svc.retention = time.Minute
	correlationID := "logs:corr-1"
	newer := ComparisonResult{
		CorrelationID: correlationID,
		ComparedAt:    time.Now().UTC().Add(time.Hour),
		Passed:        true,
	}

	svc.stateMu.Lock()
	svc.pending[correlationID] = &observedPayload{
		source:      "receiver",
		signal:      SignalLogs,
		correlation: correlationID,
		receivedAt:  time.Now().UTC(),
		flattened:   map[string]string{"message": "old"},
	}
	svc.lastResults[correlationID] = newer
	svc.stateMu.Unlock()
	svc.pendingTotal.Store(1)
	svc.pendingGauge.WithLabelValues("shadow-results").Set(1)

	result, matched := svc.observe(&observedPayload{
		source:      "exporter",
		signal:      SignalLogs,
		correlation: correlationID,
		receivedAt:  time.Now().UTC(),
		flattened:   map[string]string{"message": "new"},
	})

	require.True(t, matched)
	require.NotNil(t, result)

	svc.stateMu.RLock()
	got := svc.lastResults[correlationID]
	svc.stateMu.RUnlock()
	assert.True(t, got.Passed)
	assert.Equal(t, newer.ComparedAt, got.ComparedAt)
}

func TestExtractIndexedGroupsTraces(t *testing.T) {
	t.Parallel()

	flattened := map[string]string{
		"traces[0][0].trace_id": "trace-aaa",
		"traces[0][0].span_id":  "span-111",
		"traces[0][1].trace_id": "trace-aaa",
		"traces[0][1].span_id":  "span-222",
		"traces[1][0].trace_id": "trace-bbb",
		"traces[1][0].span_id":  "span-333",
	}

	groups := extractIndexedGroups(flattened, "traces")
	require.Len(t, groups, 2)

	assert.Equal(t, "trace-aaa", groups[0]["[0].trace_id"])
	assert.Equal(t, "span-111", groups[0]["[0].span_id"])
	assert.Equal(t, "trace-aaa", groups[0]["[1].trace_id"])
	assert.Equal(t, "span-222", groups[0]["[1].span_id"])

	assert.Equal(t, "trace-bbb", groups[1]["[0].trace_id"])
	assert.Equal(t, "span-333", groups[1]["[0].span_id"])
}

func TestExtractIndexedGroupsMetrics(t *testing.T) {
	t.Parallel()

	flattened := map[string]string{
		"series[0].metric":    "cpu.usage",
		"series[0].points[0]": "42",
		"series[1].metric":    "mem.usage",
		"series[1].points[0]": "1024",
	}

	groups := extractIndexedGroups(flattened, "series")
	require.Len(t, groups, 2)
	assert.Equal(t, "cpu.usage", groups[0]["metric"])
	assert.Equal(t, "mem.usage", groups[1]["metric"])
}

func TestExtractIndexedGroupsNoMatchReturnsOriginal(t *testing.T) {
	t.Parallel()

	flattened := map[string]string{
		"trace_id": "abc",
		"span_id":  "def",
	}

	groups := extractIndexedGroups(flattened, "traces")
	require.Len(t, groups, 1)
	assert.Equal(t, flattened, groups[0])
}

// TestDecodeAllTracesUnwrappedFormat verifies that /v0.4/traces payloads (root-level [[span,...]]
// with no "traces" wrapper) are correctly split into per-trace groups and that individual span
// maps have bare field names (e.g. "span_id", "name") suitable for span matching.
func TestDecodeAllTracesUnwrappedFormat(t *testing.T) {
	t.Parallel()

	// Two trace groups: trace-aaa has 2 spans, trace-bbb has 1 span.
	// Flat keys follow the unwrapped [[span,...]] structure.
	rawFlat := map[string]string{
		"[0][0].trace_id": "trace-aaa",
		"[0][0].span_id":  "span-111",
		"[0][0].name":     "web.request",
		"[0][1].trace_id": "trace-aaa",
		"[0][1].span_id":  "span-222",
		"[0][1].name":     "db.query",
		"[1][0].trace_id": "trace-bbb",
		"[1][0].span_id":  "span-333",
		"[1][0].name":     "background.job",
	}

	// hasKeyPrefix should detect no "traces[" key and choose root-level grouping.
	assert.False(t, hasKeyPrefix(rawFlat, "traces["))

	groups := extractIndexedGroups(rawFlat, "")
	require.Len(t, groups, 2)

	// Each group should contain [j].field keys for its spans.
	assert.Equal(t, "trace-aaa", groups[0]["[0].trace_id"])
	assert.Equal(t, "span-111", groups[0]["[0].span_id"])
	assert.Equal(t, "trace-aaa", groups[0]["[1].trace_id"])
	assert.Equal(t, "span-222", groups[0]["[1].span_id"])

	// newTraceGroup should then yield bare field names per span.
	spans := newTraceGroup(groups[0]).spans
	require.Len(t, spans, 2)
	assert.Equal(t, "span-111", spans[0]["span_id"])
	assert.Equal(t, "web.request", spans[0]["name"])
	assert.Equal(t, "span-222", spans[1]["span_id"])
	assert.Equal(t, "db.query", spans[1]["name"])
}

func TestCaptureRequestsBatchSplitMatchesIndependently(t *testing.T) {
	t.Parallel()

	// Receiver sends one batch with two trace groups; exporter sends each trace separately.
	// Both should match against their respective receiver group.
	svc, _ := newMetricsTestService("batch-split")
	svc.retention = time.Minute

	// Receiver: one HTTP request carrying two trace groups (canonical attributes, already field-mapped).
	svc.translators[defaultTranslatorID] = staticTranslator{
		name: defaultTranslatorID,
		batch: []DecodedPayload{
			{Signal: SignalTraces, Attributes: map[string]string{"trace_id": "trace-aaa", "span_id": "span-111"}, Format: "json"},
			{Signal: SignalTraces, Attributes: map[string]string{"trace_id": "trace-bbb", "span_id": "span-333"}, Format: "json"},
		},
	}

	recvReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v0.4/traces", strings.NewReader("{}"))
	payloads, results, anyMatched, err := svc.captureRequests(defaultPairID, defaultTranslatorID, "receiver", SignalTraces, ":8126", "/v0.4/traces", recvReq)
	require.NoError(t, err)
	require.Len(t, payloads, 2)
	assert.False(t, anyMatched)
	assert.Nil(t, results[0])
	assert.Nil(t, results[1])

	// Exporter sends trace-aaa alone (batch split: this is the first of two exporter requests).
	svc.translators[defaultTranslatorID] = staticTranslator{
		name: defaultTranslatorID,
		batch: []DecodedPayload{
			{Signal: SignalTraces, Attributes: map[string]string{"trace_id": "trace-aaa", "span_id": "span-111"}, Format: "json"},
		},
	}
	expReqA := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v0.4/traces", strings.NewReader("{}"))
	pA, rA, matchedA, err := svc.captureRequests(defaultPairID, defaultTranslatorID, "exporter", SignalTraces, ":18081", "/v0.4/traces", expReqA)
	require.NoError(t, err)
	require.Len(t, pA, 1)
	assert.True(t, matchedA, "trace-aaa should match receiver group 0")
	require.NotNil(t, rA[0])

	// Exporter sends trace-bbb alone (second exporter request from same original batch).
	svc.translators[defaultTranslatorID] = staticTranslator{
		name: defaultTranslatorID,
		batch: []DecodedPayload{
			{Signal: SignalTraces, Attributes: map[string]string{"trace_id": "trace-bbb", "span_id": "span-333"}, Format: "json"},
		},
	}
	expReqB := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v0.4/traces", strings.NewReader("{}"))
	pB, rB, matchedB, err := svc.captureRequests(defaultPairID, defaultTranslatorID, "exporter", SignalTraces, ":18081", "/v0.4/traces", expReqB)
	require.NoError(t, err)
	require.Len(t, pB, 1)
	assert.True(t, matchedB, "trace-bbb should match receiver group 1")
	require.NotNil(t, rB[0])
}

func TestExtractIndexedGroupsLogsRootArray(t *testing.T) {
	t.Parallel()

	// Datadog log payloads are root-level JSON arrays; flat keys are [i].field.
	flattened := map[string]string{
		"[0].message":   "log entry A",
		"[0].timestamp": "2025-01-01T00:00:00Z",
		"[0].hostname":  "host-1",
		"[1].message":   "log entry B",
		"[1].timestamp": "2025-01-01T00:00:01Z",
		"[1].hostname":  "host-2",
	}

	groups := extractIndexedGroups(flattened, "")
	require.Len(t, groups, 2)
	assert.Equal(t, "log entry A", groups[0]["message"])
	assert.Equal(t, "host-1", groups[0]["hostname"])
	assert.Equal(t, "log entry B", groups[1]["message"])
	assert.Equal(t, "host-2", groups[1]["hostname"])
}

func TestCaptureRequestsLogBatchMergeMatchesPerCorrelationGroup(t *testing.T) {
	t.Parallel()

	// Receiver gets two separate log requests (two envoy UUIDs, no per-entry correlation_id
	// in the log content itself — the receiver uses the header strategy).
	// Exporter merges them into one request, with each entry carrying a correlation_id field.
	// After grouping by per-entry correlation_id the exporter split matches each receiver batch.
	svc, _ := newMetricsTestService("log-merge")
	svc.retention = time.Minute

	// Receiver batch 1: one payload (no per-entry correlation_id → kept as one batch).
	svc.translators[defaultTranslatorID] = staticTranslator{
		name: defaultTranslatorID,
		batch: []DecodedPayload{
			{Signal: SignalLogs, Attributes: map[string]string{"message": "msg-A", "hostname": "h1"}, Format: "json"},
		},
	}
	recv1Req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v2/logs", strings.NewReader("{}"))
	recv1Req.Header.Set("X-Correlation-ID", "uuid-1")
	p1, _, matched1, err := svc.captureRequests(defaultPairID, defaultTranslatorID, "receiver", SignalLogs, ":8126", "/api/v2/logs", recv1Req)
	require.NoError(t, err)
	require.Len(t, p1, 1)
	assert.Equal(t, "logs:uuid-1", p1[0].correlation)
	assert.False(t, matched1)

	// Receiver batch 2: one payload.
	svc.translators[defaultTranslatorID] = staticTranslator{
		name: defaultTranslatorID,
		batch: []DecodedPayload{
			{Signal: SignalLogs, Attributes: map[string]string{"message": "msg-B", "hostname": "h2"}, Format: "json"},
		},
	}
	recv2Req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v2/logs", strings.NewReader("{}"))
	recv2Req.Header.Set("X-Correlation-ID", "uuid-2")
	p2, _, matched2, err := svc.captureRequests(defaultPairID, defaultTranslatorID, "receiver", SignalLogs, ":8126", "/api/v2/logs", recv2Req)
	require.NoError(t, err)
	require.Len(t, p2, 1)
	assert.Equal(t, "logs:uuid-2", p2[0].correlation)
	assert.False(t, matched2)

	// Exporter sends merged batch: each entry carries its original correlation_id so the
	// translator strategy produces the right per-group correlation.
	svc.translators[defaultTranslatorID] = staticTranslator{
		name: defaultTranslatorID,
		batch: []DecodedPayload{
			{Signal: SignalLogs, CorrelationID: "uuid-1", Attributes: map[string]string{"message": "msg-A", "correlation_id": "uuid-1", "hostname": "h1"}, Format: "json"},
			{Signal: SignalLogs, CorrelationID: "uuid-2", Attributes: map[string]string{"message": "msg-B", "correlation_id": "uuid-2", "hostname": "h2"}, Format: "json"},
		},
	}
	expReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v2/logs", strings.NewReader("{}"))
	expPayloads, expResults, anyMatched, err := svc.captureRequests(defaultPairID, defaultTranslatorID, "exporter", SignalLogs, ":18081", "/api/v2/logs", expReq)
	require.NoError(t, err)
	require.Len(t, expPayloads, 2)
	assert.True(t, anyMatched, "both exporter groups should match their receiver batches")
	require.NotNil(t, expResults[0])
	require.NotNil(t, expResults[1])
	assert.True(t, expResults[0].FullPayloadPassed, "uuid-1 group should pass")
	assert.True(t, expResults[1].FullPayloadPassed, "uuid-2 group should pass")
}

func TestObserveDropsExpiredPendingWithoutFullShardGC(t *testing.T) {
	t.Parallel()

	svc, _ := newMetricsTestService("shadow-expired")
	svc.retention = time.Second

	correlationID := "metrics:corr-1"
	svc.stateMu.Lock()
	svc.pending[correlationID] = &observedPayload{
		source:      "receiver",
		signal:      SignalMetrics,
		correlation: correlationID,
		receivedAt:  time.Now().Add(-2 * time.Second),
		flattened:   map[string]string{"metric_name": "stale"},
	}
	svc.stateMu.Unlock()
	svc.pendingTotal.Store(1)
	svc.pendingGauge.WithLabelValues("shadow-expired").Set(1)

	payload := &observedPayload{
		source:      "exporter",
		signal:      SignalMetrics,
		correlation: correlationID,
		receivedAt:  time.Now(),
		flattened:   map[string]string{"metric_name": "fresh"},
	}

	result, matched := svc.observe(payload)
	assert.False(t, matched)
	assert.Nil(t, result)
	assert.Equal(t, int64(1), svc.pendingTotal.Load())
	assert.InDelta(t, float64(1), gaugeValue(t, svc.pendingGauge.WithLabelValues("shadow-expired")), 0.000001)
}

func TestGCExpiredStateRemovesExpiredPendingAndUpdatesGauge(t *testing.T) {
	t.Parallel()

	svc, _ := newMetricsTestService("shadow-gc")
	svc.retention = time.Second

	correlationID := "logs:corr-1"
	svc.stateMu.Lock()
	svc.pending[correlationID] = &observedPayload{
		source:      "receiver",
		signal:      SignalLogs,
		correlation: correlationID,
		receivedAt:  time.Now().Add(-2 * time.Second),
	}
	svc.stateMu.Unlock()
	svc.pendingTotal.Store(1)
	svc.pendingGauge.WithLabelValues("shadow-gc").Set(1)

	svc.gcExpiredState(time.Now())

	svc.stateMu.RLock()
	assert.Empty(t, svc.pending)
	svc.stateMu.RUnlock()
	assert.Equal(t, int64(0), svc.pendingTotal.Load())
	assert.InDelta(t, float64(0), gaugeValue(t, svc.pendingGauge.WithLabelValues("shadow-gc")), 0.000001)
}

func TestResolveCorrelationIDFromHeaderFallbacks(t *testing.T) {
	t.Parallel()

	fields := map[string]string{}
	body := []byte(`{"message":"hello"}`)

	t.Run("x-request-id", func(t *testing.T) {
		t.Parallel()
		headers := http.Header{}
		headers.Set("X-Request-ID", "req-123")
		got := resolveCorrelationID(SignalLogs, fields, headers, body)
		assert.Equal(t, "logs:req-123", got.CorrelationID)
		assert.Equal(t, "header", got.Strategy)
		assert.Equal(t, "X-Request-ID", got.Field)
	})
}

func TestResolveCorrelationIDPrefersHeaderOverField(t *testing.T) {
	t.Parallel()

	fields := map[string]string{
		"correlation_id": "from-field",
	}
	headers := http.Header{}
	headers.Set("X-Correlation-ID", "from-header")

	got := resolveCorrelationID(SignalLogs, fields, headers, []byte(`{"message":"hello"}`))
	assert.Equal(t, "logs:from-header", got.CorrelationID)
	assert.Equal(t, "header", got.Strategy)
	assert.Equal(t, "X-Correlation-ID", got.Field)
}

func TestDeriveCorrelationFromMetricTagsBeforeMetricName(t *testing.T) {
	t.Parallel()

	fields := map[string]string{
		"series[0].metric":  "ddgen.checkout.duration",
		"series[0].tags[0]": "service:checkout",
		"series[0].tags[1]": "correlation_id:corr-123",
	}

	got := deriveCorrelationFromFields("metrics", fields)
	assert.Equal(t, "metrics:corr-123", got)
}

func TestDeriveCorrelationFromBatchExplodedMetricTags(t *testing.T) {
	t.Parallel()

	// After extractIndexedGroups strips the "series[i]" prefix, keys are top-level:
	// "tags[j]" not "series[0].tags[j]". Correlation extraction must still work.
	fields := map[string]string{
		"metric":  "ddgen.checkout.duration",
		"tags[0]": "service:checkout",
		"tags[1]": "env:dev",
		"tags[2]": "correlation_id:corr-456",
		"type":    "gauge",
	}

	got := deriveCorrelationFromFields("metrics", fields)
	assert.Equal(t, "metrics:corr-456", got)
}

func TestDeriveCorrelationFromLogDDTags(t *testing.T) {
	t.Parallel()

	fields := map[string]string{
		"[0].ddtags": "env:dev,correlation_id:corr-log-1",
	}

	got := deriveCorrelationFromFields("logs", fields)
	assert.Equal(t, "logs:corr-log-1", got)
}

func TestComparePairNormalizesSingleLogArrayPrefix(t *testing.T) {
	t.Parallel()

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
	assert.True(t, result.FullPayloadPassed, "expected full payload pass, got %#v", result)
	assert.Len(t, result.Matched, 2)
}

func TestComparePairStripsCorrelationFromLogDDTags(t *testing.T) {
	t.Parallel()

	receiver := &observedPayload{
		source:      "receiver",
		signal:      "logs",
		correlation: "logs:corr-1",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"ddtags": "env:dev,correlation_id:corr-a,otel_source:datadog_exporter",
		},
	}
	exporter := &observedPayload{
		source:      "exporter",
		signal:      "logs",
		correlation: "logs:corr-1",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"[0].ddtags": "env:dev,correlation_id:corr-b,otel_source:datadog_exporter",
		},
	}

	result := comparePair(receiver, exporter, Policy{})
	assert.True(t, result.FullPayloadPassed, "expected full payload pass, got %#v", result)
}

func TestComparePairStripsCorrelationFromLogMessageJSON(t *testing.T) {
	t.Parallel()

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
	assert.True(t, result.FullPayloadPassed, "expected full payload pass, got %#v", result)
}

func TestSortSpansByID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rawGroup map[string]string
		want     map[string]string
	}{
		{
			name: "sorts numeric span IDs ascending",
			rawGroup: map[string]string{
				"[0].span_id": "300",
				"[0].name":    "third",
				"[1].span_id": "100",
				"[1].name":    "first",
				"[2].span_id": "200",
				"[2].name":    "second",
			},
			want: map[string]string{
				"[0].span_id": "100",
				"[0].name":    "first",
				"[1].span_id": "200",
				"[1].name":    "second",
				"[2].span_id": "300",
				"[2].name":    "third",
			},
		},
		{
			name: "preserves non-indexed keys",
			rawGroup: map[string]string{
				"[0].span_id":     "200",
				"[1].span_id":     "100",
				"top-level-value": "kept",
			},
			want: map[string]string{
				"[0].span_id":     "100",
				"[1].span_id":     "200",
				"top-level-value": "kept",
			},
		},
		{
			name: "sorts numeric IDs before non-numeric IDs",
			rawGroup: map[string]string{
				"[0].span_id": "10abc",
				"[0].name":    "non-numeric",
				"[1].span_id": "9",
				"[1].name":    "numeric-nine",
				"[2].span_id": "10",
				"[2].name":    "numeric-ten",
			},
			want: map[string]string{
				"[0].span_id": "9",
				"[0].name":    "numeric-nine",
				"[1].span_id": "10",
				"[1].name":    "numeric-ten",
				"[2].span_id": "10abc",
				"[2].name":    "non-numeric",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := sortSpansByID(tt.rawGroup)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestComparePairReorderedSpansNoTopLevelMismatch(t *testing.T) {
	setDefaultFieldMappingPath(t)
	mapping, _, err := loadFieldMapping()
	require.NoError(t, err)
	translator := datadogRawTranslator{mapping: newMappingStore(mapping)}

	receiverDecoded := translator.Decode(SignalTraces, "/api/v0.4/traces", "", "application/json", []byte(`[
		[
			{"trace_id":"t1","span_id":"100","name":"root","service":"svc","meta":{"correlation_id":"traces:t1"}},
			{"trace_id":"t1","span_id":"200","name":"child","service":"svc","meta":{"correlation_id":"traces:t1"}}
		]
	]`))
	exporterDecoded := translator.Decode(SignalTraces, "/api/v0.4/traces", "", "application/json", []byte(`[
		[
			{"trace_id":"t1","span_id":"200","name":"child","service":"svc","meta":{"correlation_id":"traces:t1"}},
			{"trace_id":"t1","span_id":"100","name":"root","service":"svc","meta":{"correlation_id":"traces:t1"}}
		]
	]`))
	require.Empty(t, receiverDecoded.DecodeError)
	require.Empty(t, exporterDecoded.DecodeError)
	assert.Equal(t, "100", receiverDecoded.Attributes["span_id"])
	assert.Equal(t, "100", exporterDecoded.Attributes["span_id"])
	assert.Equal(t, "root", receiverDecoded.Attributes["operation"])
	assert.Equal(t, "root", exporterDecoded.Attributes["operation"])

	receiver := &observedPayload{
		source:      "receiver",
		signal:      receiverDecoded.Signal,
		correlation: receiverDecoded.CorrelationID,
		receivedAt:  time.Now(),
		flattened:   receiverDecoded.Attributes,
		rawGroup:    receiverDecoded.RawGroup,
		spans:       receiverDecoded.Spans,
	}
	exporter := &observedPayload{
		source:      "exporter",
		signal:      exporterDecoded.Signal,
		correlation: exporterDecoded.CorrelationID,
		receivedAt:  time.Now(),
		flattened:   exporterDecoded.Attributes,
		rawGroup:    exporterDecoded.RawGroup,
		spans:       exporterDecoded.Spans,
	}

	result := comparePair(receiver, exporter, Policy{})

	assert.True(t, result.FullPayloadPassed, "span reordering should not cause top-level mismatches; got: %v", result.Mismatched)
	assert.Empty(t, result.Mismatched)
	require.Len(t, result.Spans, 2)
	for _, s := range result.Spans {
		assert.True(t, s.Passed, "span %s should pass", s.SpanID)
		assert.Empty(t, s.OnlyIn)
	}
}

func TestComparePairSpanMissingInExporterFailsFullPayloadPassed(t *testing.T) {
	t.Parallel()

	receiver := &observedPayload{
		source:      "receiver",
		signal:      "traces",
		correlation: "abc",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"trace_id": "t1",
			"span_id":  "s1",
		},
		rawGroup: map[string]string{
			"[0].span_id": "s1",
			"[0].name":    "root",
			"[1].span_id": "s2",
			"[1].name":    "child",
		},
		spans: []map[string]string{
			{"span_id": "s1", "name": "root"},
			{"span_id": "s2", "name": "child"},
		},
	}
	exporter := &observedPayload{
		source:      "exporter",
		signal:      "traces",
		correlation: "abc",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"trace_id": "t1",
			"span_id":  "s1",
		},
		rawGroup: map[string]string{
			"[0].span_id": "s1",
			"[0].name":    "root",
		},
		spans: []map[string]string{
			{"span_id": "s1", "name": "root"},
		},
	}

	result := comparePair(receiver, exporter, Policy{})

	assert.False(t, result.FullPayloadPassed)
	require.Len(t, result.Spans, 2)
	assert.Empty(t, result.Spans[0].OnlyIn)
	assert.True(t, result.Spans[0].Passed)
	assert.Equal(t, "receiver", result.Spans[1].OnlyIn)
}

func TestComparePairIgnoresCorrelationIDField(t *testing.T) {
	t.Parallel()

	receiver := &observedPayload{
		source:      "receiver",
		signal:      "metrics",
		correlation: "metrics:corr-1",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"metric_name":    "ddgen.checkout.duration",
			"point_value":    "123.45",
			"correlation_id": "corr-a",
		},
	}
	exporter := &observedPayload{
		source:      "exporter",
		signal:      "metrics",
		correlation: "metrics:corr-1",
		receivedAt:  time.Now(),
		flattened: map[string]string{
			"metric_name":    "ddgen.checkout.duration",
			"point_value":    "123.45",
			"correlation_id": "corr-b",
		},
	}

	result := comparePair(receiver, exporter, Policy{})
	assert.True(t, result.FullPayloadPassed, "expected correlation_id to be ignored, got %#v", result)
	assert.True(t, result.Passed, "expected empty policy to pass, got %#v", result)
	assert.Equal(t, []string{"metric_name", "point_value"}, result.Matched)
	assert.Empty(t, result.Mismatched)
	assert.Empty(t, result.MissingIn)
	assert.Equal(t, 2, result.AttributeTotal)
	assert.Equal(t, "corr-a", result.ReceiverRawFields["correlation_id"])
	assert.Equal(t, "corr-b", result.ExporterRawFields["correlation_id"])
}

func TestResolvePairForRequest(t *testing.T) {
	t.Parallel()

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
	assert.Equal(t, defaultPairID, gotDefault.ID)

	req.Header.Set("X-Forwarded-Port", "18126")
	gotPortMapped := svc.resolvePairForRequest(req, "receiver", ":8126")
	assert.Equal(t, "shadow-a", gotPortMapped.ID)

	req.Header.Set(pairHeaderKey, "shadow-a")
	gotNamed := svc.resolvePairForRequest(req, "receiver", ":8126")
	assert.Equal(t, "shadow-a", gotNamed.ID)
}

func TestHandleAdminPairs(t *testing.T) {
	t.Parallel()

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

	body := `{"id":"shadow-b","receiver_translator":"datadog_raw","exporter_translator":"datadog_raw","default":true}`
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/pairs", strings.NewReader(body))
	rec := httptest.NewRecorder()

	svc.handleAdminPairs(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code, "body=%s", rec.Body.String())

	_, ok := svc.pairs["shadow-b"]
	require.True(t, ok, "expected pair shadow-b to be saved")
	assert.Equal(t, "shadow-b", svc.defaultPair)

	body = `{"id":"shadow-c","receiver_translator":"datadog_raw","exporter_translator":"datadog_raw","receiver_ports":["18126"],"exporter_ports":["18081"]}`
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/pairs", strings.NewReader(body))
	rec = httptest.NewRecorder()
	svc.handleAdminPairs(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code, "body=%s", rec.Body.String())
	assert.Equal(t, "shadow-c", svc.receiverPairByPort["18126"])
	assert.Equal(t, "shadow-c", svc.exporterPairByPort["18081"])

	body = `{"id":"shadow-d","receiver_translator":"datadog_raw","exporter_translator":"datadog_raw","exporter_ignore_paths":["/api/beta/sketches","api/v2/sketches"]}`
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/pairs", strings.NewReader(body))
	rec = httptest.NewRecorder()
	svc.handleAdminPairs(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code, "body=%s", rec.Body.String())
	assert.True(t, reflect.DeepEqual(svc.pairs["shadow-d"].ExporterIgnorePaths, []string{"/api/beta/sketches", "/api/v2/sketches"}), "unexpected exporter ignore paths: %#v", svc.pairs["shadow-d"].ExporterIgnorePaths)
}

func TestNormalizePathPatternList(t *testing.T) {
	t.Parallel()

	got, err := normalizePathPatternList([]string{" api/beta/sketches ", "/api/beta/*", "/api/beta/sketches"})
	require.NoError(t, err)
	want := []string{"/api/beta/*", "/api/beta/sketches"}
	assert.True(t, reflect.DeepEqual(got, want), "normalizePathPatternList() = %#v want %#v", got, want)
}

func TestConfiguredPairShouldIgnorePath(t *testing.T) {
	t.Parallel()

	pair := configuredPair{
		PairConfig: PairConfig{
			ID:                  "shadow-e",
			ExporterIgnorePaths: []string{"/api/beta/sketches", "/api/v2/*"},
			ReceiverIgnorePaths: []string{"/v0.4/traces"},
		},
	}

	assert.True(t, pair.shouldIgnorePath("exporter", "/exporter/datadog/api/beta/sketches"))
	assert.True(t, pair.shouldIgnorePath("exporter", "/api/v2/series"))
	assert.True(t, pair.shouldIgnorePath("receiver", "/v0.4/traces"))
	assert.False(t, pair.shouldIgnorePath("exporter", "/exporter/datadog/api/v1/validate"))
}

func TestDefaultPairIgnoresAgentHousekeeping(t *testing.T) {
	t.Parallel()

	pair := configuredPair{
		PairConfig: PairConfig{
			ID:                 defaultPairID,
			ReceiverTranslator: defaultTranslatorID,
			ExporterTranslator: defaultTranslatorID,
			ReceiverIgnorePaths: []string{
				"/api/v0.2/stats",
				"/api/v1/metadata",
				"/api/beta/sketches",
				"/support/flare",
				"/intake/",
			},
		},
	}

	ignored := []string{
		"/api/v0.2/stats",
		"/api/v1/metadata",
		"/api/beta/sketches",
		"/support/flare",
		"/intake/",
	}
	for _, p := range ignored {
		assert.True(t, pair.shouldIgnorePath("receiver", p), "should ignore %s", p)
	}

	kept := []string{
		"/api/v2/logs",
		"/v0.4/traces",
		"/api/v2/series",
		"/api/v1/series",
		"/api/v0.2/traces",
	}
	for _, p := range kept {
		assert.False(t, pair.shouldIgnorePath("receiver", p), "should not ignore %s", p)
	}

	// Ignore list applies to the receiver side only.
	assert.False(t, pair.shouldIgnorePath("exporter", "/api/v0.2/stats"))
}

func TestHandleExporterAPIIgnoresConfiguredPath(t *testing.T) {
	t.Parallel()

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
	require.Equal(t, http.StatusAccepted, rec.Code, "body=%s", rec.Body.String())
	_, ok := svc.lastBySource.Load("exporter")
	assert.False(t, ok, "did not expect ignored exporter payload to be captured")
}

func TestAdminAndMetricsRoutesAreSplit(t *testing.T) {
	t.Parallel()

	svc := &Service{}

	adminReq := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", http.NoBody)
	adminRec := httptest.NewRecorder()
	svc.AdminRoutes().ServeHTTP(adminRec, adminReq)
	assert.Equal(t, http.StatusNotFound, adminRec.Code)

	metricsReq := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", http.NoBody)
	metricsRec := httptest.NewRecorder()
	svc.MetricsRoutes().ServeHTTP(metricsRec, metricsReq)
	assert.Equal(t, http.StatusOK, metricsRec.Code)
}
