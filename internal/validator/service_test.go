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

	candidates := correlationCandidates(fields)
	require.NotEmpty(t, candidates)
	assert.Contains(t, []string{"resource.correlation_id", "correlation_id"}, candidates[0])
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
	assert.NotContains(t, got.Request.Headers, "DD-API-KEY")
	assert.NotContains(t, got.Request.Headers, "DD_API_KEY")
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
	assert.NotContains(t, got.ReceiverWire.Headers, "DD-API-KEY")
	assert.NotContains(t, got.ExporterWire.Headers, "DD_API_KEY")
	assert.Equal(t, "application/json", got.ReceiverWire.Headers["Content-Type"])
	assert.Equal(t, "application/json", got.ExporterWire.Headers["Content-Type"])
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

	observed, result, matched, err := svc.captureRequest(
		defaultPairID,
		defaultTranslatorID,
		"receiver",
		SignalTraces,
		":8126",
		"/v0.4/traces",
		req,
	)
	require.NoError(t, err)
	require.NotNil(t, observed)
	assert.Nil(t, result)
	assert.False(t, matched)
	assert.InDelta(t, float64(1), counterValue(t, svc.receivedTotal.WithLabelValues("shadow-b", "receiver", "traces")), 0.000001)

	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_payloads_received_total", "shadow-b")
}

func TestAdjustPendingTotalUsesConnectionLabel(t *testing.T) {
	t.Parallel()

	svc, registry := newMetricsTestService("shadow-c")
	svc.adjustPendingTotal(2)

	assert.InDelta(t, float64(2), gaugeValue(t, svc.pendingGauge.WithLabelValues("shadow-c")), 0.000001)

	requireMetricHasConnectionLabel(t, registry, "mdai_fidelity_pending_payloads", "shadow-c")
}

func TestObserveDropsExpiredPendingWithoutFullShardGC(t *testing.T) {
	t.Parallel()

	svc, _ := newMetricsTestService("shadow-expired")
	svc.retention = time.Second

	correlationID := "metrics:corr-1"
	sh := svc.getShard(correlationID)
	sh.pending[correlationID] = &observedPayload{
		source:      "receiver",
		signal:      SignalMetrics,
		correlation: correlationID,
		receivedAt:  time.Now().Add(-2 * time.Second),
		flattened:   map[string]string{"metric_name": "stale"},
	}
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

func TestGCExpiredShardStateRemovesExpiredPendingAndUpdatesGauge(t *testing.T) {
	t.Parallel()

	svc, _ := newMetricsTestService("shadow-gc")
	svc.retention = time.Second

	correlationID := "logs:corr-1"
	sh := svc.getShard(correlationID)
	sh.pending[correlationID] = &observedPayload{
		source:      "receiver",
		signal:      SignalLogs,
		correlation: correlationID,
		receivedAt:  time.Now().Add(-2 * time.Second),
	}
	svc.pendingTotal.Store(1)
	svc.pendingGauge.WithLabelValues("shadow-gc").Set(1)

	svc.gcExpiredShardState(time.Now())

	assert.Empty(t, sh.pending)
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
