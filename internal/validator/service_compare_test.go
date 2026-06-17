package validator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComparePair(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		receiver         *observedPayload
		exporter         *observedPayload
		wantFullPassed   bool
		wantPassed       bool
		wantMatchedLen   int
		wantMismatchLen  int
		wantMissingInLen int
	}{
		{
			name: "passes when fields match",
			receiver: &observedPayload{
				source: "receiver", signal: "traces", correlation: "abc", receivedAt: time.Now(),
				flattened: map[string]string{"trace_id": "123", "span_id": "456"},
			},
			exporter: &observedPayload{
				source: "exporter", signal: "traces", correlation: "abc", receivedAt: time.Now(),
				flattened: map[string]string{"trace_id": "123", "span_id": "456"},
			},
			wantFullPassed: true,
			wantPassed:     true,
			wantMatchedLen: 2,
		},
		{
			name: "fails when fields differ",
			receiver: &observedPayload{
				source: "receiver", signal: "metrics", correlation: "abc", receivedAt: time.Now(),
				flattened: map[string]string{"series[0].metric": "demo.metric", "series[0].value": "10"},
			},
			exporter: &observedPayload{
				source: "exporter", signal: "metrics", correlation: "abc", receivedAt: time.Now(),
				flattened: map[string]string{"series[0].metric": "demo.metric", "series[0].value": "11", "series[0].host": "node-a"},
			},
			wantFullPassed:   false,
			wantPassed:       true,
			wantMismatchLen:  1,
			wantMissingInLen: 1,
		},
		{
			name: "normalizes single-entry log array prefix",
			receiver: &observedPayload{
				source: "receiver", signal: "logs", correlation: "logs:corr-1", receivedAt: time.Now(),
				flattened: map[string]string{"message": "ddgen synthetic log event", "ddtags": "env:dev,correlation_id:corr-1"},
			},
			exporter: &observedPayload{
				source: "exporter", signal: "logs", correlation: "logs:corr-1", receivedAt: time.Now(),
				flattened: map[string]string{"[0].message": "ddgen synthetic log event", "[0].ddtags": "env:dev,correlation_id:corr-1"},
			},
			wantFullPassed: true,
			wantPassed:     true,
			wantMatchedLen: 2,
		},
		{
			name: "strips correlation_id from log ddtags",
			receiver: &observedPayload{
				source: "receiver", signal: "logs", correlation: "logs:corr-1", receivedAt: time.Now(),
				flattened: map[string]string{"ddtags": "env:dev,correlation_id:corr-a,otel_source:datadog_exporter"},
			},
			exporter: &observedPayload{
				source: "exporter", signal: "logs", correlation: "logs:corr-1", receivedAt: time.Now(),
				flattened: map[string]string{"[0].ddtags": "env:dev,correlation_id:corr-b,otel_source:datadog_exporter"},
			},
			wantFullPassed: true,
			wantPassed:     true,
		},
		{
			name: "strips correlation_id from log message JSON",
			receiver: &observedPayload{
				source: "receiver", signal: "logs", correlation: "logs:corr-1", receivedAt: time.Now(),
				flattened: map[string]string{"message": `{"message":"ddgen synthetic log event","service":"ddgen-svc","correlation_id":"corr-a"}`},
			},
			exporter: &observedPayload{
				source: "exporter", signal: "logs", correlation: "logs:corr-1", receivedAt: time.Now(),
				flattened: map[string]string{"[0].message": `{"service":"ddgen-svc","correlation_id":"corr-b","message":"ddgen synthetic log event"}`},
			},
			wantFullPassed: true,
			wantPassed:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := comparePair(tt.receiver, tt.exporter, Policy{})
			assert.Equal(t, tt.wantFullPassed, result.FullPayloadPassed, "FullPayloadPassed; result=%#v", result)
			assert.Equal(t, tt.wantPassed, result.Passed, "Passed; result=%#v", result)
			if tt.wantMatchedLen > 0 {
				assert.Len(t, result.Matched, tt.wantMatchedLen)
			}
			if tt.wantMismatchLen > 0 {
				assert.Len(t, result.Mismatched, tt.wantMismatchLen)
			}
			if tt.wantMissingInLen > 0 {
				assert.Len(t, result.MissingIn, tt.wantMissingInLen)
			}
		})
	}
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
