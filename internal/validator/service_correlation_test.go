package validator

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCorrelationCandidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		signal     Signal
		fields     map[string]string
		wantFirst  []string // acceptable values for candidates[0]
		wantSecond string   // exact value for candidates[1], if set
		wantThird  string   // exact value for candidates[2], if set
		wantMinLen int
	}{
		{
			name:   "prefers correlation_id paths for unknown signal",
			signal: SignalUnknown,
			fields: map[string]string{
				"spans[0].meta.correlation_id":  "dd-span-1",
				"resource.correlation_id":       "resource-1",
				"resource.attributes.trace_id":  "trace-1",
				"series[0].metric":              "metric-name",
				"logs[0].attributes.service":    "checkout",
				"logs[0].attributes.trace_id":   "trace-2",
				"logs[0].attributes.otherthing": "value",
			},
			wantFirst:  []string{"resource.correlation_id", "correlation_id"},
			wantMinLen: 1,
		},
		{
			name:   "traces prefer trace_id over span_id over correlation_id",
			signal: SignalTraces,
			fields: map[string]string{
				"span_id":        "span-111",
				"trace_id":       "trace-222",
				"correlation_id": "corr-333",
			},
			wantFirst:  []string{"trace_id"},
			wantSecond: "span_id",
			wantThird:  "correlation_id",
			wantMinLen: 3,
		},
		{
			name:   "traces use suffix fallback preferring trace_id over span_id",
			signal: SignalTraces,
			fields: map[string]string{
				"[0][0].span_id":             "span-111",
				"[0][0].trace_id":            "trace-222",
				"[0][0].meta.correlation_id": "corr-333",
			},
			wantFirst:  []string{"[0][0].trace_id"},
			wantSecond: "[0][0].span_id",
			wantThird:  "[0][0].meta.correlation_id",
			wantMinLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			candidates := correlationCandidates(tt.signal, tt.fields)
			require.GreaterOrEqual(t, len(candidates), tt.wantMinLen)
			require.NotEmpty(t, candidates)
			assert.Contains(t, tt.wantFirst, candidates[0])
			if tt.wantSecond != "" {
				assert.Equal(t, tt.wantSecond, candidates[1])
			}
			if tt.wantThird != "" {
				assert.Equal(t, tt.wantThird, candidates[2])
			}
		})
	}
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

func TestResolveCorrelationID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		signal       Signal
		fields       map[string]string
		headers      map[string]string
		wantID       string
		wantStrategy string
		wantField    string
	}{
		{
			name:         "x-request-id header used as fallback",
			signal:       SignalLogs,
			fields:       map[string]string{},
			headers:      map[string]string{"X-Request-ID": "req-123"},
			wantID:       "logs:req-123",
			wantStrategy: "header",
			wantField:    "X-Request-ID",
		},
		{
			name:         "x-correlation-id header preferred over field",
			signal:       SignalLogs,
			fields:       map[string]string{"correlation_id": "from-field"},
			headers:      map[string]string{"X-Correlation-ID": "from-header"},
			wantID:       "logs:from-header",
			wantStrategy: "header",
			wantField:    "X-Correlation-ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			headers := http.Header{}
			for k, v := range tt.headers {
				headers.Set(k, v)
			}
			got := resolveCorrelationID(tt.signal, tt.fields, headers, []byte(`{"message":"hello"}`))
			assert.Equal(t, tt.wantID, got.CorrelationID)
			assert.Equal(t, tt.wantStrategy, got.Strategy)
			assert.Equal(t, tt.wantField, got.Field)
		})
	}
}

func TestDeriveCorrelationFromFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		signal Signal
		fields map[string]string
		wantID string
	}{
		{
			name:   "metric tags before metric name",
			signal: SignalMetrics,
			fields: map[string]string{
				"series[0].metric":  "ddgen.checkout.duration",
				"series[0].tags[0]": "service:checkout",
				"series[0].tags[1]": "correlation_id:corr-123",
			},
			wantID: "metrics:corr-123",
		},
		{
			name:   "batch-exploded metric tags (no series prefix)",
			signal: SignalMetrics,
			fields: map[string]string{
				"metric":  "ddgen.checkout.duration",
				"tags[0]": "service:checkout",
				"tags[1]": "env:dev",
				"tags[2]": "correlation_id:corr-456",
				"type":    "gauge",
			},
			wantID: "metrics:corr-456",
		},
		{
			name:   "log ddtags field",
			signal: SignalLogs,
			fields: map[string]string{
				"[0].ddtags": "env:dev,correlation_id:corr-log-1",
			},
			wantID: "logs:corr-log-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, _, _, ok := deriveCorrelationFromFieldsDetailed(tt.signal, tt.fields)
			require.True(t, ok)
			assert.Equal(t, tt.wantID, got)
		})
	}
}
