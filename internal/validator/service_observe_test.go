package validator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestAdjustPendingTotalUsesConnectionLabel(t *testing.T) {
	t.Parallel()

	svc, registry := newMetricsTestService("shadow-c")
	svc.stateMu.Lock()
	svc.adjustPendingTotal(2)
	svc.stateMu.Unlock()

	assert.InDelta(t, float64(2), gaugeValue(t, svc.pendingGauge.WithLabelValues("shadow-c")), 0.000001)

	requireMetricsHaveConnectionLabel(t, registry, []string{"mdai_fidelity_pending_payloads"}, "shadow-c")
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

	requireMetricsHaveConnectionLabel(t, registry, []string{
		"mdai_fidelity_attribute_checks_total",
		"mdai_fidelity_signal_checks_total",
		"mdai_fidelity_required_attribute_checks_total",
		"mdai_fidelity_required_signal_checks_total",
	}, "shadow-a")
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

	requireMetricsHaveConnectionLabel(t, registry, []string{"mdai_fidelity_payloads_received_total"}, "shadow-b")
}
