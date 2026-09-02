package validator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func requireMetricsHaveLabels(t *testing.T, registry *prometheus.Registry, metricNames []string, labels map[string]string) {
	t.Helper()

	families, err := registry.Gather()
	require.NoError(t, err)

	familiesByName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		familiesByName[family.GetName()] = family
	}

	for _, metricName := range metricNames {
		family, ok := familiesByName[metricName]
		require.Truef(t, ok, "metric family %s not found", metricName)
		require.NotEmpty(t, family.GetMetric(), "metric %s had no series", metricName)
		for _, metric := range family.GetMetric() {
			assert.True(t, metricHasLabels(metric, labels), "metric %s missing labels %+v: %+v", metricName, labels, metric.GetLabel())
		}
	}
}

func requireMetricsHaveConnectionLabel(t *testing.T, registry *prometheus.Registry, metricNames []string, connection string) {
	t.Helper()

	requireMetricsHaveLabels(t, registry, metricNames, map[string]string{"mdai_connection": connection})
}

func metricHasLabels(metric *dto.Metric, labels map[string]string) bool {
	found := make(map[string]string, len(metric.GetLabel()))
	for _, metricLabel := range metric.GetLabel() {
		found[metricLabel.GetName()] = metricLabel.GetValue()
	}
	for name, value := range labels {
		if found[name] != value {
			return false
		}
	}
	return true
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

func captureWithBatch(
	t *testing.T,
	svc *Service,
	source string,
	signal Signal,
	listener string,
	requestPath string,
	headers map[string]string,
	batch []DecodedPayload,
) ([]*observedPayload, []*ComparisonResult, bool) {
	t.Helper()

	svc.translators[defaultTranslatorID] = staticTranslator{
		name:  defaultTranslatorID,
		batch: batch,
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, requestPath, strings.NewReader("{}"))
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	payloads, results, matched, err := svc.captureRequests(
		defaultPairID,
		defaultTranslatorID,
		source,
		signal,
		listener,
		requestPath,
		req,
	)
	require.NoError(t, err)
	return payloads, results, matched
}

type sanitizerTestCase[T any] struct {
	name  string
	input T
	want  T
}

func testSanitizer[T any](t *testing.T, sanitize func(T) T, tests []sanitizerTestCase[T]) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, sanitize(tt.input))
		})
	}
}
