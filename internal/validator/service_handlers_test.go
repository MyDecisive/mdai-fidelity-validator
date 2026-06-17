package validator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTrimSyntheticSourcePath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		path       string
		source     string
		wantSignal string
		wantOK     bool
	}{
		{name: "exporter observe path", path: "/observe/exporter/traces", source: "exporter", wantSignal: "traces", wantOK: true},
		{name: "receiver observe path", path: "/observe/receiver/logs", source: "receiver", wantSignal: "logs", wantOK: true},
		{name: "exporter intake path", path: "/intake/exporter/metrics", source: "exporter", wantSignal: "", wantOK: false},
		{name: "receiver intake path", path: "/intake/receiver/traces", source: "receiver", wantSignal: "", wantOK: false},
		{name: "datadog api path", path: "/api/v2/logs", source: "exporter", wantSignal: "", wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotSignal, gotOK := trimSyntheticSourcePath(tc.path, tc.source)
			assert.Equal(t, tc.wantSignal, gotSignal)
			assert.Equal(t, tc.wantOK, gotOK)
		})
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

func newAdminPairsTestService() *Service {
	return &Service{
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
}

func postAdminPair(t *testing.T, svc *Service, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/pairs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	svc.handleAdminPairs(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code, "body=%s", rec.Body.String())
	return rec
}

func TestHandleAdminPairs(t *testing.T) {
	t.Parallel()

	svc := newAdminPairsTestService()

	body := `{"id":"shadow-b","receiver_translator":"datadog_raw","exporter_translator":"datadog_raw","default":true}`
	postAdminPair(t, svc, body)
	_, ok := svc.pairs["shadow-b"]
	require.True(t, ok, "expected pair shadow-b to be saved")
	assert.Equal(t, "shadow-b", svc.defaultPair)

	body = `{"id":"shadow-c","receiver_translator":"datadog_raw","exporter_translator":"datadog_raw","receiver_ports":["18126"],"exporter_ports":["18081"]}`
	postAdminPair(t, svc, body)
	assert.Equal(t, "shadow-c", svc.receiverPairByPort["18126"])
	assert.Equal(t, "shadow-c", svc.exporterPairByPort["18081"])

	body = `{"id":"shadow-d","receiver_translator":"datadog_raw","exporter_translator":"datadog_raw","exporter_ignore_paths":["/api/beta/sketches","api/v2/sketches"]}`
	postAdminPair(t, svc, body)
	assert.Equal(t, []string{"/api/beta/sketches", "/api/v2/sketches"}, svc.pairs["shadow-d"].ExporterIgnorePaths)
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

	tests := []struct {
		source     string
		path       string
		wantIgnore bool
	}{
		{source: "receiver", path: "/api/v0.2/stats", wantIgnore: true},
		{source: "receiver", path: "/api/v1/metadata", wantIgnore: true},
		{source: "receiver", path: "/api/beta/sketches", wantIgnore: true},
		{source: "receiver", path: "/support/flare", wantIgnore: true},
		{source: "receiver", path: "/intake/", wantIgnore: true},
		{source: "receiver", path: "/api/v2/logs", wantIgnore: false},
		{source: "receiver", path: "/v0.4/traces", wantIgnore: false},
		{source: "receiver", path: "/api/v2/series", wantIgnore: false},
		{source: "receiver", path: "/api/v1/series", wantIgnore: false},
		{source: "receiver", path: "/api/v0.2/traces", wantIgnore: false},
		{source: "exporter", path: "/api/v0.2/stats", wantIgnore: false}, // ignore list is receiver-only
	}

	for _, tt := range tests {
		t.Run(tt.source+":"+tt.path, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.wantIgnore, pair.shouldIgnorePath(tt.source, tt.path))
		})
	}
}
