package validator

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setDefaultFieldMappingPath(t *testing.T) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller(0) failed")
	t.Setenv(fieldMappingPathEnvVar, filepath.Join(filepath.Dir(thisFile), "field-mapping.yaml"))
}

func TestFieldMappingMapLogs(t *testing.T) {
	setDefaultFieldMappingPath(t)
	mapping, _, err := loadFieldMapping()
	require.NoError(t, err)
	fields := map[string]string{
		"[0].ddtags":  "env:dev,correlation_id:corr-1",
		"[0].message": `{"message":"event","service":"svc-a","status":"info","timestamp":1773343955346}`,
	}

	mapped := mapping.Map(SignalLogs, fields)
	assert.Equal(t, "event", mapped["message"])
	assert.Equal(t, "svc-a", mapped["service"])
	assert.Equal(t, "corr-1", mapped["correlation_id"])
}

func TestFieldMappingMapTracesStatus(t *testing.T) {
	setDefaultFieldMappingPath(t)
	mapping, _, err := loadFieldMapping()
	require.NoError(t, err)

	mapped := mapping.Map(SignalTraces, map[string]string{
		"[0][0].status": "Ok",
		"[0][0].name":   "ddgen.request",
	})
	assert.Equal(t, "Ok", mapped["status"])

	mapped = mapping.MapForPath(SignalTraces, "/exporter/datadog/api/v0.2/traces", map[string]string{
		"traces[0][0].meta.otel.status_code": "Ok",
		"traces[0][0].resource":              "ddgen.request",
	})
	assert.Equal(t, "Ok", mapped["status"])
}

func TestExtractMappedValueOps(t *testing.T) {
	t.Parallel()

	fields := map[string]string{
		"raw_message": `{"inner":{"value":"abc"}}`,
		"tags":        "env:dev,service:payments",
	}
	value, ok := extractMappedValue(fields, "raw_message|json:inner.value")
	require.True(t, ok)
	assert.Equal(t, "abc", value)
	value, ok = extractMappedValue(fields, "tags|tag:service")
	require.True(t, ok)
	assert.Equal(t, "payments", value)
}

func TestExtractMappedValueLowerOp(t *testing.T) {
	t.Parallel()

	fields := map[string]string{
		"status":     "INFO",
		"raw_status": `{"level":"WARN"}`,
		"blank":      "   ",
	}

	value, ok := extractMappedValue(fields, "status|lower")
	require.True(t, ok)
	assert.Equal(t, "info", value)

	value, ok = extractMappedValue(fields, "raw_status|json:level|lower")
	require.True(t, ok)
	assert.Equal(t, "warn", value)

	_, ok = extractMappedValue(fields, "blank|lower")
	assert.False(t, ok)
}

func TestDefaultMappingLowercasesLogStatus(t *testing.T) {
	setDefaultFieldMappingPath(t)
	mapping, _, err := loadFieldMapping()
	require.NoError(t, err)

	receiver := mapping.MapForPath(SignalLogs, "/api/v2/logs", map[string]string{"status": "info"})
	exporter := mapping.MapForPath(SignalLogs, "/api/v2/logs", map[string]string{"status": "INFO"})

	assert.Equal(t, "info", receiver["status"])
	assert.Equal(t, "info", exporter["status"])
	assert.Equal(t, receiver["status"], exporter["status"])
}

func TestExtractMappedValuePrefersShallowSuffixMatch(t *testing.T) {
	t.Parallel()

	fields := map[string]string{
		"traces[0][0].name":                             "server.request",
		"traces[0][0].meta.deployment.environment.name": "dev",
		"traces[0][0].trace_id":                         "218523465776977553",
		"traces[0][0].meta.otel.trace_id":               "0000000000000000030859ef30b95691",
	}

	value, ok := extractMappedValue(fields, "suffix:.name")
	require.True(t, ok)
	assert.Equal(t, "server.request", value)

	value, ok = extractMappedValue(fields, "suffix:.trace_id")
	require.True(t, ok)
	assert.Equal(t, "218523465776977553", value)
}

func TestFieldMappingMapForPathPrefersExporterSpecificRules(t *testing.T) {
	t.Parallel()

	mapping := FieldMapping{
		Signals: map[Signal]map[string][]string{
			SignalLogs: {
				"message": []string{"[0].message"},
				"service": []string{"service"},
			},
		},
		Exporters: map[string]FieldMappingExporters{
			"splunk": {
				Signals: map[Signal]map[string][]string{
					SignalLogs: {
						"message": []string{"event"},
					},
				},
			},
		},
	}

	mapped := mapping.MapForPath(SignalLogs, "/exporter/splunk/services/collector/event", map[string]string{
		"[0].message": "datadog-msg",
		"service":     "svc-a",
		"event":       "splunk-msg",
	})
	assert.Equal(t, "splunk-msg", mapped["message"])
	assert.Equal(t, "svc-a", mapped["service"])
}

func TestFieldMappingMapForPathExporterSpecificOverridesOnlySpecifiedKeys(t *testing.T) {
	t.Parallel()

	mapping := FieldMapping{
		Signals: map[Signal]map[string][]string{
			SignalTraces: {
				"correlation_id": []string{"generic_correlation"},
				"operation":      []string{"generic_operation"},
				"service":        []string{"generic_service"},
			},
		},
		Exporters: map[string]FieldMappingExporters{
			"datadog": {
				Signals: map[Signal]map[string][]string{
					SignalTraces: {
						"operation": []string{"vendor_operation"},
					},
				},
			},
		},
	}

	mapped := mapping.MapForPath(SignalTraces, "/exporter/datadog/api/v0.2/traces", map[string]string{
		"generic_correlation": "corr-1",
		"generic_operation":   "generic-op",
		"vendor_operation":    "vendor-op",
		"generic_service":     "svc-a",
	})

	assert.Equal(t, "corr-1", mapped["correlation_id"])
	assert.Equal(t, "vendor-op", mapped["operation"])
	assert.Equal(t, "svc-a", mapped["service"])
}

func TestFieldMappingMetricCorrelationFromBatchExplodedTags(t *testing.T) {
	// After extractIndexedGroups strips the "series[i]" prefix, metric group keys are
	// top-level: "tags[j]" instead of "series[0].tags[j]". The default mapping must still
	// extract correlation_id via "contains:tags[|tag:correlation_id".
	setDefaultFieldMappingPath(t)
	mapping, _, err := loadFieldMapping()
	require.NoError(t, err)

	// Simulate the per-series group after batch explode (series[i] prefix stripped).
	fields := map[string]string{
		"metric":  "ddgen.checkout.duration",
		"type":    "gauge",
		"tags[0]": "service:checkout",
		"tags[1]": "env:staging",
		"tags[2]": "correlation_id:corr-batch-explode",
	}

	mapped := mapping.Map(SignalMetrics, fields)
	assert.Equal(t, "corr-batch-explode", mapped["correlation_id"])
	assert.Equal(t, "checkout", mapped["service"])
	assert.Equal(t, "staging", mapped["env"])
}

func TestFieldMappingMapForPathUnknownSignalSelectsBestMatch(t *testing.T) {
	t.Parallel()

	mapping := FieldMapping{
		Signals: map[Signal]map[string][]string{
			SignalLogs: {
				"message": []string{"event"},
				"service": []string{"service"},
			},
			SignalTraces: {
				"trace_id": []string{"trace_id"},
			},
		},
	}

	mapped := mapping.MapForPath(SignalUnknown, "/splunk/services/collector/event", map[string]string{
		"event":   "hello",
		"service": "svc-a",
	})
	assert.Equal(t, "hello", mapped["message"])
	assert.Equal(t, "svc-a", mapped["service"])
}
