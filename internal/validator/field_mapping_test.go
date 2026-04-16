package validator

import (
	"path/filepath"
	"runtime"
	"testing"
)

func setDefaultFieldMappingPath(t *testing.T) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	t.Setenv(fieldMappingPathEnvVar, filepath.Join(filepath.Dir(thisFile), "field-mapping.yaml"))
}

func TestFieldMappingMapLogs(t *testing.T) {
	setDefaultFieldMappingPath(t)
	mapping, _, err := loadFieldMapping()
	if err != nil {
		t.Fatalf("loadFieldMapping: %v", err)
	}
	fields := map[string]string{
		"[0].ddtags":  "env:dev,correlation_id:corr-1",
		"[0].message": `{"message":"event","service":"svc-a","status":"info","timestamp":1773343955346}`,
	}

	mapped := mapping.Map(SignalLogs, fields)
	if got := mapped["message"]; got != "event" {
		t.Fatalf("message=%q", got)
	}
	if got := mapped["service"]; got != "svc-a" {
		t.Fatalf("service=%q", got)
	}
	if got := mapped["correlation_id"]; got != "corr-1" {
		t.Fatalf("correlation_id=%q", got)
	}
}

func TestExtractMappedValueOps(t *testing.T) {
	fields := map[string]string{
		"raw_message": `{"inner":{"value":"abc"}}`,
		"tags":        "env:dev,service:payments",
	}
	if value, ok := extractMappedValue(fields, "raw_message|json:inner.value"); !ok || value != "abc" {
		t.Fatalf("json op value=%q ok=%v", value, ok)
	}
	if value, ok := extractMappedValue(fields, "tags|tag:service"); !ok || value != "payments" {
		t.Fatalf("tag op value=%q ok=%v", value, ok)
	}
}

func TestFieldMappingMapForPathPrefersExporterSpecificRules(t *testing.T) {
	mapping := FieldMapping{
		Signals: map[Signal]map[string][]string{
			SignalLogs: {
				"message": []string{"[0].message"},
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
		"event":       "splunk-msg",
	})
	if got := mapped["message"]; got != "splunk-msg" {
		t.Fatalf("message=%q", got)
	}
}

func TestFieldMappingMapForPathUnknownSignalSelectsBestMatch(t *testing.T) {
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
	if got := mapped["message"]; got != "hello" {
		t.Fatalf("message=%q", got)
	}
	if got := mapped["service"]; got != "svc-a" {
		t.Fatalf("service=%q", got)
	}
}
