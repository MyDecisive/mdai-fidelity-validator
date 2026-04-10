package validator

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCanonicalizeDatadogFieldsLogs(t *testing.T) {
	setDefaultFieldMappingPath(t)
	fields := map[string]string{
		"[0].ddtags":  "env:dev,correlation_id:corr-1,fidelity.correlation_id:corr-1,otel_source:datadog_exporter",
		"[0].message": `{"ddsource":"mdai-fidelity-validator","hostname":"localhost","message":"ddgen synthetic log event","service":"ddgen-svc","status":"info","timestamp":1773343955346}`,
	}
	mapping, _, err := loadFieldMapping()
	if err != nil {
		t.Fatalf("loadFieldMapping: %v", err)
	}
	canonical := mapping.Map(SignalLogs, fields)

	if got := canonical["service"]; got != "ddgen-svc" {
		t.Fatalf("service=%q", got)
	}

	if got := canonical["message"]; got != "ddgen synthetic log event" {
		t.Fatalf("message=%q", got)
	}

	if got := canonical["correlation_id"]; got != "corr-1" {
		t.Fatalf("correlation=%q", got)
	}

	if got := canonical["fidelity_correlation_id"]; got != "corr-1" {
		t.Fatalf("fidelity=%q", got)
	}
}

func TestEvaluatePolicyLogsSemanticMatch(t *testing.T) {
	setDefaultFieldMappingPath(t)
	receiver := map[string]string{
		"message":                 "ddgen synthetic log event",
		"service":                 "ddgen-svc",
		"correlation_id":          "corr-1",
		"fidelity.correlation_id": "corr-1",
	}
	exporter := map[string]string{
		"[0].ddtags":  "env:dev,correlation_id:corr-1,fidelity.correlation_id:corr-1,otel_source:datadog_exporter",
		"[0].message": `{"message":"ddgen synthetic log event","service":"ddgen-svc"}`,
	}
	mapping, _, err := loadFieldMapping()
	if err != nil {
		t.Fatalf("loadFieldMapping: %v", err)
	}
	exporter = mapping.Map(SignalLogs, exporter)

	policy := Policy{
		Signals: map[Signal]SignalPolicy{
			"logs": {
				RequiredAttributes: []RequiredAttributePolicy{
					{Name: "message"},
					{Name: "correlation_id"},
					{Name: "fidelity_correlation_id"},
					{Name: "service"},
				},
			},
		},
	}

	checks, passed := evaluatePolicy("logs", receiver, exporter, policy)
	if !passed {
		t.Fatalf("expected policy to pass; checks=%+v", checks)
	}
	if len(checks) != 4 {
		t.Fatalf("expected 4 checks, got %d", len(checks))
	}
	for _, check := range checks {
		if !check.Passed {
			t.Fatalf("expected check %q to pass, got %+v", check.Attribute, check)
		}
	}
}

func TestEvaluatePolicyLogsPresenceOnlyTimestamp(t *testing.T) {
	setDefaultFieldMappingPath(t)
	receiver := map[string]string{
		"message":   "log entry",
		"timestamp": "1773343955346",
	}
	exporter := map[string]string{
		"[0].message":   `{"message":"log entry"}`,
		"[0].timestamp": "1773343956000",
	}
	mapping, _, err := loadFieldMapping()
	if err != nil {
		t.Fatalf("loadFieldMapping: %v", err)
	}
	exporter = mapping.Map(SignalLogs, exporter)

	policy := Policy{
		Signals: map[Signal]SignalPolicy{
			SignalLogs: {
				RequiredAttributes: []RequiredAttributePolicy{
					{Name: "message"},
					{Name: "timestamp", Compare: "presence_only"},
				},
			},
		},
	}

	checks, passed := evaluatePolicy(SignalLogs, receiver, exporter, policy)
	if !passed {
		t.Fatalf("expected policy to pass; checks=%+v", checks)
	}
	if len(checks) != 2 {
		t.Fatalf("expected 2 checks, got %d", len(checks))
	}
	for _, check := range checks {
		if !check.Passed {
			t.Fatalf("expected check %q to pass, got %+v", check.Attribute, check)
		}
	}
}

func TestRequiredAttributesYAMLSupportsStringAndObject(t *testing.T) {
	body := []byte(`
signals:
  logs:
    required_attributes:
      - message
      - name: timestamp
        compare: presence_only
`)
	var policy Policy
	if err := yaml.Unmarshal(body, &policy); err != nil {
		t.Fatalf("yaml unmarshal: %v", err)
	}
	logs := policy.Signals[SignalLogs]
	if len(logs.RequiredAttributes) != 2 {
		t.Fatalf("expected 2 required attributes, got %d", len(logs.RequiredAttributes))
	}
	if logs.RequiredAttributes[0].Name != "message" || logs.RequiredAttributes[0].compareMode() != "value" {
		t.Fatalf("unexpected first attribute: %+v", logs.RequiredAttributes[0])
	}
	if logs.RequiredAttributes[1].Name != "timestamp" || logs.RequiredAttributes[1].compareMode() != "presence_only" {
		t.Fatalf("unexpected second attribute: %+v", logs.RequiredAttributes[1])
	}
}
