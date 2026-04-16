package validator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestCanonicalizeDatadogFieldsLogs(t *testing.T) {
	setDefaultFieldMappingPath(t)
	fields := map[string]string{
		"[0].ddtags":  "env:dev,correlation_id:corr-1,otel_source:datadog_exporter",
		"[0].message": `{"ddsource":"mdai-fidelity-validator","hostname":"localhost","message":"ddgen synthetic log event","service":"ddgen-svc","status":"info","timestamp":1773343955346}`,
	}
	mapping, _, err := loadFieldMapping()
	require.NoError(t, err)
	canonical := mapping.Map(SignalLogs, fields)

	assert.Equal(t, "ddgen-svc", canonical["service"])
	assert.Equal(t, "ddgen synthetic log event", canonical["message"])
	assert.Equal(t, "corr-1", canonical["correlation_id"])
}

func TestEvaluatePolicyLogsSemanticMatch(t *testing.T) {
	setDefaultFieldMappingPath(t)
	receiver := map[string]string{
		"message":        "ddgen synthetic log event",
		"service":        "ddgen-svc",
		"correlation_id": "corr-1",
	}
	exporter := map[string]string{
		"[0].ddtags":  "env:dev,correlation_id:corr-1,otel_source:datadog_exporter",
		"[0].message": `{"message":"ddgen synthetic log event","service":"ddgen-svc"}`,
	}
	mapping, _, err := loadFieldMapping()
	require.NoError(t, err)
	exporter = mapping.Map(SignalLogs, exporter)

	policy := Policy{
		Signals: map[Signal]SignalPolicy{
			"logs": {
				RequiredAttributes: []RequiredAttributePolicy{
					{Name: "message"},
					{Name: "correlation_id"},
					{Name: "service"},
				},
			},
		},
	}

	checks, passed := evaluatePolicy("logs", receiver, exporter, policy)
	require.True(t, passed, "expected policy to pass; checks=%+v", checks)
	require.Len(t, checks, 3)
	for _, check := range checks {
		assert.True(t, check.Passed, "expected check %q to pass, got %+v", check.Attribute, check)
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
	require.NoError(t, err)
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
	require.True(t, passed, "expected policy to pass; checks=%+v", checks)
	require.Len(t, checks, 2)
	for _, check := range checks {
		assert.True(t, check.Passed, "expected check %q to pass, got %+v", check.Attribute, check)
	}
}

func TestRequiredAttributesYAMLSupportsStringAndObject(t *testing.T) {
	t.Parallel()

	body := []byte(`
signals:
  logs:
    required_attributes:
      - message
      - name: timestamp
        compare: presence_only
`)
	var policy Policy
	require.NoError(t, yaml.Unmarshal(body, &policy))
	logs := policy.Signals[SignalLogs]
	require.Len(t, logs.RequiredAttributes, 2)
	assert.Equal(t, "message", logs.RequiredAttributes[0].Name)
	assert.Equal(t, "value", logs.RequiredAttributes[0].compareMode())
	assert.Equal(t, "timestamp", logs.RequiredAttributes[1].Name)
	assert.Equal(t, "presence_only", logs.RequiredAttributes[1].compareMode())
}
