package validator

import (
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const defaultPolicyPath = "fidelity-policy.yaml"

type Policy struct {
	Signals map[string]SignalPolicy `yaml:"signals"`
}

type SignalPolicy struct {
	RequiredAttributes []string `yaml:"required_attributes"`
}

type RequiredAttributeCheck struct {
	Attribute string `json:"attribute"`
	Receiver  string `json:"receiver,omitempty"`
	Exporter  string `json:"exporter,omitempty"`
	Passed    bool   `json:"passed"`
	Reason    string `json:"reason,omitempty"`
}

func loadPolicy() (Policy, error) {
	policy := defaultPolicy()

	body, err := os.ReadFile(defaultPolicyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return policy, nil
		}
		return Policy{}, err
	}

	if err := yaml.Unmarshal(body, &policy); err != nil {
		return Policy{}, err
	}
	return policy, nil
}

func defaultPolicy() Policy {
	return Policy{
		Signals: map[string]SignalPolicy{
			"metrics": {
				RequiredAttributes: []string{
					"metric_name",
					"correlation_id",
					"fidelity_correlation_id",
					"service",
					"env",
					"point_timestamp",
					"point_value",
				},
			},
			"traces": {
				RequiredAttributes: []string{
					"trace_id",
					"span_count",
					"correlation_id",
					"fidelity_correlation_id",
					"service",
				},
			},
			"logs": {
				RequiredAttributes: []string{
					"message",
					"correlation_id",
					"fidelity_correlation_id",
					"service",
				},
			},
		},
	}
}

func evaluatePolicy(signal string, receiver, exporter map[string]string, policy Policy) ([]RequiredAttributeCheck, bool) {
	signalPolicy, ok := policy.Signals[signal]
	if !ok || len(signalPolicy.RequiredAttributes) == 0 {
		return nil, true
	}

	checks := make([]RequiredAttributeCheck, 0, len(signalPolicy.RequiredAttributes))
	allPassed := true
	for _, attribute := range signalPolicy.RequiredAttributes {
		receiverValue, receiverOK := extractRequiredAttribute(signal, attribute, receiver)
		exporterValue, exporterOK := extractRequiredAttribute(signal, attribute, exporter)

		check := RequiredAttributeCheck{
			Attribute: attribute,
			Receiver:  receiverValue,
			Exporter:  exporterValue,
		}

		switch {
		case !receiverOK:
			check.Reason = "missing_in_receiver"
		case !exporterOK:
			check.Reason = "missing_in_exporter"
		case receiverValue != exporterValue:
			check.Reason = "value_mismatch"
		default:
			check.Passed = true
		}

		if !check.Passed {
			allPassed = false
		}
		checks = append(checks, check)
	}

	return checks, allPassed
}

func extractRequiredAttribute(signal, attribute string, fields map[string]string) (string, bool) {
	switch signal {
	case "metrics":
		return extractMetricAttribute(attribute, fields)
	case "traces":
		return extractTraceAttribute(attribute, fields)
	case "logs":
		return extractLogAttribute(attribute, fields)
	default:
		return "", false
	}
}

func extractMetricAttribute(attribute string, fields map[string]string) (string, bool) {
	switch attribute {
	case "metric_name":
		return firstNonEmpty(fields, "series[0].metric")
	case "correlation_id":
		return findTagValue(fields, "correlation_id")
	case "fidelity_correlation_id":
		return findTagValue(fields, "fidelity.correlation_id")
	case "service":
		return findTagValue(fields, "service")
	case "env":
		return findTagValue(fields, "env")
	case "point_timestamp":
		return firstNonEmpty(fields, "series[0].points[0][0]", "series[0].points[0].timestamp")
	case "point_value":
		return firstNonEmpty(fields, "series[0].points[0][1]", "series[0].points[0].value")
	default:
		return "", false
	}
}

func extractTraceAttribute(attribute string, fields map[string]string) (string, bool) {
	switch attribute {
	case "trace_id":
		return firstNonEmpty(fields,
			"[0][0].trace_id",
			"[0][1].trace_id",
			"tracerPayloads[0].chunks[0].spans[0].trace_id",
			"tracerPayloads[0].chunks[0].spans[1].trace_id",
			"tracer_payloads[0].chunks[0].spans[0].trace_id",
			"tracer_payloads[0].chunks[0].spans[1].trace_id",
			"tracer_payloads[0].chunks[0].spans[0].traceID",
			"tracerPayloads[0].chunks[0].spans[0].traceID",
		)
	case "span_count":
		return traceSpanCount(fields)
	case "correlation_id":
		return firstNonEmpty(fields,
			"[0][0].meta.correlation_id",
			"[0][1].meta.correlation_id",
			"tracerPayloads[0].chunks[0].spans[0].meta.correlation_id",
			"tracerPayloads[0].chunks[0].spans[1].meta.correlation_id",
			"tracer_payloads[0].chunks[0].spans[0].meta.correlation_id",
			"tracer_payloads[0].chunks[0].spans[1].meta.correlation_id",
		)
	case "fidelity_correlation_id":
		return firstNonEmpty(fields,
			"[0][0].meta.fidelity.correlation_id",
			"[0][1].meta.fidelity.correlation_id",
			"tracerPayloads[0].chunks[0].spans[0].meta.fidelity.correlation_id",
			"tracerPayloads[0].chunks[0].spans[1].meta.fidelity.correlation_id",
			"tracer_payloads[0].chunks[0].spans[0].meta.fidelity.correlation_id",
			"tracer_payloads[0].chunks[0].spans[1].meta.fidelity.correlation_id",
		)
	case "service":
		return firstNonEmpty(fields,
			"[0][0].service",
			"[0][1].service",
			"tracerPayloads[0].chunks[0].spans[0].service",
			"tracerPayloads[0].chunks[0].spans[1].service",
			"tracer_payloads[0].chunks[0].spans[0].service",
			"tracer_payloads[0].chunks[0].spans[1].service",
		)
	default:
		return "", false
	}
}

func extractLogAttribute(attribute string, fields map[string]string) (string, bool) {
	switch attribute {
	case "message":
		if value, ok := nestedJSONField(fields, "[0].message", "message"); ok {
			return value, true
		}
		return firstNonEmpty(fields, "message")
	case "correlation_id":
		if value, ok := firstNonEmpty(fields, "correlation_id"); ok {
			return value, true
		}
		if value, ok := findCommaTagValue(fields, "[0].ddtags", "correlation_id"); ok {
			return value, true
		}
		return findTagValue(fields, "correlation_id")
	case "fidelity_correlation_id":
		if value, ok := firstNonEmpty(fields, "attributes.fidelity.correlation_id"); ok {
			return value, true
		}
		if value, ok := findCommaTagValue(fields, "[0].ddtags", "fidelity.correlation_id"); ok {
			return value, true
		}
		return findTagValue(fields, "fidelity.correlation_id")
	case "service":
		if value, ok := nestedJSONField(fields, "[0].message", "service"); ok {
			return value, true
		}
		return firstNonEmpty(fields, "service")
	default:
		return "", false
	}
}

func firstNonEmpty(fields map[string]string, keys ...string) (string, bool) {
	for _, key := range keys {
		if value := fields[key]; value != "" {
			return value, true
		}
	}
	return "", false
}

func findTagValue(fields map[string]string, prefix string) (string, bool) {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if !strings.Contains(key, ".tags[") {
			continue
		}
		value := fields[key]
		if strings.HasPrefix(value, prefix+":") {
			return strings.TrimPrefix(value, prefix+":"), true
		}
	}
	return "", false
}

func findCommaTagValue(fields map[string]string, key, prefix string) (string, bool) {
	value := fields[key]
	if value == "" {
		return "", false
	}

	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, prefix+":") {
			return strings.TrimPrefix(part, prefix+":"), true
		}
	}
	return "", false
}

func nestedJSONField(fields map[string]string, key, nestedField string) (string, bool) {
	raw := fields[key]
	if raw == "" || !strings.HasPrefix(strings.TrimSpace(raw), "{") {
		return "", false
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return "", false
	}
	value, ok := payload[nestedField]
	if !ok {
		return "", false
	}
	return strings.TrimSpace(strings.ReplaceAll(strings.Trim(fmtValue(value), "\""), "\n", " ")), true
}

func fmtValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return strings.TrimSpace(strings.ReplaceAll(strings.TrimSpace(toJSONScalar(typed)), "\n", " "))
	}
}

func toJSONScalar(value any) string {
	body, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(body)
}

func traceSpanCount(fields map[string]string) (string, bool) {
	maxIndex := -1
	for key := range fields {
		if !strings.HasPrefix(key, "[0][") || !strings.Contains(key, "].trace_id") {
			continue
		}
		rest := strings.TrimPrefix(key, "[0][")
		indexStr := strings.SplitN(rest, "]", 2)[0]
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			continue
		}
		if index > maxIndex {
			maxIndex = index
		}
	}
	if maxIndex >= 0 {
		return strconv.Itoa(maxIndex + 1), true
	}

	count := 0
	for key := range fields {
		if strings.Contains(key, ".spans[") && (strings.HasSuffix(key, ".traceID") || strings.HasSuffix(key, ".trace_id")) {
			count++
		}
	}
	if count > 0 {
		return strconv.Itoa(count), true
	}
	return "", false
}
