package validator

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	defaultPolicyPath = "fidelity-policy.yaml"
	policyPathEnvVar  = "MDAI_FIDELITY_POLICY_PATH"
)

//go:embed fidelity-policy.yaml
var embeddedPolicy []byte

type Signal string

const (
	SignalTraces   Signal = "traces"
	SignalMetrics  Signal = "metrics"
	SignalLogs     Signal = "logs"
	SignalUnknown  Signal = "unknown"
	SignalAPI      Signal = "api"
	SignalValidate Signal = "validate"
)

type Policy struct {
	Signals map[Signal]SignalPolicy `yaml:"signals"`
}

type SignalPolicy struct {
	RequiredAttributes []string `yaml:"required_attributes"` //nolint:tagliatelle
}

type RequiredAttributeCheck struct {
	Attribute    string `json:"attribute"`
	Passed       bool   `json:"passed"`
	TotalItems   int    `json:"total_items,omitempty"`
	PassedItems  int    `json:"passed_items,omitempty"`
	MismatchedAt string `json:"mismatched_at,omitempty"`
	Receiver     string `json:"receiver,omitempty"`
	Exporter     string `json:"exporter,omitempty"`
	Reason       string `json:"reason,omitempty"`
}

func loadPolicy() (Policy, string, error) {
	policy := defaultPolicy()

	if configuredPath := os.Getenv(policyPathEnvVar); configuredPath != "" {
		body, err := os.ReadFile(configuredPath) //nolint:gosec
		if err != nil {
			return Policy{}, "", err
		}
		err = yaml.Unmarshal(body, &policy)
		if err != nil {
			return Policy{}, "", err
		}
		return policy, fmt.Sprintf("file:%s (via %s)", configuredPath, policyPathEnvVar), nil
	}

	body, err := os.ReadFile(defaultPolicyPath)
	if err == nil {
		err = yaml.Unmarshal(body, &policy)
		if err != nil {
			return Policy{}, "", err
		}
		return policy, "file:" + defaultPolicyPath, nil
	}
	if !os.IsNotExist(err) {
		return Policy{}, "", err
	}

	if len(embeddedPolicy) > 0 {
		err = yaml.Unmarshal(embeddedPolicy, &policy)
		if err != nil {
			return Policy{}, "", err
		}
		return policy, "embedded:internal/validator/fidelity-policy.yaml", nil
	}

	return policy, "builtin-defaults", nil
}

func defaultPolicy() Policy {
	return Policy{
		Signals: map[Signal]SignalPolicy{
			SignalMetrics: {
				RequiredAttributes: []string{
					"metric_name",
					"service",
					"env",
					"point_timestamp",
					"point_value",
				},
			},
			SignalTraces: {
				RequiredAttributes: []string{
					"trace_id",
					"span_id",
					"service",
					"operation",
				},
			},
			SignalLogs: {
				RequiredAttributes: []string{
					"message",
					"service",
				},
			},
		},
	}
}

func evaluatePolicy(signal Signal, receiver, exporter map[string]string, policy Policy) ([]RequiredAttributeCheck, bool) {
	signalPolicy, ok := policy.Signals[signal]
	if !ok || len(signalPolicy.RequiredAttributes) == 0 {
		return nil, true
	}

	checks := make([]RequiredAttributeCheck, 0, len(signalPolicy.RequiredAttributes))
	allPassed := true

	for _, attribute := range signalPolicy.RequiredAttributes {
		check := evaluateAttributeDeep(signal, attribute, receiver, exporter)
		checks = append(checks, check)
		if !check.Passed {
			allPassed = false
		}
	}

	return checks, allPassed
}

func evaluateAttributeDeep(signal Signal, attribute string, receiver, exporter map[string]string) RequiredAttributeCheck {
	if signal == SignalLogs {
		return evaluateLogAttribute(attribute, receiver, exporter)
	}

	check := RequiredAttributeCheck{
		Attribute: attribute,
		Passed:    true,
	}

	// 1. Identify all keys in the receiver that match this attribute pattern
	receiverKeys := findKeysForAttribute(signal, attribute, receiver)
	if len(receiverKeys) == 0 {
		check.Passed = false
		check.Reason = "missing_in_receiver"
		return check
	}

	check.TotalItems = len(receiverKeys)

	// 2. For every key found in the receiver, verify it exists and matches in the exporter
	for _, rKey := range receiverKeys {
		rVal := receiver[rKey]

		// Map the receiver key to the corresponding exporter key
		// Usually they are identical if the mirroring is 1:1
		eVal, ok := exporter[rKey]

		if !ok {
			check.Passed = false
			if check.MismatchedAt == "" {
				check.MismatchedAt = rKey
				check.Reason = "missing_in_exporter"
				check.Receiver = rVal
			}
			continue
		}

		if rVal != eVal {
			check.Passed = false
			if check.MismatchedAt == "" {
				check.MismatchedAt = rKey
				check.Reason = "value_mismatch"
				check.Receiver = rVal
				check.Exporter = eVal
			}
			continue
		}

		check.PassedItems++
	}

	return check
}

func evaluateLogAttribute(attribute string, receiver, exporter map[string]string) RequiredAttributeCheck {
	check := RequiredAttributeCheck{
		Attribute:   attribute,
		Passed:      true,
		TotalItems:  1,
		PassedItems: 1,
	}

	receiverValue, receiverOK := extractLogAttribute(attribute, receiver)
	if !receiverOK {
		check.Passed = false
		check.PassedItems = 0
		check.Reason = "missing_in_receiver"
		return check
	}

	exporterValue, exporterOK := extractLogAttribute(attribute, exporter)
	if !exporterOK {
		check.Passed = false
		check.PassedItems = 0
		check.Reason = "missing_in_exporter"
		check.Receiver = receiverValue
		return check
	}

	if receiverValue != exporterValue {
		check.Passed = false
		check.PassedItems = 0
		check.Reason = "value_mismatch"
		check.Receiver = receiverValue
		check.Exporter = exporterValue
	}

	return check
}

func findKeysForAttribute(signal Signal, attribute string, fields map[string]string) []string {
	switch signal {
	case SignalTraces:
		return findTraceKeysForAttribute(attribute, fields)
	case SignalMetrics:
		return findMetricKeysForAttribute(attribute, fields)
	case SignalLogs:
		return findLogKeysForAttribute(attribute, fields)
	case SignalAPI, SignalValidate, SignalUnknown:
		return nil
	}
	return nil
}

func findTraceKeysForAttribute(attribute string, fields map[string]string) []string {
	switch attribute {
	case "trace_id":
		return collectMatchingKeys(fields, func(k, _ string) bool {
			return strings.HasSuffix(k, ".trace_id") || strings.HasSuffix(k, ".traceID")
		})
	case "span_id":
		return collectMatchingKeys(fields, func(k, _ string) bool {
			return strings.HasSuffix(k, ".span_id") || strings.HasSuffix(k, ".spanID")
		})
	default:
		suffix := "." + attribute
		return collectMatchingKeys(fields, func(k, _ string) bool {
			return strings.HasSuffix(k, suffix)
		})
	}
}

func findMetricKeysForAttribute(attribute string, fields map[string]string) []string {
	switch attribute {
	case "metric_name":
		return collectMatchingKeys(fields, func(k, _ string) bool {
			return strings.HasSuffix(k, ".metric")
		})
	case "point_timestamp":
		return collectMatchingKeys(fields, func(k, _ string) bool {
			return strings.Contains(k, ".points[") && (strings.HasSuffix(k, "][0]") || strings.HasSuffix(k, ".timestamp"))
		})
	case "point_value":
		return collectMatchingKeys(fields, func(k, _ string) bool {
			return strings.Contains(k, ".points[") && (strings.HasSuffix(k, "][1]") || strings.HasSuffix(k, ".value"))
		})
	default:
		return collectMatchingKeys(fields, func(k, v string) bool {
			return strings.Contains(k, ".tags[") && strings.HasPrefix(v, attribute+":")
		})
	}
}

func findLogKeysForAttribute(attribute string, fields map[string]string) []string {
	return collectMatchingKeys(fields, func(k, _ string) bool {
		return strings.HasSuffix(k, "."+attribute) || k == attribute
	})
}

func collectMatchingKeys(fields map[string]string, match func(key, value string) bool) []string {
	keys := make([]string, 0, len(fields))
	for k, v := range fields {
		if match(k, v) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
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

func extractLogAttribute(attribute string, fields map[string]string) (string, bool) {
	keys := sortedFieldKeys(fields)
	if value, ok := extractLogAttributeFromNestedMessage(attribute, fields, keys); ok {
		return value, true
	}
	if value, ok := extractLogAttributeDirect(attribute, fields, keys); ok {
		return value, true
	}
	return extractLogAttributeFromDDTags(attribute, fields, keys)
}

func sortedFieldKeys(fields map[string]string) []string {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func extractLogAttributeFromNestedMessage(attribute string, fields map[string]string, keys []string) (string, bool) {
	for _, k := range keys {
		if k == "message" || strings.HasSuffix(k, ".message") {
			if val, ok := nestedJSONField(fields, k, attribute); ok {
				return val, true
			}
		}
	}
	return "", false
}

func extractLogAttributeDirect(attribute string, fields map[string]string, keys []string) (string, bool) {
	for _, k := range keys {
		if k == attribute || strings.HasSuffix(k, "."+attribute) {
			return fields[k], true
		}
		if attribute == "fidelity_correlation_id" && (k == "fidelity.correlation_id" || strings.HasSuffix(k, ".fidelity.correlation_id")) {
			return fields[k], true
		}
	}
	return "", false
}

func extractLogAttributeFromDDTags(attribute string, fields map[string]string, keys []string) (string, bool) {
	for _, k := range keys {
		if k == "ddtags" || strings.HasSuffix(k, ".ddtags") {
			if value, ok := findAttributeInDDTags(attribute, fields[k]); ok {
				return value, true
			}
		}
	}
	return "", false
}

func findAttributeInDDTags(attribute, ddtags string) (string, bool) {
	for tag := range strings.SplitSeq(ddtags, ",") {
		key, val, ok := parseTagKV(tag)
		if !ok {
			continue
		}
		if key == attribute {
			return val, true
		}
		if attribute == "fidelity_correlation_id" && key == "fidelity.correlation_id" {
			return val, true
		}
	}
	return "", false
}

func parseTagKV(tag string) (string, string, bool) {
	parts := strings.SplitN(tag, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}

func extractTraceAttribute(attribute string, fields map[string]string) (string, bool) {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	if attribute == "span_count" {
		prefixes := make(map[string]struct{})
		for _, k := range keys {
			if strings.HasSuffix(k, ".trace_id") || strings.HasSuffix(k, ".traceID") {
				idx := strings.LastIndex(k, ".")
				if idx != -1 {
					prefixes[k[:idx]] = struct{}{}
				}
			}
		}
		if len(prefixes) > 0 {
			return strconv.Itoa(len(prefixes)), true
		}
		return "", false
	}

	for _, k := range keys {
		if k == attribute || strings.HasSuffix(k, "."+attribute) {
			return fields[k], true
		}
	}
	return "", false
}
