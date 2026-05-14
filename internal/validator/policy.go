package validator

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	policyPathEnvVar = "MDAI_FIDELITY_RULES_PATH"
)

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
	RequiredAttributes []RequiredAttributePolicy `yaml:"required_attributes"` //nolint:tagliatelle
}

type RequiredAttributePolicy struct {
	Name    string `yaml:"name"`
	Compare string `yaml:"compare,omitempty"`
}

func (r *RequiredAttributePolicy) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind { //nolint:exhaustive
	case yaml.ScalarNode:
		var value string
		if err := node.Decode(&value); err != nil {
			return err
		}
		r.Name = strings.TrimSpace(value)
		r.Compare = ""
		return nil
	case yaml.MappingNode:
		var raw struct {
			Name      string `yaml:"name"`
			Attribute string `yaml:"attribute"`
			Compare   string `yaml:"compare"`
		}
		if err := node.Decode(&raw); err != nil {
			return err
		}
		name := strings.TrimSpace(raw.Name)
		if name == "" {
			name = strings.TrimSpace(raw.Attribute)
		}
		if name == "" {
			return errors.New("required attribute entry missing name/attribute")
		}
		r.Name = name
		r.Compare = strings.TrimSpace(raw.Compare)
		return nil
	default:
		return fmt.Errorf("unsupported required attribute entry kind %d", node.Kind)
	}
}

func (r *RequiredAttributePolicy) compareMode() string {
	if strings.EqualFold(r.Compare, "presence_only") {
		return "presence_only"
	}
	return "value"
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

	configuredPath := strings.TrimSpace(os.Getenv(policyPathEnvVar))
	if configuredPath != "" {
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

	return policy, "runtime-empty-defaults", nil
}

func defaultPolicy() Policy {
	return Policy{
		Signals: map[Signal]SignalPolicy{},
	}
}

func evaluatePolicy(signal Signal, receiver, exporter map[string]string, policy Policy) ([]RequiredAttributeCheck, bool) {
	signalPolicy, ok := policy.Signals[signal]
	if !ok || len(signalPolicy.RequiredAttributes) == 0 {
		return nil, true
	}

	checks := make([]RequiredAttributeCheck, 0, len(signalPolicy.RequiredAttributes))
	allPassed := true

	for _, required := range signalPolicy.RequiredAttributes {
		check := evaluateAttributeDeep(signal, required, receiver, exporter)
		checks = append(checks, check)
		if !check.Passed {
			allPassed = false
		}
	}

	return checks, allPassed
}

func evaluateAttributeDeep(signal Signal, required RequiredAttributePolicy, receiver, exporter map[string]string) RequiredAttributeCheck {
	attribute := required.Name
	presenceOnly := required.compareMode() == "presence_only"
	check := RequiredAttributeCheck{
		Attribute:   attribute,
		Passed:      true,
		TotalItems:  1,
		PassedItems: 1,
	}

	receiverValue, receiverOK := lookupRequiredAttribute(attribute, receiver)
	if !receiverOK {
		if signal != SignalLogs {
			return evaluateAttributeDeepLegacy(signal, required, receiver, exporter)
		}
		check.Passed = false
		check.PassedItems = 0
		check.Reason = "missing_in_receiver"
		return check
	}

	exporterValue, exporterOK := lookupRequiredAttribute(attribute, exporter)
	if !exporterOK {
		check.Passed = false
		check.PassedItems = 0
		check.Reason = "missing_in_exporter"
		check.Receiver = receiverValue
		return check
	}

	if !presenceOnly && receiverValue != exporterValue {
		check.Passed = false
		check.PassedItems = 0
		check.Reason = "value_mismatch"
		check.Receiver = receiverValue
		check.Exporter = exporterValue
	}

	return check
}

func evaluateAttributeDeepLegacy(signal Signal, required RequiredAttributePolicy, receiver, exporter map[string]string) RequiredAttributeCheck {
	attribute := required.Name
	presenceOnly := required.compareMode() == "presence_only"

	check := RequiredAttributeCheck{
		Attribute: attribute,
		Passed:    true,
	}

	receiverKeys := findKeysForAttribute(signal, attribute, receiver)
	if len(receiverKeys) == 0 {
		check.Passed = false
		check.Reason = "missing_in_receiver"
		return check
	}

	check.TotalItems = len(receiverKeys)
	for _, rKey := range receiverKeys {
		rVal := receiver[rKey]
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
		if !presenceOnly && rVal != eVal {
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

func findKeysForAttribute(signal Signal, attribute string, fields map[string]string) []string {
	switch signal {
	case SignalTraces:
		return findTraceKeysForAttribute(attribute, fields)
	case SignalMetrics:
		return findMetricKeysForAttribute(attribute, fields)
	case SignalLogs:
		return findLogKeysForAttribute(attribute, fields)
	default:
		return nil
	}
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

func lookupRequiredAttribute(attribute string, fields map[string]string) (string, bool) {
	if value, ok := fields[attribute]; ok {
		return value, true
	}
	return "", false
}
