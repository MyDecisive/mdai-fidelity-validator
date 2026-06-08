package validator

import (
	"encoding/json"
	"slices"
	"strings"
	"time"
)

func comparePair(a, b *observedPayload, policy Policy) ComparisonResult {
	receiver := a
	exporter := b
	if receiver.source != "receiver" {
		receiver, exporter = exporter, receiver
	}

	receiverCompareFields := normalizeFieldsForComparison(receiver.signal, receiver.flattened)
	exporterCompareFields := normalizeFieldsForComparison(exporter.signal, exporter.flattened)

	result := ComparisonResult{
		Signal:            receiver.signal,
		CorrelationID:     receiver.correlation,
		ReceiverFields:    receiverCompareFields,
		ExporterFields:    exporterCompareFields,
		ReceiverRawFields: receiver.flattened,
		ExporterRawFields: exporter.flattened,
		ReceiverWire:      receiver.request,
		ExporterWire:      exporter.request,
		ComparedAt:        time.Now().UTC(),
	}

	allKeys := mergeAndSortKeys(receiverCompareFields, exporterCompareFields)
	for _, key := range allKeys {
		receiverValue, receiverOK := receiverCompareFields[key]
		exporterValue, exporterOK := exporterCompareFields[key]

		switch {
		case receiverOK && exporterOK && receiverValue == exporterValue:
			result.Matched = append(result.Matched, key)
		case receiverOK && exporterOK:
			result.Mismatched = append(result.Mismatched, AttributeDelta{
				Attribute: key,
				Receiver:  receiverValue,
				Exporter:  exporterValue,
			})
		case receiverOK:
			result.MissingIn = append(result.MissingIn, MissingField{
				Attribute: key,
				Side:      "exporter",
				Value:     receiverValue,
			})
		default:
			result.MissingIn = append(result.MissingIn, MissingField{
				Attribute: key,
				Side:      "receiver",
				Value:     exporterValue,
			})
		}
	}

	result.AttributeTotal = len(allKeys)
	if result.Signal == SignalTraces && (receiver.spans != nil || exporter.spans != nil) {
		result.Spans = compareSpans(receiver.spans, exporter.spans)
	}
	spansMatch := true
	for _, s := range result.Spans {
		if !s.Passed || s.OnlyIn != "" {
			spansMatch = false
			break
		}
	}
	result.FullPayloadPassed = len(result.Mismatched) == 0 && // no mismatches
		len(result.MissingIn) == 0 && // no missing
		spansMatch // spans good
	result.RequiredChecks, result.Passed = evaluatePolicy(result.Signal, receiver.flattened, exporter.flattened, policy)
	return result
}

func mergeAndSortKeys(a, b map[string]string) []string {
	allKeys := make(map[string]struct{}, len(a)+len(b))
	for key := range a {
		allKeys[key] = struct{}{}
	}
	for key := range b {
		allKeys[key] = struct{}{}
	}

	keys := make([]string, 0, len(allKeys))
	for key := range allKeys {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func normalizeFieldsForComparison(signal Signal, fields map[string]string) map[string]string {
	normalized := make(map[string]string, len(fields))
	for key, value := range fields {
		normalizedKey := normalizeFieldKeyForComparison(signal, key)
		if shouldSkipComparisonField(normalizedKey) {
			continue
		}
		normalized[normalizedKey] = normalizeFieldValueForComparison(signal, normalizedKey, value)
	}
	return normalized
}

func shouldSkipComparisonField(key string) bool {
	return key == "correlation_id"
}

func normalizeFieldKeyForComparison(signal Signal, key string) string {
	if signal == "logs" && strings.HasPrefix(key, "[0].") {
		return strings.TrimPrefix(key, "[0].")
	}
	return key
}

func normalizeFieldValueForComparison(signal Signal, key, value string) string {
	if signal != SignalLogs {
		return value
	}

	lowerKey := strings.ToLower(key)
	switch {
	case lowerKey == "ddtags" || strings.HasSuffix(lowerKey, ".ddtags"):
		return stripCorrelationFromDDTags(value)
	case lowerKey == "message" || strings.HasSuffix(lowerKey, ".message"):
		return stripCorrelationFromMessageJSON(value)
	default:
		return value
	}
}

func stripCorrelationFromDDTags(tags string) string {
	if tags == "" {
		return tags
	}

	parts := strings.Split(tags, ",")
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		kv := strings.SplitN(trimmed, ":", 2)
		if len(kv) != 2 {
			if trimmed != "" {
				filtered = append(filtered, trimmed)
			}
			continue
		}
		key := strings.TrimSpace(kv[0])
		if key == "correlation_id" {
			continue
		}
		filtered = append(filtered, trimmed)
	}
	return strings.Join(filtered, ",")
}

func stripCorrelationFromMessageJSON(message string) string {
	trimmed := strings.TrimSpace(message)
	if !strings.HasPrefix(trimmed, "{") {
		return message
	}

	if !strings.Contains(trimmed, `"correlation_id"`) {
		return message
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return message
	}
	delete(payload, "correlation_id")

	normalized, err := json.Marshal(payload)
	if err != nil {
		return message
	}
	return string(normalized)
}
