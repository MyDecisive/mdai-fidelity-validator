package validator

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"slices"
	"strings"
)

type correlationResolution struct {
	CorrelationID string
	Strategy      string
	Field         string
	RawValue      string
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func deriveCorrelationFromFields(signal Signal, fields map[string]string) string {
	correlationID, _, _, ok := deriveCorrelationFromFieldsDetailed(signal, fields)
	if !ok {
		return ""
	}
	return correlationID
}

func deriveCorrelationFromFieldsDetailed(signal Signal, fields map[string]string) (string, string, string, bool) {
	for _, key := range correlationCandidates(signal, fields) {
		if candidate := correlationValueForField(key, fields[key]); candidate != "" {
			return string(signal) + ":" + candidate, key, fields[key], true
		}
	}
	return "", "", "", false
}

func resolveCorrelationIDFromDecoded(signal Signal, translatorCorrelationID string, fields map[string]string, headers http.Header, body []byte) correlationResolution {
	translatorCorrelationID = strings.TrimSpace(translatorCorrelationID)
	if signal == SignalTraces {
		// For traces, prefer span_id > trace_id > correlation_id from intrinsic fields before falling back to headers.
		if correlationID, field, rawValue, ok := deriveCorrelationFromFieldsDetailed(signal, fields); ok {
			return correlationResolution{
				CorrelationID: correlationID,
				Strategy:      "field",
				Field:         field,
				RawValue:      rawValue,
			}
		}
		return resolveCorrelationID(signal, fields, headers, body)
	}
	if translatorCorrelationID != "" {
		return correlationResolution{
			CorrelationID: string(signal) + ":" + translatorCorrelationID,
			Strategy:      "translator",
			Field:         "correlation_id",
			RawValue:      translatorCorrelationID,
		}
	}
	return resolveCorrelationID(signal, fields, headers, body)
}

func resolveCorrelationID(signal Signal, fields map[string]string, headers http.Header, body []byte) correlationResolution {
	if headerKey, headerValue := firstHeaderValue(headers, "X-Correlation-ID", "X-Request-ID"); headerValue != "" {
		return correlationResolution{
			CorrelationID: string(signal) + ":" + headerValue,
			Strategy:      "header",
			Field:         headerKey,
			RawValue:      headerValue,
		}
	}
	if correlationID, field, rawValue, ok := deriveCorrelationFromFieldsDetailed(signal, fields); ok {
		return correlationResolution{
			CorrelationID: correlationID,
			Strategy:      "field",
			Field:         field,
			RawValue:      rawValue,
		}
	}
	if len(fields) > 0 {
		return correlationResolution{
			CorrelationID: deriveFingerprintCorrelationID(signal, fields),
			Strategy:      "fingerprint",
		}
	}
	return correlationResolution{
		CorrelationID: deriveRawBodyCorrelationID(signal, body),
		Strategy:      "raw_body",
	}
}

func firstHeaderValue(headers http.Header, keys ...string) (string, string) {
	for _, key := range keys {
		if value := headers.Get(key); value != "" {
			return key, value
		}
	}
	return "", ""
}

func deriveFingerprintCorrelationID(signal Signal, fields map[string]string) string {
	identityFields := map[Signal][]string{
		SignalTraces:  {"trace_id", "traceID", "[0][0].trace_id"},
		SignalMetrics: {"metric_name", "point_timestamp", "series[0].metric", "series[0].points[0][0]"},
		SignalLogs:    {"message", "timestamp", "attributes.http.url"},
	}

	builder := strings.Builder{}
	builder.WriteString(string(signal))

	foundIdentity := false
	if keys, ok := identityFields[signal]; ok {
		for _, key := range keys {
			if val, ok := fields[key]; ok && val != "" {
				builder.WriteString("|" + key + "=" + val)
				foundIdentity = true
			}
		}
	}

	if !foundIdentity {
		stableTags := []string{"service", "env", "version", "meta.service", "meta.env"}
		keys := make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
		slices.Sort(keys)

		for _, tag := range stableTags {
			for _, fieldKey := range keys {
				val := fields[fieldKey]
				if strings.Contains(fieldKey, "tags") && strings.Contains(val, tag+":") {
					builder.WriteString("|" + fieldKey + "=" + val)
				}
			}
		}
	}

	hash := sha256.Sum256([]byte(builder.String()))
	return string(signal) + ":fp:" + hex.EncodeToString(hash[:12])
}

func deriveRawBodyCorrelationID(signal Signal, body []byte) string {
	hash := sha256.Sum256(body)
	return string(signal) + ":" + hex.EncodeToString(hash[:8])
}

func correlationCandidates(signal Signal, fields map[string]string) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	var candidates []string
	seen := make(map[string]struct{})
	add := func(key string) {
		if _, ok := seen[key]; ok {
			return
		}
		if _, ok := fields[key]; ok {
			seen[key] = struct{}{}
			candidates = append(candidates, key)
		}
	}

	// Exact canonical key priority differs by signal.
	// Traces: trace_id > span_id > correlation_id.
	// Others: correlation_id > trace_id.
	if signal == SignalTraces {
		for _, key := range []string{"trace_id", "span_id", "correlation_id"} {
			add(key)
		}
		// Suffix fallbacks for grouped trace payloads (fields like "[0].trace_id").
		// trace_id is preferred over span_id here: all spans in a group share the same
		// trace_id, so it is stable regardless of span ordering within the chunk.
		for _, key := range keys {
			if strings.HasSuffix(strings.ToLower(key), ".trace_id") {
				add(key)
			}
		}
		for _, key := range keys {
			if strings.HasSuffix(strings.ToLower(key), ".span_id") {
				add(key)
			}
		}
	} else {
		for _, key := range []string{"correlation_id", "trace_id"} {
			add(key)
		}
	}

	for _, key := range keys {
		lower := strings.ToLower(key)
		valueLower := strings.ToLower(fields[key])
		switch {
		case strings.HasSuffix(lower, ".correlation_id"),
			strings.Contains(lower, ".correlation_id."),
			strings.Contains(lower, "correlationid"),
			strings.HasSuffix(lower, ".ddtags"),
			lower == "ddtags",
			strings.Contains(lower, "tags[") && strings.Contains(valueLower, "correlation_id:"):
			add(key)
		default:
		}
	}

	return candidates
}

func correlationValueForField(key, value string) string {
	lowerKey := strings.ToLower(key)
	if strings.Contains(lowerKey, "tags[") {
		if parsed := parseCorrelationTag(value); parsed != "" {
			return parsed
		}
	}
	if strings.HasSuffix(lowerKey, ".ddtags") || lowerKey == "ddtags" || strings.HasSuffix(lowerKey, "ddtags") {
		for part := range strings.SplitSeq(value, ",") {
			if parsed := parseCorrelationTag(strings.TrimSpace(part)); parsed != "" {
				return parsed
			}
		}
	}
	return value
}

func parseCorrelationTag(tag string) string {
	switch {
	case strings.HasPrefix(tag, "correlation_id:"):
		return strings.TrimPrefix(tag, "correlation_id:")
	default:
		return ""
	}
}
