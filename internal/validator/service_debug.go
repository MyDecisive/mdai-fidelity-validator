package validator

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"go.uber.org/zap"
)

func logCorrelationDecision(s *Service, source string, signal Signal, decision correlationResolution) {
	switch decision.Strategy {
	case "field", "header":
		s.logger.Info("correlation selection",
			zap.String("source", source),
			zap.String("signal", string(signal)),
			zap.String("strategy", decision.Strategy),
			zap.String("key", decision.Field),
			zap.String("raw_value", decision.RawValue),
			zap.String("correlation_id", decision.CorrelationID),
		)
	default:
		s.logger.Info("correlation selection",
			zap.String("source", source),
			zap.String("signal", string(signal)),
			zap.String("strategy", decision.Strategy),
			zap.String("correlation_id", decision.CorrelationID),
		)
	}
}

func logObservedPayload(s *Service, payload *observedPayload) {
	fields := []zap.Field{
		zap.String("source", payload.source),
		zap.String("signal", string(payload.signal)),
		zap.String("correlation_id", payload.correlation),
		zap.String("format", payload.format),
	}
	if payload.decodeError != "" {
		fields = append(fields, zap.String("decode_error", payload.decodeError))
	}
	s.logger.Info("captured payload", fields...)
	if ce := s.logger.Check(zap.DebugLevel, "captured payload detail"); ce != nil {
		debug := sanitizedDebugPayloadFromObserved(payload)
		body, err := json.Marshal(debug)
		if err != nil {
			ce.Write(zap.Error(err))
			return
		}
		ce.Write(zap.String("json", string(body)))
	}
}

func sanitizedDebugPayloadFromObserved(payload *observedPayload) DebugPayload {
	debug := debugPayloadFromObserved(payload)
	debug.Request.Headers = sanitizedHeaders(debug.Request.Headers)
	debug.Attributes = sanitizedAttributes(debug.Attributes)
	if shouldIncludeRawPayload(payload) {
		var rawBody string
		if utf8.Valid(payload.body) {
			rawBody = string(payload.body)
		} else {
			rawBody = base64.StdEncoding.EncodeToString(payload.body)
		}
		debug.RawBody = sanitizedRawPayload(rawBody)
	}
	return debug
}

func sanitizedComparisonResult(result ComparisonResult) ComparisonResult {
	result.ReceiverFields = sanitizedAttributes(result.ReceiverFields)
	result.ExporterFields = sanitizedAttributes(result.ExporterFields)
	result.ReceiverRawFields = sanitizedAttributes(result.ReceiverRawFields)
	result.ExporterRawFields = sanitizedAttributes(result.ExporterRawFields)
	result.Mismatched = sanitizedAttributeDeltas(result.Mismatched)
	result.MissingIn = sanitizedMissingFields(result.MissingIn)
	result.ReceiverWire.Headers = sanitizedHeaders(result.ReceiverWire.Headers)
	result.ExporterWire.Headers = sanitizedHeaders(result.ExporterWire.Headers)
	return result
}

func sanitizedHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return headers
	}
	sanitized := make(map[string]string, len(headers))
	for key, value := range headers {
		if isSensitiveFieldName(key) {
			sanitized[key] = "[REDACTED]"
			continue
		}
		sanitized[key] = value
	}
	return sanitized
}

func sanitizedAttributes(attributes map[string]string) map[string]string {
	if len(attributes) == 0 {
		return attributes
	}
	sanitized := make(map[string]string, len(attributes))
	for key, value := range attributes {
		if isSensitiveFieldName(key) {
			sanitized[key] = "[REDACTED]"
			continue
		}
		sanitized[key] = value
	}
	return sanitized
}

func sanitizedAttributeDeltas(deltas []AttributeDelta) []AttributeDelta {
	if len(deltas) == 0 {
		return deltas
	}
	sanitized := make([]AttributeDelta, len(deltas))
	for i, delta := range deltas {
		sanitized[i] = delta
		if isSensitiveFieldName(delta.Attribute) {
			sanitized[i].Receiver = "[REDACTED]"
			sanitized[i].Exporter = "[REDACTED]"
		}
	}
	return sanitized
}

func sanitizedMissingFields(fields []MissingField) []MissingField {
	if len(fields) == 0 {
		return fields
	}
	sanitized := make([]MissingField, len(fields))
	for i, field := range fields {
		sanitized[i] = field
		if isSensitiveFieldName(field.Attribute) {
			sanitized[i].Value = "[REDACTED]"
		}
	}
	return sanitized
}

func shouldIncludeRawPayload(payload *observedPayload) bool {
	return strings.Contains(strings.ToLower(payload.decodeError), "decode")
}

func sanitizedRawPayload(rawBody string) string {
	var decoded any
	if err := json.Unmarshal([]byte(rawBody), &decoded); err == nil {
		sanitizeJSONValue(decoded)
		if body, err := json.Marshal(decoded); err == nil {
			return string(body)
		}
	}
	return "[unparseable payload redacted]"
}

func sanitizeJSONValue(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			if isSensitiveFieldName(key) {
				typed[key] = "[REDACTED]"
				continue
			}
			sanitizeJSONValue(item)
		}
	case []any:
		for _, item := range typed {
			sanitizeJSONValue(item)
		}
	default:
		return
	}
}

func isSensitiveFieldName(name string) bool {
	normalized := strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
	parts := strings.FieldsFunc(normalized, func(r rune) bool {
		return r == '.' || r == '[' || r == ']'
	})
	return slices.Contains(parts, "DD_API_KEY")
}

func logComparisonSummary(s *Service, result ComparisonResult) {
	requiredPassed := 0
	requiredFailed := make([]string, 0)
	for _, check := range result.RequiredChecks {
		if check.Passed {
			requiredPassed++
			continue
		}
		requiredFailed = append(requiredFailed, check.Attribute+":"+check.Reason)
	}

	s.logger.Info("comparison result",
		zap.String("signal", string(result.Signal)),
		zap.String("correlation_id", result.CorrelationID),
		zap.Bool("policy_pass", result.Passed),
		zap.Bool("full_payload_pass", result.FullPayloadPassed),
		zap.Int("matched", len(result.Matched)),
		zap.Int("mismatched", len(result.Mismatched)),
		zap.Int("missing", len(result.MissingIn)),
		zap.Int("required_passed", requiredPassed),
		zap.Int("required_total", len(result.RequiredChecks)),
		zap.Strings("mismatched_attributes", mismatchAttributeNames(result.Mismatched)),
		zap.Strings("missing_attributes", missingAttributeNames(result.MissingIn)),
		zap.Strings("required_failed", requiredFailed),
	)
}

func mismatchAttributeNames(deltas []AttributeDelta) []string {
	names := make([]string, 0, len(deltas))
	for _, delta := range deltas {
		names = append(names, delta.Attribute)
	}
	return names
}

func missingAttributeNames(missing []MissingField) []string {
	names := make([]string, 0, len(missing))
	for _, item := range missing {
		names = append(names, item.Attribute+":"+item.Side)
	}
	return names
}

func summarizePolicy(policy Policy) string {
	signals := make([]string, 0, len(policy.Signals))
	for signal, signalPolicy := range policy.Signals {
		signals = append(signals, fmt.Sprintf("%s:%d", signal, len(signalPolicy.RequiredAttributes)))
	}
	slices.Sort(signals)
	return strings.Join(signals, ",")
}
