package validator

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

func decodeDatadogProtoByPath(path string, body []byte) (any, error) {
	_, normalizedPath := parseExporterPath(path)

	switch {
	case strings.HasSuffix(normalizedPath, "/series"):
		return decodeDatadogSeriesProto(path, body)
	case strings.HasSuffix(normalizedPath, "/traces"):
		return decodeDatadogTraceProto(path, body)
	default:
		return nil, fmt.Errorf("protobuf payloads are not supported for path %q", normalizedPath)
	}
}

func decodeDatadogTraceProto(path string, body []byte) (any, error) {
	_, normalizedPath := parseExporterPath(path)
	if !strings.HasSuffix(normalizedPath, "/traces") {
		return nil, fmt.Errorf("protobuf trace payloads are not supported for path %q", normalizedPath)
	}

	spans, err := findDatadogTraceSpans(body)
	if err != nil {
		return nil, err
	}
	if len(spans) == 0 {
		return nil, errors.New("trace payload had no spans")
	}

	traceGroups := make(map[string][]any)
	order := make([]string, 0, len(spans))
	for _, span := range spans {
		traceID := fmt.Sprint(span["trace_id"])
		if _, ok := traceGroups[traceID]; !ok {
			order = append(order, traceID)
		}
		traceGroups[traceID] = append(traceGroups[traceID], span)
	}

	traces := make([]any, 0, len(order))
	for _, traceID := range order {
		traces = append(traces, traceGroups[traceID])
	}
	return map[string]any{"traces": traces}, nil
}

func findDatadogTraceSpans(body []byte) ([]map[string]any, error) {
	var spans []map[string]any
	if err := collectDatadogTraceSpans(body, &spans, 0); err != nil {
		return nil, err
	}
	return spans, nil
}

func collectDatadogTraceSpans(body []byte, spans *[]map[string]any, depth int) error {
	if depth > 16 {
		return nil
	}

	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return protowire.ParseError(n)
		}
		body = body[n:]

		switch typ {
		case protowire.BytesType:
			raw, next, err := consumeBytesField(typ, body)
			if err != nil {
				return err
			}
			body = next

			if !isProbableProtoMessage(raw) {
				continue
			}

			if span, ok, err := parseDatadogSpan(raw); err == nil && ok {
				*spans = append(*spans, span)
				continue
			}

			if len(raw) > 0 {
				if err := collectDatadogTraceSpans(raw, spans, depth+1); err != nil {
					return err
				}
			}
		default:
			next, err := skipFieldValue(num, typ, body)
			if err != nil {
				return err
			}
			body = next
		}
	}

	return nil
}

func isProbableProtoMessage(body []byte) bool {
	if len(body) == 0 {
		return false
	}

	parsedField := false
	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 || num <= 0 {
			return false
		}
		body = body[n:]

		next, err := skipFieldValue(num, typ, body)
		if err != nil {
			return false
		}
		body = next
		parsedField = true
	}

	return parsedField
}

func parseDatadogSpan(body []byte) (map[string]any, bool, error) {
	span := map[string]any{}
	meta := map[string]any{}
	metrics := map[string]any{}

	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return nil, false, protowire.ParseError(n)
		}
		body = body[n:]

		switch num {
		case 1:
			value, next, err := consumeStringField(typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
			span["service"] = value
		case 2:
			value, next, err := consumeStringField(typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
			span["name"] = value
		case 3:
			value, next, err := consumeStringField(typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
			span["resource"] = value
		case 4:
			value, next, err := consumeVarintField(typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
			span["trace_id"] = value
		case 5:
			value, next, err := consumeVarintField(typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
			span["span_id"] = value
		case 6:
			value, next, err := consumeVarintField(typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
			span["parent_id"] = value
		case 7:
			value, next, err := consumeVarintField(typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
			span["start"] = value
		case 8:
			value, next, err := consumeVarintField(typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
			span["duration"] = value
		case 10:
			raw, next, err := consumeBytesField(typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
			key, value, ok, err := parseDatadogStringMapEntry(raw)
			if err != nil {
				return nil, false, err
			}
			if ok {
				meta[key] = value
			}
		case 11:
			raw, next, err := consumeBytesField(typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
			key, value, ok, err := parseDatadogNumericMapEntry(raw)
			if err != nil {
				return nil, false, err
			}
			if ok {
				metrics[key] = value
			}
		case 12:
			value, next, err := consumeStringField(typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
			span["type"] = value
		default:
			next, err := skipFieldValue(num, typ, body)
			if err != nil {
				return nil, false, err
			}
			body = next
		}
	}

	if len(meta) > 0 {
		span["meta"] = meta
	}
	if len(metrics) > 0 {
		span["metrics"] = metrics
	}

	if !looksLikeDatadogSpan(span) {
		return nil, false, nil
	}
	return span, true, nil
}

func looksLikeDatadogSpan(span map[string]any) bool {
	required := []string{"trace_id", "span_id", "start", "duration"}
	for _, key := range required {
		if _, ok := span[key]; !ok {
			return false
		}
	}
	return true
}

func parseDatadogStringMapEntry(body []byte) (string, string, bool, error) {
	var (
		key   string
		value string
	)
	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return "", "", false, protowire.ParseError(n)
		}
		body = body[n:]

		switch num {
		case 1:
			parsed, next, err := consumeStringField(typ, body)
			if err != nil {
				return "", "", false, err
			}
			body = next
			key = parsed
		case 2:
			parsed, next, err := consumeStringField(typ, body)
			if err != nil {
				return "", "", false, err
			}
			body = next
			value = parsed
		default:
			next, err := skipFieldValue(num, typ, body)
			if err != nil {
				return "", "", false, err
			}
			body = next
		}
	}
	return key, value, key != "", nil
}

func parseDatadogNumericMapEntry(body []byte) (string, any, bool, error) {
	var (
		key    string
		value  any
		hasVal bool
	)
	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return "", nil, false, protowire.ParseError(n)
		}
		body = body[n:]

		switch num {
		case 1:
			parsed, next, err := consumeStringField(typ, body)
			if err != nil {
				return "", nil, false, err
			}
			body = next
			key = parsed
		case 2:
			parsed, next, err := consumeNumericField(typ, body)
			if err != nil {
				return "", nil, false, err
			}
			body = next
			value = parsed
			hasVal = true
		default:
			next, err := skipFieldValue(num, typ, body)
			if err != nil {
				return "", nil, false, err
			}
			body = next
		}
	}
	return key, value, key != "" && hasVal, nil
}

func consumeNumericField(typ protowire.Type, body []byte) (any, []byte, error) {
	switch typ {
	case protowire.VarintType:
		value, next, err := consumeVarintField(typ, body)
		if err != nil {
			return nil, nil, err
		}
		return value, next, nil
	case protowire.Fixed64Type:
		value, n := protowire.ConsumeFixed64(body)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		return math.Float64frombits(value), body[n:], nil
	case protowire.Fixed32Type:
		value, n := protowire.ConsumeFixed32(body)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		return float64(math.Float32frombits(value)), body[n:], nil
	default:
		return nil, nil, fmt.Errorf("expected numeric field, got %v", typ)
	}
}
