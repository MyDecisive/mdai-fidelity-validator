package validator

import (
	"errors"
	"fmt"
	"strings"

	dptrace "github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace"
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

	// TracerPayload is the agent/receiver format; AgentPayload wraps multiple TracerPayloads
	// and is what the OTel Datadog exporter sends to /api/v0.2/traces.
	if spans, err := spansFromTracerPayload(body); err == nil && len(spans) > 0 {
		return groupSpansByTraceID(spans), nil
	}
	spans, err := spansFromAgentPayload(body)
	if err != nil {
		return nil, fmt.Errorf("unmarshal trace payload: %w", err)
	}
	if len(spans) == 0 {
		return nil, errors.New("trace payload had no spans")
	}
	return groupSpansByTraceID(spans), nil
}

func spansFromTracerPayload(body []byte) ([]map[string]any, error) {
	var payload dptrace.TracerPayload
	if err := payload.UnmarshalVT(body); err != nil {
		return nil, err
	}
	var spans []map[string]any
	for _, chunk := range payload.GetChunks() {
		for _, s := range chunk.GetSpans() {
			spans = append(spans, spanToMap(s))
		}
	}
	return spans, nil
}

func spansFromAgentPayload(body []byte) ([]map[string]any, error) {
	var payload dptrace.AgentPayload
	if err := payload.UnmarshalVT(body); err != nil {
		return nil, err
	}
	var spans []map[string]any
	for _, tp := range payload.GetTracerPayloads() {
		for _, chunk := range tp.GetChunks() {
			for _, s := range chunk.GetSpans() {
				spans = append(spans, spanToMap(s))
			}
		}
	}
	return spans, nil
}

func spanToMap(s *dptrace.Span) map[string]any {
	span := map[string]any{
		"trace_id": s.GetTraceID(),
		"span_id":  s.GetSpanID(),
		"start":    s.GetStart(),
		"duration": s.GetDuration(),
	}
	if s.GetService() != "" {
		span["service"] = s.GetService()
	}
	if s.GetName() != "" {
		span["name"] = s.GetName()
	}
	if s.GetResource() != "" {
		span["resource"] = s.GetResource()
	}
	if s.GetParentID() != 0 {
		span["parent_id"] = s.GetParentID()
	}
	if s.GetType() != "" {
		span["type"] = s.GetType()
	}
	if len(s.GetMeta()) > 0 {
		meta := make(map[string]any, len(s.GetMeta()))
		for k, v := range s.GetMeta() {
			meta[k] = v
		}
		span["meta"] = meta
	}
	if len(s.GetMetrics()) > 0 {
		metrics := make(map[string]any, len(s.GetMetrics()))
		for k, v := range s.GetMetrics() {
			metrics[k] = v
		}
		span["metrics"] = metrics
	}
	return span
}

func groupSpansByTraceID(spans []map[string]any) map[string]any {
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
	return map[string]any{"traces": traces}
}
