package ddgen

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestBuildRequestJSON(t *testing.T) {
	req, err := BuildRequest(Options{
		Signal:   SignalMetrics,
		Encoding: EncodingJSON,
		Service:  "checkout",
	})
	if err != nil {
		t.Fatalf("BuildRequest error: %v", err)
	}
	if req.Path != "/api/v1/series" {
		t.Fatalf("unexpected path %q", req.Path)
	}
	if req.ContentType != ContentTypeJSON {
		t.Fatalf("unexpected content type %q", req.ContentType)
	}

	var payload map[string]any
	if err := json.Unmarshal(req.Body, &payload); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if _, ok := payload["series"]; !ok {
		t.Fatalf("expected series in payload: %#v", payload)
	}
}

func TestBuildRequestMsgpack(t *testing.T) {
	req, err := BuildRequest(Options{
		Signal:   SignalTraces,
		Encoding: EncodingMsgpack,
		Gzip:     true,
	})
	if err != nil {
		t.Fatalf("BuildRequest error: %v", err)
	}
	if req.ContentType != ContentTypeMsgpack {
		t.Fatalf("unexpected content type %q", req.ContentType)
	}
	if req.ContentEncoding != ContentEncodingGzip {
		t.Fatalf("unexpected content encoding %q", req.ContentEncoding)
	}
}

func TestBuildLogsMsgpackRoundTrip(t *testing.T) {
	req, err := BuildRequest(Options{
		Signal:   SignalLogs,
		Encoding: EncodingMsgpack,
	})
	if err != nil {
		t.Fatalf("BuildRequest error: %v", err)
	}

	var payload any
	if err := msgpack.Unmarshal(req.Body, &payload); err != nil {
		t.Fatalf("msgpack unmarshal: %v", err)
	}

	payloadMap, ok := payload.(map[string]any)
	if !ok {
		t.Fatalf("expected top-level map, got %T", payload)
	}
	if _, ok := payloadMap["message"]; !ok {
		t.Fatalf("expected message field in %#v", payloadMap)
	}
	if _, ok := payloadMap["timestamp"].(int64); !ok {
		t.Fatalf("expected int64 timestamp in %#v", payloadMap)
	}
}

func TestMetricsRejectMsgpack(t *testing.T) {
	_, err := BuildRequest(Options{
		Signal:   SignalMetrics,
		Encoding: EncodingMsgpack,
	})
	if err == nil {
		t.Fatal("expected error for msgpack metrics")
	}
}

func TestBuildRequestWithCorrelationAndDropsPreservesCorrelationTags(t *testing.T) {
	req, err := BuildRequestWithCorrelation(Options{
		Signal:   SignalMetrics,
		Encoding: EncodingJSON,
	}, "corr-fixed", 1)
	if err != nil {
		t.Fatalf("BuildRequestWithCorrelation error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(req.Body, &payload); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}

	series := payload["series"].([]any)
	entry := series[0].(map[string]any)
	tags := entry["tags"].([]any)
	foundCorrelation := false
	for _, tag := range tags {
		if strings.HasPrefix(tag.(string), "correlation_id:corr-fixed") {
			foundCorrelation = true
		}
	}
	if !foundCorrelation {
		t.Fatalf("expected correlation_id tag in %#v", tags)
	}
}

func TestBuildRequestWithoutCorrelationID(t *testing.T) {
	req, err := BuildRequest(Options{
		Signal:          SignalMetrics,
		Encoding:        EncodingJSON,
		OmitCorrelation: true,
	})
	if err != nil {
		t.Fatalf("BuildRequest error: %v", err)
	}
	if req.CorrelationID != "" {
		t.Fatalf("expected empty correlation id, got %q", req.CorrelationID)
	}

	var payload map[string]any
	if err := json.Unmarshal(req.Body, &payload); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	series := payload["series"].([]any)
	entry := series[0].(map[string]any)
	tags := entry["tags"].([]any)
	for _, tag := range tags {
		tagValue := tag.(string)
		if strings.HasPrefix(tagValue, "correlation_id:") || strings.HasPrefix(tagValue, "fidelity.correlation_id:") {
			t.Fatalf("expected no correlation tags, got %#v", tags)
		}
	}
}

func TestBuildLogsWithoutCorrelationID(t *testing.T) {
	req, err := BuildRequest(Options{
		Signal:          SignalLogs,
		Encoding:        EncodingJSON,
		OmitCorrelation: true,
	})
	if err != nil {
		t.Fatalf("BuildRequest error: %v", err)
	}
	if req.CorrelationID != "" {
		t.Fatalf("expected empty correlation id, got %q", req.CorrelationID)
	}

	var payload map[string]any
	if err := json.Unmarshal(req.Body, &payload); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if _, ok := payload["correlation_id"]; ok {
		t.Fatalf("did not expect correlation_id in payload: %#v", payload)
	}
	if tags, ok := payload["ddtags"].(string); !ok || strings.Contains(tags, "correlation_id:") || strings.Contains(tags, "fidelity.correlation_id:") {
		t.Fatalf("did not expect correlation tags in ddtags=%q", payload["ddtags"])
	}
	attrs, ok := payload["attributes"].(map[string]any)
	if !ok {
		t.Fatalf("expected attributes map, got %#v", payload["attributes"])
	}
	if _, ok := attrs["fidelity.correlation_id"]; ok {
		t.Fatalf("did not expect fidelity.correlation_id in attributes: %#v", attrs)
	}
}
