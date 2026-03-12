package validator

import "testing"

func TestExtractLogAttributeFromWrappedExporterLog(t *testing.T) {
	fields := map[string]string{
		"[0].ddtags":  "env:dev,correlation_id:corr-1,fidelity.correlation_id:corr-1,otel_source:datadog_exporter",
		"[0].message": `{"ddsource":"mdai-dd-fidelity-validator","hostname":"localhost","message":"ddgen synthetic log event","service":"ddgen-svc","status":"info","timestamp":1773343955346}`,
	}

	service, ok := extractLogAttribute("service", fields)
	if !ok || service != "ddgen-svc" {
		t.Fatalf("service=%q ok=%v", service, ok)
	}

	message, ok := extractLogAttribute("message", fields)
	if !ok || message != "ddgen synthetic log event" {
		t.Fatalf("message=%q ok=%v", message, ok)
	}

	correlation, ok := extractLogAttribute("correlation_id", fields)
	if !ok || correlation != "corr-1" {
		t.Fatalf("correlation=%q ok=%v", correlation, ok)
	}

	fidelity, ok := extractLogAttribute("fidelity_correlation_id", fields)
	if !ok || fidelity != "corr-1" {
		t.Fatalf("fidelity=%q ok=%v", fidelity, ok)
	}
}

func TestExtractTraceAttributesFromReceiverAndExporterShapes(t *testing.T) {
	receiver := map[string]string{
		"[0][0].trace_id":                     "trace-1",
		"[0][0].service":                      "svc-a",
		"[0][0].meta.correlation_id":          "corr-1",
		"[0][0].meta.fidelity.correlation_id": "corr-1",
		"[0][1].trace_id":                     "trace-1",
	}
	exporter := map[string]string{
		"tracerPayloads[0].chunks[0].spans[0].trace_id":                     "trace-1",
		"tracerPayloads[0].chunks[0].spans[0].service":                      "svc-a",
		"tracerPayloads[0].chunks[0].spans[0].meta.correlation_id":          "corr-1",
		"tracerPayloads[0].chunks[0].spans[0].meta.fidelity.correlation_id": "corr-1",
		"tracerPayloads[0].chunks[0].spans[1].trace_id":                     "trace-1",
	}

	for _, tc := range []struct {
		name   string
		fields map[string]string
	}{
		{name: "receiver", fields: receiver},
		{name: "exporter", fields: exporter},
	} {
		traceID, ok := extractTraceAttribute("trace_id", tc.fields)
		if !ok || traceID != "trace-1" {
			t.Fatalf("%s trace_id=%q ok=%v", tc.name, traceID, ok)
		}
		service, ok := extractTraceAttribute("service", tc.fields)
		if !ok || service != "svc-a" {
			t.Fatalf("%s service=%q ok=%v", tc.name, service, ok)
		}
		spanCount, ok := extractTraceAttribute("span_count", tc.fields)
		if !ok || spanCount != "2" {
			t.Fatalf("%s span_count=%q ok=%v", tc.name, spanCount, ok)
		}
	}
}
