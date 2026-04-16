package validator

import (
	"bytes"
	"compress/zlib"
	"math"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

func TestDecodeBodyDatadogSeriesProtoV3(t *testing.T) {
	body := encodeDatadogSeriesV3Payload(
		"app.request.count",
		1710000000,
		12.5,
		[]string{
			"service:checkout",
			"env:prod",
			"correlation_id:corr-123",
		},
	)

	compressed := deflateBytes(t, body)
	payload, format, err := decodeBody(
		compressed,
		"/exporter/datadog/api/v2/series",
		"deflate",
		"application/x-protobuf",
	)
	if err != nil {
		t.Fatalf("decodeBody() error = %v", err)
	}
	if format != "protobuf" {
		t.Fatalf("decodeBody() format = %q want %q", format, "protobuf")
	}

	fields := flattenValueMap(payload)
	if got := fields["series[0].metric"]; got != "app.request.count" {
		t.Fatalf("series[0].metric = %q", got)
	}
	if got := fields["series[0].tags[2]"]; got != "correlation_id:corr-123" {
		t.Fatalf("series[0].tags[2] = %q", got)
	}
	if got := fields["series[0].points[0][0]"]; got != "1710000000" {
		t.Fatalf("series[0].points[0][0] = %q", got)
	}
	if got := fields["series[0].points[0][1]"]; got != "12.5" {
		t.Fatalf("series[0].points[0][1] = %q", got)
	}
}

func TestDatadogFieldMappingExtractsCorrelationFromSeriesProtoV3(t *testing.T) {
	setDefaultFieldMappingPath(t)
	mapping, _, err := loadFieldMapping()
	if err != nil {
		t.Fatalf("loadFieldMapping: %v", err)
	}

	body := deflateBytes(t, encodeDatadogSeriesV3Payload(
		"app.request.count",
		1710000000,
		12.5,
		[]string{
			"service:checkout",
			"env:prod",
			"correlation_id:corr-123",
		},
	))

	payload, format, err := decodeBody(
		body,
		"/exporter/datadog/api/v2/series",
		"deflate",
		"application/x-protobuf",
	)
	if err != nil {
		t.Fatalf("decodeBody() error = %v", err)
	}
	if format != "protobuf" {
		t.Fatalf("format = %q want %q", format, "protobuf")
	}

	fields := mapping.MapForPath(SignalMetrics, "/exporter/datadog/api/v2/series", flattenValueMap(payload))
	if got := fields["correlation_id"]; got != "corr-123" {
		t.Fatalf("correlation_id = %q", got)
	}
	if got := fields["metric_name"]; got != "app.request.count" {
		t.Fatalf("metric_name = %q", got)
	}
	if got := fields["service"]; got != "checkout" {
		t.Fatalf("service = %q", got)
	}
	if got := fields["env"]; got != "prod" {
		t.Fatalf("env = %q", got)
	}
	if got := fields["point_value"]; got != "12.5" {
		t.Fatalf("point_value = %q", got)
	}
}

func encodeDatadogSeriesV3Payload(metric string, timestamp int64, value float64, tags []string) []byte {
	metricData := make([]byte, 0)

	metricData = appendBytesField(metricData, 1, encodeStringTable([]string{metric}))
	if len(tags) > 0 {
		metricData = appendBytesField(metricData, 2, encodeStringTable(tags))

		tagset := []int64{int64(len(tags))}
		for range tags {
			tagset = append(tagset, 1)
		}
		metricData = appendPackedSint64Field(metricData, 3, tagset)
	}

	metricData = appendPackedUint64Field(metricData, 10, []uint64{datadogValueTypeFloat64 | datadogMetricTypeGauge})
	metricData = appendPackedSint64Field(metricData, 11, []int64{1})
	if len(tags) > 0 {
		metricData = appendPackedSint64Field(metricData, 12, []int64{1})
	}
	metricData = appendPackedUint64Field(metricData, 14, []uint64{60})
	metricData = appendPackedUint64Field(metricData, 15, []uint64{1})
	metricData = appendPackedSint64Field(metricData, 16, []int64{timestamp})
	metricData = appendPackedFloat64Field(metricData, 19, []float64{value})

	payload := make([]byte, 0)
	payload = appendBytesField(payload, 3, metricData)
	return payload
}

func encodeStringTable(values []string) []byte {
	table := make([]byte, 0)
	for _, value := range values {
		table = protowire.AppendString(table, value)
	}
	return table
}

func appendBytesField(dst []byte, num protowire.Number, value []byte) []byte {
	dst = protowire.AppendTag(dst, num, protowire.BytesType)
	return protowire.AppendBytes(dst, value)
}

func appendPackedUint64Field(dst []byte, num protowire.Number, values []uint64) []byte {
	packed := make([]byte, 0)
	for _, value := range values {
		packed = protowire.AppendVarint(packed, value)
	}
	return appendBytesField(dst, num, packed)
}

func appendPackedSint64Field(dst []byte, num protowire.Number, values []int64) []byte {
	packed := make([]byte, 0)
	for _, value := range values {
		packed = protowire.AppendVarint(packed, protowire.EncodeZigZag(value))
	}
	return appendBytesField(dst, num, packed)
}

func appendPackedFloat64Field(dst []byte, num protowire.Number, values []float64) []byte {
	packed := make([]byte, 0)
	for _, value := range values {
		packed = protowire.AppendFixed64(packed, mathFloat64bits(value))
	}
	return appendBytesField(dst, num, packed)
}

func deflateBytes(t *testing.T, body []byte) []byte {
	t.Helper()

	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		t.Fatalf("writer.Write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}
	return compressed.Bytes()
}

func mathFloat64bits(value float64) uint64 {
	return math.Float64bits(value)
}
