package validator

import (
	"bytes"
	"compress/zlib"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestDecodeBodyDatadogSeriesProtoV3(t *testing.T) {
	t.Parallel()

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
	require.NoError(t, err)
	assert.Equal(t, "protobuf", format)

	fields := flattenValueMap(payload)
	assert.Equal(t, "app.request.count", fields["series[0].metric"])
	assert.Equal(t, "correlation_id:corr-123", fields["series[0].tags[2]"])
	assert.Equal(t, "1710000000", fields["series[0].points[0][0]"])
	assert.Equal(t, "12.5", fields["series[0].points[0][1]"])
}

func TestDatadogFieldMappingExtractsCorrelationFromSeriesProtoV3(t *testing.T) {
	setDefaultFieldMappingPath(t)
	mapping, _, err := loadFieldMapping()
	require.NoError(t, err)

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
	require.NoError(t, err)
	assert.Equal(t, "protobuf", format)

	fields := mapping.MapForPath(SignalMetrics, "/exporter/datadog/api/v2/series", flattenValueMap(payload))
	assert.Equal(t, "corr-123", fields["correlation_id"])
	assert.Equal(t, "app.request.count", fields["metric_name"])
	assert.Equal(t, "checkout", fields["service"])
	assert.Equal(t, "prod", fields["env"])
	assert.Equal(t, "12.5", fields["point_value"])
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
	_, err := writer.Write(body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Bytes()
}

func mathFloat64bits(value float64) uint64 {
	return math.Float64bits(value)
}
