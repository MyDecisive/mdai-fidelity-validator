package validator

import (
	"bytes"
	"compress/zlib"
	"testing"

	ddmetrics "github.com/DataDog/agent-payload/v5/gogen"
	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeBodyDatadogSeriesProto(t *testing.T) {
	t.Parallel()

	body := encodeDatadogSeriesPayload(
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

func TestDatadogFieldMappingExtractsCorrelationFromSeriesProto(t *testing.T) {
	setDefaultFieldMappingPath(t)
	mapping, _, err := loadFieldMapping()
	require.NoError(t, err)

	body := deflateBytes(t, encodeDatadogSeriesPayload(
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

func encodeDatadogSeriesPayload(metric string, timestamp int64, value float64, tags []string) []byte {
	payload := ddmetrics.MetricPayload{
		Series: []*ddmetrics.MetricPayload_MetricSeries{
			{
				Metric: metric,
				Tags:   tags,
				Type:   ddmetrics.MetricPayload_GAUGE,
				Points: []*ddmetrics.MetricPayload_MetricPoint{
					{Timestamp: timestamp, Value: value},
				},
				Interval: 60,
			},
		},
	}
	b, err := gogoproto.Marshal(&payload)
	if err != nil {
		panic(err)
	}
	return b
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
