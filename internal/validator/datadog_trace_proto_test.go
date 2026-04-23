package validator

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeBodyDatadogTraceProto(t *testing.T) {
	t.Parallel()

	body := gzipBytesFromBase64(t, datadogTraceProtoFixtureBase64)
	payload, format, err := decodeBody(
		body,
		"/exporter/datadog/api/v0.2/traces",
		"gzip",
		"application/x-protobuf",
	)
	require.NoError(t, err)
	assert.Equal(t, "protobuf", format)

	fields := flattenValueMap(payload)
	assert.Equal(t, "8473898538427554651", fields["traces[0][0].trace_id"])
	assert.Equal(t, "2770530486580628686", fields["traces[0][0].span_id"])
	assert.Equal(t, "ddgen-515b0afa", fields["traces[0][0].service"])
	assert.Equal(t, "server.request", fields["traces[0][0].name"])
	assert.Equal(t, "ddgen.request", fields["traces[0][0].resource"])
	assert.Equal(t, "ed3b973e-609e-41df-81f9-999b0369e88d", fields["traces[0][0].meta.correlation_id"])
	assert.Equal(t, "web", fields["traces[0][0].type"])
	assert.Equal(t, "1", fields["traces[0][0].metrics._sampling_priority_v1"])
	assert.Equal(t, "2189202486988202691", fields["traces[0][1].span_id"])
	assert.Equal(t, "2770530486580628686", fields["traces[0][1].parent_id"])
	assert.Equal(t, "custom", fields["traces[0][1].type"])
}

func TestDatadogFieldMappingExtractsCorrelationFromTraceProto(t *testing.T) {
	setDefaultFieldMappingPath(t)
	mapping, _, err := loadFieldMapping()
	require.NoError(t, err)

	body := gzipBytesFromBase64(t, datadogTraceProtoFixtureBase64)
	payload, format, err := decodeBody(
		body,
		"/exporter/datadog/api/v0.2/traces",
		"gzip",
		"application/x-protobuf",
	)
	require.NoError(t, err)
	assert.Equal(t, "protobuf", format)

	fields := mapping.MapForPath(SignalTraces, "/exporter/datadog/api/v0.2/traces", flattenValueMap(payload))
	assert.Equal(t, "ed3b973e-609e-41df-81f9-999b0369e88d", fields["correlation_id"])
	assert.Equal(t, "8473898538427554651", fields["trace_id"])
	assert.Equal(t, "2770530486580628686", fields["span_id"])
	assert.Equal(t, "ddgen-515b0afa", fields["service"])
	assert.Equal(t, "server.request", fields["operation"])
}

func gzipBytesFromBase64(t *testing.T, encoded string) []byte {
	t.Helper()

	body, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)

	reader, err := gzip.NewReader(bytes.NewReader(body))
	require.NoError(t, err)
	defer reader.Close() //nolint:errcheck

	decoded, err := ioReadAll(reader)
	require.NoError(t, err)
	return bodyFromDecoded(t, decoded)
}

func ioReadAll(reader *gzip.Reader) ([]byte, error) {
	var out bytes.Buffer
	_, err := out.ReadFrom(reader)
	return out.Bytes(), err
}

func bodyFromDecoded(t *testing.T, decoded []byte) []byte {
	t.Helper()

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(decoded)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Bytes()
}

const datadogTraceProtoFixtureBase64 = "H4sIAAAAAAAE/7yUTWgkRRTHx82HsxUyJh13WUYjY4i762L3VvdMd1fvxfgRZUVZKPe0l6a66s2kSU1Vp6pm4txkb95EL34gCJ5FDyKsR0W8iCAYRMRj8CriVZDpjThoWBMP1qmq4fXj/3+//0OPD5iDfTbx7Q4Tet/nWkrgThs/FaRPUpLy1N/hpEi9eaUVXDlY2ljQTlZ+9O5S87724QJqCTEA5cdhXGDWZ17LghmDCQzsjcC69rIQA1CBgfrZ+fHTTz786LODr0eXv3n7vfe/+uHOnYvkrTd/PXz92w++u7D16uE7+/QiekAwx4QeBLZiKiiFtxalKY67uEeSmOAkIglJ6DpatWxYyVINgsqU2pRu4jXDAGOMMaYt1MyFCKpADL0zfo+eRyvagQysY25kc64FeGdu7NI9tCiEYwPrDbg2BiRzpVZ5Ka6B6BZZ2gU/wRn4vVD0fRL2Mz/LsgJ3kwwIEU9wbQxI5kqt8vJkNfQSWvlToTOMQy2R9NIuyUjcJb0ojeNeEod0DZ2tLdgtlfAW7zpL15HnQMIQnJkEVuwGig3Bu/9ZVntGEZoDNfbmBIxpiJZrzXWXvBReZ2rN7EnjLItjEfVjJsIiLuijaEWIoO5KweqR4eAtP799s3OV7wDf1SNHH0ar9V9lWRhmJn/rn6AW17M+epsgusW/GUk30EMCKqknQ1AuADUujVb1vdY3N9WziuZ3tHXeWak5k9PrrcdQO7dHGOTVEQa5YQ7ycbjaqM8vT946h1DudJVLGIP863MHnTumeqawmNuHov3awj84b15XDoxist26S7gogr0RmMkM4l98efDG7ds/f/wInoH9t59+P4L98++vHqPoeP5DkkU46pEkIyTCUZKFdP3e/F+YZWdppGwFvOyXIE4D0Hm0Uo/aOuZmQ3NvBMLTY3eS6Z84NpePYfjBl7df3H7mZudK5zl646WONgKMpXto8f8O/3/MR7HIR9bp4UYLNWf32sYaWpq+p1s5t8abny7Ap6dpeeHk2/3apaN9BK9U2jgw/nTqXEufa+VMWfg4CHtZgJ+q89TY3LreaDQajcbm1h8BAAD//wVjpnNEBgAA"
