package validator

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"testing"

	dptrace "github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace"
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

func TestDecodeBodyDatadogAgentPayloadProto(t *testing.T) {
	t.Parallel()

	inner := &dptrace.TracerPayload{
		Chunks: []*dptrace.TraceChunk{
			{
				Spans: []*dptrace.Span{
					{
						TraceID:  12345,
						SpanID:   67890,
						Service:  "test-service",
						Name:     "server.request",
						Resource: "GET /api",
					},
				},
			},
		},
	}
	agentPayload := &dptrace.AgentPayload{
		HostName:       "test-host",
		Env:            "prod",
		TracerPayloads: []*dptrace.TracerPayload{inner},
	}
	body, err := agentPayload.MarshalVT()
	require.NoError(t, err)

	payload, format, err := decodeBody(body, "/exporter/datadog/api/v0.2/traces", "", "application/x-protobuf")
	require.NoError(t, err)
	assert.Equal(t, "protobuf", format)

	fields := flattenValueMap(payload)
	assert.Equal(t, "12345", fields["traces[0][0].trace_id"])
	assert.Equal(t, "67890", fields["traces[0][0].span_id"])
	assert.Equal(t, "test-service", fields["traces[0][0].service"])
	assert.Equal(t, "server.request", fields["traces[0][0].name"])
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

const datadogTraceProtoFixtureBase64 = "H4sIAAAAAAAA/+KSTU8sSS1PrNQtzkhMyS/XTc7PyUlNLskv0i1JLS4RYkrPN+plktrAyMWXkpKemqdramiaZJCYlijEV5xaVJZapFeUWliaWlwixQuWh3EVbm/bsnb9rqtnSjXOzZy74PjNnTvVLBoaVmx8PP/5aXGHAy22QWZcfMn5RUWpOYklmfl58ZkpQiqpKcZJlubGqbpmBpapuiaGKWm6FoZplrqWlpZJBsZmlqkWFilRClyi8cWJuQU5mXnp8QVFmflFmSWV8WWGggxg8ME+ibk8NUkqAsPFHClJeoWlqUWVUhzBrj6uziEKhkjOPHz06qSmpucb5AwQDn4Cd/CCQ3JJbMmlxSX5uYAAAAD//2O77gIzAQAA"
