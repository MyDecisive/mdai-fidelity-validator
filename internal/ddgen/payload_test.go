package ddgen

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestBuildRequestJSON(t *testing.T) {
	t.Parallel()

	req, err := BuildRequest(Options{
		Signal:   SignalMetrics,
		Encoding: EncodingJSON,
		Service:  "checkout",
	})
	require.NoError(t, err)
	assert.Equal(t, "/api/v1/series", req.Path)
	assert.Equal(t, ContentTypeJSON, req.ContentType)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(req.Body, &payload))
	_, ok := payload["series"]
	assert.True(t, ok, "expected series in payload: %#v", payload)
}

func TestBuildRequestMsgpack(t *testing.T) {
	t.Parallel()

	req, err := BuildRequest(Options{
		Signal:   SignalTraces,
		Encoding: EncodingMsgpack,
		Gzip:     true,
	})
	require.NoError(t, err)
	assert.Equal(t, ContentTypeMsgpack, req.ContentType)
	assert.Equal(t, ContentEncodingGzip, req.ContentEncoding)
}

func TestBuildTraceRequestIncludesStatus(t *testing.T) {
	t.Parallel()

	req, err := BuildRequest(Options{
		Signal:   SignalTraces,
		Encoding: EncodingJSON,
	})
	require.NoError(t, err)

	var payload [][]map[string]any
	require.NoError(t, json.Unmarshal(req.Body, &payload))
	require.Len(t, payload, 1)
	require.Len(t, payload[0], 2)
	assert.Equal(t, "Ok", payload[0][0]["status"])
	assert.Equal(t, "Ok", payload[0][1]["status"])
}

func TestBuildLogsMsgpackRoundTrip(t *testing.T) {
	t.Parallel()

	req, err := BuildRequest(Options{
		Signal:   SignalLogs,
		Encoding: EncodingMsgpack,
	})
	require.NoError(t, err)

	var payload any
	require.NoError(t, msgpack.Unmarshal(req.Body, &payload))

	payloadMap, ok := payload.(map[string]any)
	require.True(t, ok, "expected top-level map, got %T", payload)
	_, ok = payloadMap["message"]
	assert.True(t, ok, "expected message field in %#v", payloadMap)
	_, ok = payloadMap["timestamp"].(int64)
	assert.True(t, ok, "expected int64 timestamp in %#v", payloadMap)
}

func TestMetricsRejectMsgpack(t *testing.T) {
	t.Parallel()

	_, err := BuildRequest(Options{
		Signal:   SignalMetrics,
		Encoding: EncodingMsgpack,
	})
	require.Error(t, err)
}

func TestBuildRequestWithCorrelationAndDropsPreservesCorrelationTags(t *testing.T) {
	t.Parallel()

	req, err := BuildRequestWithCorrelation(Options{
		Signal:   SignalMetrics,
		Encoding: EncodingJSON,
	}, "corr-fixed", 1)
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(req.Body, &payload))

	series, ok := payload["series"].([]any)
	require.True(t, ok)
	entry, ok := series[0].(map[string]any)
	require.True(t, ok)
	tags, ok := entry["tags"].([]any)
	require.True(t, ok)
	foundCorrelation := false
	for _, tag := range tags {
		if strings.HasPrefix(tag.(string), "correlation_id:corr-fixed") {
			foundCorrelation = true
		}
	}
	assert.True(t, foundCorrelation, "expected correlation_id tag in %#v", tags)
}

func TestBuildRequestWithoutCorrelationID(t *testing.T) {
	t.Parallel()

	req, err := BuildRequest(Options{
		Signal:          SignalMetrics,
		Encoding:        EncodingJSON,
		OmitCorrelation: true,
	})
	require.NoError(t, err)
	assert.Empty(t, req.CorrelationID)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(req.Body, &payload))
	series, ok := payload["series"].([]any)
	require.True(t, ok)
	entry, ok := series[0].(map[string]any)
	require.True(t, ok)
	tags, ok := entry["tags"].([]any)
	require.True(t, ok)
	for _, tag := range tags {
		tagValue := tag.(string)
		assert.False(t, strings.HasPrefix(tagValue, "correlation_id:"), "expected no correlation tags, got %#v", tags)
	}
}

func TestBuildLogsWithoutCorrelationID(t *testing.T) {
	t.Parallel()

	req, err := BuildRequest(Options{
		Signal:          SignalLogs,
		Encoding:        EncodingJSON,
		OmitCorrelation: true,
	})
	require.NoError(t, err)
	assert.Empty(t, req.CorrelationID)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(req.Body, &payload))
	_, ok := payload["correlation_id"]
	assert.False(t, ok, "did not expect correlation_id in payload: %#v", payload)
	tags, ok := payload["ddtags"].(string)
	require.True(t, ok)
	assert.NotContains(t, tags, "correlation_id:")
	attrs, ok := payload["attributes"].(map[string]any)
	require.True(t, ok, "expected attributes map, got %#v", payload["attributes"])
	assert.Len(t, attrs, 1)
	assert.Equal(t, "dev", attrs["env"])
}
