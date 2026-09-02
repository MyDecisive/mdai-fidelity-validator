package validator

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

func TestFlattenValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		keyPath []byte
		value   any
		want    map[string]string
	}{
		{
			name:    "string value",
			keyPath: []byte("foo"),
			value:   "bar",
			want:    map[string]string{"foo": "bar"},
		},
		{
			name:    "json.Number value",
			keyPath: []byte("foo"),
			value:   json.Number("9999"),
			want:    map[string]string{"foo": "9999"},
		},
		{
			name:    "float64 value",
			keyPath: []byte("foo"),
			value:   9999.99,
			want:    map[string]string{"foo": "9999.99"},
		},
		{
			name:    "bool value",
			keyPath: []byte("foo"),
			value:   true,
			want:    map[string]string{"foo": "true"},
		},
		{
			name:    "nil value",
			keyPath: []byte("foo"),
			value:   nil,
			want:    map[string]string{"foo": "null"},
		},
		{
			name:    "map[string]any value",
			keyPath: []byte("foo"),
			value:   map[string]any{"this": "that"},
			want:    map[string]string{"foo.this": "that"},
		},
		{
			name:    "[]any value",
			keyPath: []byte("foo"),
			value:   []any{"this", "that", "the", "other"},
			want:    map[string]string{"foo[0]": "this", "foo[1]": "that", "foo[2]": "the", "foo[3]": "other"},
		},
		{
			name:    "empty keypath",
			keyPath: []byte(nil),
			value:   map[string]any{"this": "that"},
			want:    map[string]string{"this": "that"},
		},
		{
			name:    "unknown type (int)",
			keyPath: []byte("foo"),
			value:   999,
			want:    map[string]string{"foo": "999"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := map[string]string{}
			flattenValue(result, tt.keyPath, tt.value)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestIsDecimalPort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		port string
		want bool
	}{
		{name: "all digits", port: "8126", want: true},
		{name: "empty string", port: "", want: false},
		{name: "letters only", port: "abc", want: false},
		{name: "mixed digits and letters", port: "80a", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isDecimalPort(tt.port)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFlattenValueMap(t *testing.T) {
	t.Parallel()

	var payload any
	require.NoError(t, json.Unmarshal([]byte(`{"resource":{"attributes":[{"key":"service.name","value":"demo"}]},"value":1,"ok":true}`), &payload))

	fields := flattenValueMap(payload)

	assert.Equal(t, "service.name", fields["resource.attributes[0].key"])
	assert.Equal(t, "1", fields["value"])
	assert.Equal(t, "true", fields["ok"])
}

func TestFlattenValueMapRootArray(t *testing.T) {
	t.Parallel()

	var payload any
	require.NoError(t, json.Unmarshal([]byte(`[{"message":"a"},{"message":"b","nested":[true]}]`), &payload))

	fields := flattenValueMap(payload)

	assert.Equal(t, "a", fields["[0].message"])
	assert.Equal(t, "b", fields["[1].message"])
	assert.Equal(t, "true", fields["[1].nested[0]"])
}

func TestDecodeBodyJSONGzip(t *testing.T) {
	t.Parallel()

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write([]byte(`{"correlation_id":"demo-1","value":1}`))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	payload, format, err := decodeBody(compressed.Bytes(), "", "gzip", "application/json")
	require.NoError(t, err)
	assert.Equal(t, "json", format)

	fields := flattenValueMap(payload)
	assert.Equal(t, "demo-1", fields["correlation_id"])
}

func TestDecodeBodyJSONPreservesLargeTraceIDs(t *testing.T) {
	t.Parallel()

	payload, format, err := decodeBody(
		[]byte(`[[{"trace_id":218523465776977553,"span_id":6933185253666401645}]]`),
		"/v0.4/traces",
		"",
		"application/json",
	)
	require.NoError(t, err)
	assert.Equal(t, "json", format)

	fields := flattenValueMap(payload)
	assert.Equal(t, "218523465776977553", fields["[0][0].trace_id"])
	assert.Equal(t, "6933185253666401645", fields["[0][0].span_id"])
}

func TestDecodeBodyMsgpack(t *testing.T) {
	t.Parallel()

	body, err := msgpack.Marshal(map[string]any{
		"resource": map[string]any{
			"correlation_id": "raw-dd-1",
		},
	})
	require.NoError(t, err)

	payload, format, err := decodeBody(body, "", "", "application/msgpack")
	require.NoError(t, err)
	assert.Equal(t, "msgpack", format)

	fields := flattenValueMap(payload)
	assert.Equal(t, "raw-dd-1", fields["resource.correlation_id"])
}

func TestInferSignalFromDatadogPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		requestPath string
		want        Signal
	}{
		{name: "traces", requestPath: "/v0.4/traces", want: SignalTraces},
		{name: "series", requestPath: "/api/v1/series", want: SignalMetrics},
		{name: "logs", requestPath: "/api/v2/logs", want: SignalLogs},
		{name: "unknown", requestPath: "/something/else", want: SignalUnknown},
		{name: "distribution points", requestPath: "/api/v1/distribution_points", want: SignalMetrics},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, inferSignalFromDatadogPath(tt.requestPath))
		})
	}
}

func TestParseExporterPath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		path           string
		wantExporter   string
		wantNormalized string
	}{
		{name: "datadog api path", path: "/api/v2/logs", wantExporter: "", wantNormalized: "/api/v2/logs"},
		{name: "explicit exporter prefix", path: "/exporter/datadog/api/v2/logs", wantExporter: "datadog", wantNormalized: "/api/v2/logs"},
		{name: "exporter root", path: "/exporter/datadog", wantExporter: "datadog", wantNormalized: "/"},
		{name: "observe exporter path", path: "/observe/exporter/mdai/sample/gateway/datadog/api/v0.2/traces", wantExporter: "datadog", wantNormalized: "/api/v0.2/traces"},
		{name: "intake path is not observe exporter path", path: "/intake/exporter/mdai/sample/gateway/datadog/api/v0.2/traces", wantExporter: "intake", wantNormalized: "/exporter/mdai/sample/gateway/datadog/api/v0.2/traces"},
		{name: "non datadog exporter", path: "/splunk/services/collector/event", wantExporter: "splunk", wantNormalized: "/services/collector/event"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotExporter, gotNormalized := parseExporterPath(tc.path)
			assert.Equal(t, tc.wantExporter, gotExporter)
			assert.Equal(t, tc.wantNormalized, gotNormalized)
		})
	}
}

func TestExtractIndexedGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		groupName string
		flattened map[string]string
		want      []map[string]string
	}{
		{
			name:      "traces",
			groupName: "traces",
			flattened: map[string]string{
				"traces[0][0].trace_id": "trace-aaa",
				"traces[0][0].span_id":  "span-111",
				"traces[0][1].trace_id": "trace-aaa",
				"traces[0][1].span_id":  "span-222",
				"traces[1][0].trace_id": "trace-bbb",
				"traces[1][0].span_id":  "span-333",
			},
			want: []map[string]string{
				{
					"[0].trace_id": "trace-aaa",
					"[0].span_id":  "span-111",
					"[1].trace_id": "trace-aaa",
					"[1].span_id":  "span-222",
				},
				{
					"[0].trace_id": "trace-bbb",
					"[0].span_id":  "span-333",
				},
			},
		},
		{
			name:      "metrics",
			groupName: "series",
			flattened: map[string]string{
				"series[0].metric":    "cpu.usage",
				"series[0].points[0]": "42",
				"series[1].metric":    "mem.usage",
				"series[1].points[0]": "1024",
			},
			want: []map[string]string{
				{
					"metric":    "cpu.usage",
					"points[0]": "42",
				},
				{
					"metric":    "mem.usage",
					"points[0]": "1024",
				},
			},
		},
		{
			name:      "logs root array",
			groupName: "",
			flattened: map[string]string{
				"[0].message":   "log entry A",
				"[0].timestamp": "2025-01-01T00:00:00Z",
				"[0].hostname":  "host-1",
				"[1].message":   "log entry B",
				"[1].timestamp": "2025-01-01T00:00:01Z",
				"[1].hostname":  "host-2",
			},
			want: []map[string]string{
				{
					"message":   "log entry A",
					"timestamp": "2025-01-01T00:00:00Z",
					"hostname":  "host-1",
				},
				{
					"message":   "log entry B",
					"timestamp": "2025-01-01T00:00:01Z",
					"hostname":  "host-2",
				},
			},
		},
		{
			name:      "no matching prefix returns original",
			groupName: "traces",
			flattened: map[string]string{
				"trace_id": "abc",
				"span_id":  "def",
			},
			want: []map[string]string{
				{
					"trace_id": "abc",
					"span_id":  "def",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, extractIndexedGroups(tt.flattened, tt.groupName))
		})
	}
}

func TestDecodeAllTracesUnwrappedFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rawFlat    map[string]string
		prefix     string
		wantGroups int
		wantSpans  [][]map[string]string
	}{
		{
			name:   "unwrapped format",
			prefix: "",
			rawFlat: map[string]string{
				"[0][0].trace_id": "trace-aaa",
				"[0][0].span_id":  "span-111",
				"[0][0].name":     "web.request",
				"[0][1].trace_id": "trace-aaa",
				"[0][1].span_id":  "span-222",
				"[0][1].name":     "db.query",
				"[1][0].trace_id": "trace-bbb",
				"[1][0].span_id":  "span-333",
				"[1][0].name":     "background.job",
			},
			wantGroups: 2,
			wantSpans: [][]map[string]string{
				{
					{"trace_id": "trace-aaa", "span_id": "span-111", "name": "web.request"},
					{"trace_id": "trace-aaa", "span_id": "span-222", "name": "db.query"},
				},
				{
					{"trace_id": "trace-bbb", "span_id": "span-333", "name": "background.job"},
				},
			},
		},
		{
			name:   "wrapped format",
			prefix: "traces",
			rawFlat: map[string]string{
				"traces[0][0].trace_id": "trace-aaa",
				"traces[0][0].span_id":  "span-111",
				"traces[0][0].name":     "web.request",
				"traces[0][1].trace_id": "trace-aaa",
				"traces[0][1].span_id":  "span-222",
				"traces[0][1].name":     "db.query",
				"traces[1][0].trace_id": "trace-bbb",
				"traces[1][0].span_id":  "span-333",
				"traces[1][0].name":     "background.job",
			},
			wantGroups: 2,
			wantSpans: [][]map[string]string{
				{
					{"trace_id": "trace-aaa", "span_id": "span-111", "name": "web.request"},
					{"trace_id": "trace-aaa", "span_id": "span-222", "name": "db.query"},
				},
				{
					{"trace_id": "trace-bbb", "span_id": "span-333", "name": "background.job"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			groups := extractIndexedGroups(tt.rawFlat, tt.prefix)
			require.Len(t, groups, tt.wantGroups)

			for i, wantSpans := range tt.wantSpans {
				spans := newTraceGroup(groups[i]).spans
				require.Len(t, spans, len(wantSpans))
				for j, want := range wantSpans {
					assert.Equal(t, want, spans[j])
				}
			}
		})
	}
}

func TestCaptureRequestsBatchSplitMatchesIndependently(t *testing.T) {
	t.Parallel()

	// Receiver sends one batch with two trace groups; exporter sends each trace separately.
	// Both should match against their respective receiver group.
	svc, _ := newMetricsTestService("batch-split")
	svc.retention = time.Minute

	payloads, results, anyMatched := captureWithBatch(
		t,
		svc,
		"receiver",
		SignalTraces,
		":8126",
		"/v0.4/traces",
		nil,
		[]DecodedPayload{
			{Signal: SignalTraces, Attributes: map[string]string{"trace_id": "trace-aaa", "span_id": "span-111"}, Format: "json"},
			{Signal: SignalTraces, Attributes: map[string]string{"trace_id": "trace-bbb", "span_id": "span-333"}, Format: "json"},
		},
	)
	require.Len(t, payloads, 2)
	assert.False(t, anyMatched)
	assert.Nil(t, results[0])
	assert.Nil(t, results[1])

	pA, rA, matchedA := captureWithBatch(
		t,
		svc,
		"exporter",
		SignalTraces,
		":18081",
		"/v0.4/traces",
		nil,
		[]DecodedPayload{
			{Signal: SignalTraces, Attributes: map[string]string{"trace_id": "trace-aaa", "span_id": "span-111"}, Format: "json"},
		},
	)
	require.Len(t, pA, 1)
	assert.True(t, matchedA, "trace-aaa should match receiver group 0")
	require.NotNil(t, rA[0])

	pB, rB, matchedB := captureWithBatch(
		t,
		svc,
		"exporter",
		SignalTraces,
		":18081",
		"/v0.4/traces",
		nil,
		[]DecodedPayload{
			{Signal: SignalTraces, Attributes: map[string]string{"trace_id": "trace-bbb", "span_id": "span-333"}, Format: "json"},
		},
	)
	require.Len(t, pB, 1)
	assert.True(t, matchedB, "trace-bbb should match receiver group 1")
	require.NotNil(t, rB[0])
}

func TestCaptureRequestsLogBatchMergeMatchesPerCorrelationGroup(t *testing.T) {
	t.Parallel()

	// Receiver gets two separate log requests (two envoy UUIDs, no per-entry correlation_id
	// in the log content itself — the receiver uses the header strategy).
	// Exporter merges them into one request, with each entry carrying a correlation_id field.
	// After grouping by per-entry correlation_id the exporter split matches each receiver batch.
	svc, _ := newMetricsTestService("log-merge")
	svc.retention = time.Minute

	p1, _, matched1 := captureWithBatch(
		t,
		svc,
		"receiver",
		SignalLogs,
		":8126",
		"/api/v2/logs",
		map[string]string{"X-Correlation-ID": "uuid-1"},
		[]DecodedPayload{
			{Signal: SignalLogs, Attributes: map[string]string{"message": "msg-A", "hostname": "h1"}, Format: "json"},
		},
	)
	require.Len(t, p1, 1)
	assert.Equal(t, "logs:uuid-1", p1[0].correlation)
	assert.False(t, matched1)

	p2, _, matched2 := captureWithBatch(
		t,
		svc,
		"receiver",
		SignalLogs,
		":8126",
		"/api/v2/logs",
		map[string]string{"X-Correlation-ID": "uuid-2"},
		[]DecodedPayload{
			{Signal: SignalLogs, Attributes: map[string]string{"message": "msg-B", "hostname": "h2"}, Format: "json"},
		},
	)
	require.Len(t, p2, 1)
	assert.Equal(t, "logs:uuid-2", p2[0].correlation)
	assert.False(t, matched2)

	// Exporter sends merged batch: each entry carries its original correlation_id so the
	// translator strategy produces the right per-group correlation.
	expPayloads, expResults, anyMatched := captureWithBatch(
		t,
		svc,
		"exporter",
		SignalLogs,
		":18081",
		"/api/v2/logs",
		nil,
		[]DecodedPayload{
			{Signal: SignalLogs, CorrelationID: "uuid-1", Attributes: map[string]string{"message": "msg-A", "correlation_id": "uuid-1", "hostname": "h1"}, Format: "json"},
			{Signal: SignalLogs, CorrelationID: "uuid-2", Attributes: map[string]string{"message": "msg-B", "correlation_id": "uuid-2", "hostname": "h2"}, Format: "json"},
		},
	)
	require.Len(t, expPayloads, 2)
	assert.True(t, anyMatched, "both exporter groups should match their receiver batches")
	require.NotNil(t, expResults[0])
	require.NotNil(t, expResults[1])
	assert.True(t, expResults[0].FullPayloadPassed, "uuid-1 group should pass")
	assert.True(t, expResults[1].FullPayloadPassed, "uuid-2 group should pass")
}

func TestResolvePairForRequest(t *testing.T) {
	t.Parallel()

	newTestService := func() *Service {
		return &Service{
			logger:      zap.NewNop(),
			defaultPair: defaultPairID,
			pairs: map[string]configuredPair{
				defaultPairID: {
					PairConfig: PairConfig{
						ID:                 defaultPairID,
						ReceiverTranslator: defaultTranslatorID,
						ExporterTranslator: defaultTranslatorID,
					},
				},
				"shadow-a": {
					PairConfig: PairConfig{
						ID:                 "shadow-a",
						ReceiverTranslator: defaultTranslatorID,
						ExporterTranslator: defaultTranslatorID,
						ReceiverPorts:      []string{"18126"},
					},
				},
			},
			receiverPairByPort: map[string]string{"18126": "shadow-a"},
			exporterPairByPort: map[string]string{},
		}
	}

	tests := []struct {
		name    string
		headers map[string]string
		wantID  string
	}{
		{name: "default pair", wantID: defaultPairID},
		{name: "port mapped pair", headers: map[string]string{"X-Forwarded-Port": "18126"}, wantID: "shadow-a"},
		{name: "named pair header", headers: map[string]string{pairHeaderKey: "shadow-a"}, wantID: "shadow-a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v0.4/traces", http.NoBody)
			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}
			got := newTestService().resolvePairForRequest(req, "receiver", ":8126")
			assert.Equal(t, tt.wantID, got.ID)
		})
	}
}

func TestNormalizePathPatternList(t *testing.T) {
	t.Parallel()

	got, err := normalizePathPatternList([]string{" api/beta/sketches ", "/api/beta/*", "/api/beta/sketches"})
	require.NoError(t, err)
	want := []string{"/api/beta/*", "/api/beta/sketches"}
	assert.Equal(t, want, got)
}

func TestCanonicalPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{name: "plain port", raw: "8126", want: "8126", ok: true},
		{name: "colon prefixed port", raw: ":8126", want: "8126", ok: true},
		{name: "host port", raw: "127.0.0.1:8126", want: "8126", ok: true},
		{name: "ipv6 host port", raw: "[::1]:8126", want: "8126", ok: true},
		{name: "first forwarded value", raw: "8126, 18126", want: "8126", ok: true},
		{name: "empty", raw: " ", ok: false},
		{name: "non numeric", raw: "localhost", ok: false},
		{name: "host non numeric port", raw: "localhost:http", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := canonicalPort(tt.raw)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLooksLikeJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body []byte
		want bool
	}{
		{name: "object", body: []byte(`{"ok":true}`), want: true},
		{name: "array with whitespace", body: []byte("\n\t [1]"), want: true},
		{name: "empty", body: nil, want: false},
		{name: "whitespace only", body: []byte(" \r\n\t"), want: false},
		{name: "not json", body: []byte("text"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, looksLikeJSON(tt.body))
		})
	}
}

func TestConfiguredPairShouldIgnorePath(t *testing.T) {
	t.Parallel()

	pair := configuredPair{
		PairConfig: PairConfig{
			ID:                  "shadow-e",
			ExporterIgnorePaths: []string{"/api/beta/sketches", "/api/v2/*"},
			ReceiverIgnorePaths: []string{"/v0.4/traces"},
		},
	}

	assert.True(t, pair.shouldIgnorePath("exporter", "/exporter/datadog/api/beta/sketches"))
	assert.True(t, pair.shouldIgnorePath("exporter", "/api/v2/series"))
	assert.True(t, pair.shouldIgnorePath("receiver", "/v0.4/traces"))
	assert.False(t, pair.shouldIgnorePath("exporter", "/exporter/datadog/api/v1/validate"))
}
