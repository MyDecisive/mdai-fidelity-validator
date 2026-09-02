package validator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizedDebugPayloadRedactsDatadogAPIKey(t *testing.T) {
	t.Parallel()

	payload := &observedPayload{
		request: RequestSnapshot{
			Headers: map[string]string{
				"Content-Type": "application/json",
				"DD-API-KEY":   "secret",
				"DD_API_KEY":   "secret",
			},
		},
		flattened: map[string]string{
			"message":               "ok",
			"attributes.DD_API_KEY": "secret",
		},
	}

	got := sanitizedDebugPayloadFromObserved(payload)
	assert.Equal(t, "application/json", got.Request.Headers["Content-Type"])
	assert.Equal(t, "[REDACTED]", got.Request.Headers["DD-API-KEY"])
	assert.Equal(t, "[REDACTED]", got.Request.Headers["DD_API_KEY"])
	assert.Equal(t, "ok", got.Attributes["message"])
	assert.Equal(t, "[REDACTED]", got.Attributes["attributes.DD_API_KEY"])
}

func TestSanitizedDebugPayloadIncludesRawBodyOnlyForDecodeError(t *testing.T) {
	t.Parallel()

	decoded := &observedPayload{
		body:        []byte(`{"message":"ok"}`),
		decodeError: "",
	}
	assert.Empty(t, sanitizedDebugPayloadFromObserved(decoded).RawBody)

	mappingError := &observedPayload{
		body:        []byte(`{"message":"ok"}`),
		decodeError: "field mapping produced no canonical attributes",
	}
	assert.Empty(t, sanitizedDebugPayloadFromObserved(mappingError).RawBody)

	decodeError := &observedPayload{
		body:        []byte(`{"message":"ok","DD_API_KEY":"secret"}`),
		decodeError: "failed to decode payload: unsupported payload encoding",
	}
	assert.JSONEq(t, `{"message":"ok","DD_API_KEY":"[REDACTED]"}`, sanitizedDebugPayloadFromObserved(decodeError).RawBody)
}

func TestSanitizedComparisonResultRedactsSensitiveFields(t *testing.T) {
	t.Parallel()

	result := ComparisonResult{
		ReceiverFields:    map[string]string{"DD_API_KEY": "receiver-secret", "message": "ok"},
		ExporterFields:    map[string]string{"attributes.DD_API_KEY": "exporter-secret"},
		ReceiverRawFields: map[string]string{"resource[0].DD-API-KEY": "receiver-secret"},
		ExporterRawFields: map[string]string{"message": "ok"},
		Mismatched: []AttributeDelta{
			{Attribute: "DD_API_KEY", Receiver: "receiver-secret", Exporter: "exporter-secret"},
		},
		MissingIn: []MissingField{
			{Attribute: "DD_API_KEY", Side: "exporter", Value: "receiver-secret"},
		},
		ReceiverWire: RequestSnapshot{
			Headers: map[string]string{"DD-API-KEY": "receiver-secret", "Content-Type": "application/json"},
		},
		ExporterWire: RequestSnapshot{
			Headers: map[string]string{"DD_API_KEY": "exporter-secret", "Content-Type": "application/json"},
		},
	}

	got := sanitizedComparisonResult(result)
	assert.Equal(t, "[REDACTED]", got.ReceiverFields["DD_API_KEY"])
	assert.Equal(t, "[REDACTED]", got.ExporterFields["attributes.DD_API_KEY"])
	assert.Equal(t, "[REDACTED]", got.ReceiverRawFields["resource[0].DD-API-KEY"])
	assert.Equal(t, "ok", got.ExporterRawFields["message"])
	assert.Equal(t, "[REDACTED]", got.Mismatched[0].Receiver)
	assert.Equal(t, "[REDACTED]", got.Mismatched[0].Exporter)
	assert.Equal(t, "[REDACTED]", got.MissingIn[0].Value)
	assert.Equal(t, "[REDACTED]", got.ReceiverWire.Headers["DD-API-KEY"])
	assert.Equal(t, "[REDACTED]", got.ExporterWire.Headers["DD_API_KEY"])
	assert.Equal(t, "application/json", got.ReceiverWire.Headers["Content-Type"])
	assert.Equal(t, "application/json", got.ExporterWire.Headers["Content-Type"])
}

func TestIsSensitiveFieldName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		field     string
		sensitive bool
	}{
		{"DD_API_KEY", true},
		{"dd_api_key", true},
		{"DD-API-KEY", true},
		{"dd-api-key", true},
		{"Dd-Api-Key", true},
		{"attributes.DD_API_KEY", true},
		{"attributes.DD-API-KEY", true},
		{"resource[0].DD_API_KEY", true},
		{"resource[0].DD-API-KEY", true},
		{"Content-Type", false},
		{"message", false},
		{"X-Request-ID", false},
		{"service.name", false},
		{"DD_API_KEY_EXTRA", false},
		{"MY_DD_API_KEY", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.sensitive, isSensitiveFieldName(tt.field))
		})
	}
}

func TestSanitizers(t *testing.T) {
	t.Parallel()

	t.Run("headers", func(t *testing.T) {
		t.Parallel()
		testSanitizer(t, sanitizedHeaders, []sanitizerTestCase[map[string]string]{
			{
				name:  "nil map returned as-is",
				input: nil,
				want:  nil,
			},
			{
				name:  "empty map returned as-is",
				input: map[string]string{},
				want:  map[string]string{},
			},
			{
				name:  "non-sensitive headers pass through unchanged",
				input: map[string]string{"Content-Type": "application/json", "X-Request-ID": "abc"},
				want:  map[string]string{"Content-Type": "application/json", "X-Request-ID": "abc"},
			},
			{
				name:  "sensitive header value masked, key preserved",
				input: map[string]string{"DD-API-KEY": "secret", "Content-Type": "application/json"},
				want:  map[string]string{"DD-API-KEY": "[REDACTED]", "Content-Type": "application/json"},
			},
		})
	})

	t.Run("attributes", func(t *testing.T) {
		t.Parallel()
		testSanitizer(t, sanitizedAttributes, []sanitizerTestCase[map[string]string]{
			{
				name:  "nil map returned as-is",
				input: nil,
				want:  nil,
			},
			{
				name:  "empty map returned as-is",
				input: map[string]string{},
				want:  map[string]string{},
			},
			{
				name:  "non-sensitive attributes pass through unchanged",
				input: map[string]string{"message": "hello", "service.name": "svc"},
				want:  map[string]string{"message": "hello", "service.name": "svc"},
			},
			{
				name:  "sensitive attribute value masked, key preserved",
				input: map[string]string{"attributes.DD_API_KEY": "secret", "message": "hello"},
				want:  map[string]string{"attributes.DD_API_KEY": "[REDACTED]", "message": "hello"},
			},
		})
	})

	t.Run("attribute deltas", func(t *testing.T) {
		t.Parallel()
		testSanitizer(t, sanitizedAttributeDeltas, []sanitizerTestCase[[]AttributeDelta]{
			{
				name:  "nil slice returned as-is",
				input: nil,
				want:  nil,
			},
			{
				name:  "empty slice returned as-is",
				input: []AttributeDelta{},
				want:  []AttributeDelta{},
			},
			{
				name:  "non-sensitive delta passes through unchanged",
				input: []AttributeDelta{{Attribute: "message", Receiver: "hello", Exporter: "world"}},
				want:  []AttributeDelta{{Attribute: "message", Receiver: "hello", Exporter: "world"}},
			},
			{
				name:  "sensitive delta values masked, attribute name preserved",
				input: []AttributeDelta{{Attribute: "DD_API_KEY", Receiver: "r-secret", Exporter: "e-secret"}},
				want:  []AttributeDelta{{Attribute: "DD_API_KEY", Receiver: "[REDACTED]", Exporter: "[REDACTED]"}},
			},
		})
	})

	t.Run("missing fields", func(t *testing.T) {
		t.Parallel()
		testSanitizer(t, sanitizedMissingFields, []sanitizerTestCase[[]MissingField]{
			{
				name:  "nil slice returned as-is",
				input: nil,
				want:  nil,
			},
			{
				name:  "empty slice returned as-is",
				input: []MissingField{},
				want:  []MissingField{},
			},
			{
				name:  "non-sensitive field passes through unchanged",
				input: []MissingField{{Attribute: "message", Side: "exporter", Value: "hello"}},
				want:  []MissingField{{Attribute: "message", Side: "exporter", Value: "hello"}},
			},
			{
				name:  "sensitive field value masked, attribute and side preserved",
				input: []MissingField{{Attribute: "DD_API_KEY", Side: "receiver", Value: "secret"}},
				want:  []MissingField{{Attribute: "DD_API_KEY", Side: "receiver", Value: "[REDACTED]"}},
			},
		})
	})
}

func TestSanitizedRawPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
		json  bool
	}{
		{
			name:  "non-JSON body returned as-is",
			input: "not json at all",
			want:  "[unparseable payload redacted]",
		},
		{
			name:  "JSON without sensitive fields passes through unchanged",
			input: `{"message":"ok","service":"svc"}`,
			want:  `{"message":"ok","service":"svc"}`,
			json:  true,
		},
		{
			name:  "top-level sensitive field masked",
			input: `{"message":"ok","DD_API_KEY":"secret"}`,
			want:  `{"message":"ok","DD_API_KEY":"[REDACTED]"}`,
			json:  true,
		},
		{
			name:  "nested sensitive field masked",
			input: `{"meta":{"DD_API_KEY":"secret"},"message":"ok"}`,
			want:  `{"meta":{"DD_API_KEY":"[REDACTED]"},"message":"ok"}`,
			json:  true,
		},
		{
			name:  "array of objects with sensitive field masked",
			input: `[{"DD_API_KEY":"secret","message":"ok"}]`,
			want:  `[{"DD_API_KEY":"[REDACTED]","message":"ok"}]`,
			json:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := sanitizedRawPayload(tt.input)
			if tt.json {
				assert.JSONEq(t, tt.want, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestShouldIncludeRawPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		decodeError string
		want        bool
	}{
		{"failed to decode payload", true},
		{"decode error occurred", true},
		{"DECODE: unsupported format", true},
		{"", false},
		{"field mapping produced no attributes", false},
		{"unsupported payload encoding", false},
	}
	for _, tt := range tests {
		t.Run(tt.decodeError, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, shouldIncludeRawPayload(&observedPayload{decodeError: tt.decodeError}))
		})
	}
}
