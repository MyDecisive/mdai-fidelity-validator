package validator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareSpans(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		receiverSpans []map[string]string
		exporterSpans []map[string]string
		want          []SpanComparison
	}{
		{
			name: "matched pair all fields equal",
			receiverSpans: []map[string]string{
				{"span_id": "s1", "name": "root"},
			},
			exporterSpans: []map[string]string{
				{"span_id": "s1", "name": "root"},
			},
			want: []SpanComparison{
				{SpanID: "s1", Matched: []string{"name", "span_id"}, Passed: true},
			},
		},
		{
			name: "matched pair with field mismatch",
			receiverSpans: []map[string]string{
				{"span_id": "s1", "name": "root", "service": "web"},
			},
			exporterSpans: []map[string]string{
				{"span_id": "s1", "name": "root", "service": "api"},
			},
			want: []SpanComparison{
				{
					SpanID:  "s1",
					Matched: []string{"name", "span_id"},
					Mismatched: []SpanDelta{
						{Attribute: "service", Receiver: "web", Exporter: "api"},
					},
					Passed: false,
				},
			},
		},
		{
			name: "receiver only span",
			receiverSpans: []map[string]string{
				{"span_id": "s1", "name": "root"},
			},
			exporterSpans: []map[string]string{},
			want: []SpanComparison{
				{SpanID: "s1", OnlyIn: "receiver"},
			},
		},
		{
			name:          "exporter only span",
			receiverSpans: []map[string]string{},
			exporterSpans: []map[string]string{
				{"span_id": "s1", "name": "root"},
			},
			want: []SpanComparison{
				{SpanID: "s1", OnlyIn: "exporter"},
			},
		},
		{
			name: "duplicate receiver span_id",
			receiverSpans: []map[string]string{
				{"span_id": "s1", "name": "root"},
				{"span_id": "s1", "name": "duplicate"},
			},
			exporterSpans: []map[string]string{
				{"span_id": "s1", "name": "root"},
			},
			want: []SpanComparison{
				{
					SpanID: "s1",
					Mismatched: []SpanDelta{
						{Attribute: "span_id", Receiver: "span count: 2", Exporter: "span count: 1"},
					},
					Passed: false,
				},
			},
		},
		{
			name: "duplicate exporter span_id",
			receiverSpans: []map[string]string{
				{"span_id": "s1", "name": "root"},
			},
			exporterSpans: []map[string]string{
				{"span_id": "s1", "name": "root"},
				{"span_id": "s1", "name": "duplicate"},
			},
			want: []SpanComparison{
				{
					SpanID: "s1",
					Mismatched: []SpanDelta{
						{Attribute: "span_id", Receiver: "span count: 1", Exporter: "span count: 2"},
					},
					Passed: false,
				},
			},
		},
		{
			name: "duplicate span_id on both sides",
			receiverSpans: []map[string]string{
				{"span_id": "s1", "name": "root"},
				{"span_id": "s1", "name": "receiver-duplicate"},
			},
			exporterSpans: []map[string]string{
				{"span_id": "s1", "name": "root"},
				{"span_id": "s1", "name": "exporter-duplicate"},
			},
			want: []SpanComparison{
				{
					SpanID: "s1",
					Mismatched: []SpanDelta{
						{Attribute: "span_id", Receiver: "span count: 2", Exporter: "span count: 2"},
					},
					Passed: false,
				},
			},
		},
		{
			name:          "no span_id on both sides",
			receiverSpans: []map[string]string{{"name": "root"}},
			exporterSpans: []map[string]string{{"name": "root"}},
			want: []SpanComparison{
				{
					MissingIn: []MissingField{
						{Attribute: "span_id", Side: "receiver"},
					},
					Passed: false,
				},
				{
					MissingIn: []MissingField{
						{Attribute: "span_id", Side: "exporter"},
					},
					Passed: false,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := compareSpans(tt.receiverSpans, tt.exporterSpans)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCompareSpanIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		a      string
		b      string
		assert func(t assert.TestingT, e any, msgAndArgs ...any) bool
	}{
		{name: "both numeric, a less than b", a: "2", b: "10", assert: assert.Negative},
		{name: "both numeric, a greater than b", a: "10", b: "2", assert: assert.Positive},
		{name: "both numeric, equal", a: "5", b: "5", assert: assert.Zero},
		{name: "numeric before non-numeric", a: "10", b: "abc", assert: assert.Negative},
		{name: "non-numeric after numeric", a: "abc", b: "10", assert: assert.Positive},
		{name: "both non-numeric, a less than b", a: "abc", b: "xyz", assert: assert.Negative},
		{name: "both non-numeric, a greater than b", a: "xyz", b: "abc", assert: assert.Positive},
		{name: "both non-numeric, equal", a: "abc", b: "abc", assert: assert.Zero},
		{name: "overflow treated as non-numeric", a: "18446744073709551616", b: "1", assert: assert.Positive},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := compareSpanIDs(
				map[string]string{spanKeyID: tt.a},
				map[string]string{spanKeyID: tt.b},
			)
			tt.assert(t, got)
		})
	}
}

func TestGroupSpansByID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		spans []map[string]string
		want  map[string][]map[string]string
	}{
		{
			name: "all spans have a unique span_id",
			spans: []map[string]string{
				{"span_id": "s1", "name": "root"},
				{"span_id": "s2", "name": "notroot"},
			},
			want: map[string][]map[string]string{
				"s1": {{"span_id": "s1", "name": "root"}},
				"s2": {{"span_id": "s2", "name": "notroot"}},
			},
		},
		{
			name: "spans have a duplicate span_id",
			spans: []map[string]string{
				{"span_id": "s1", "name": "root"},
				{"span_id": "s1", "name": "notroot"},
			},
			want: map[string][]map[string]string{
				"s1": {{"span_id": "s1", "name": "root"}, {"span_id": "s1", "name": "notroot"}},
			},
		},
		{
			name: "span with span_id, span without span_id",
			spans: []map[string]string{
				{"span_id": "s1", "name": "root"},
				{"name": "notroot"},
			},
			want: map[string][]map[string]string{
				"s1": {{"span_id": "s1", "name": "root"}},
			},
		},
		{
			name: "no spans with span_id",
			spans: []map[string]string{
				{"name": "root"},
				{"name": "notroot"},
			},
			want: map[string][]map[string]string{},
		},
		{
			name:  "empty input",
			spans: []map[string]string{},
			want:  map[string][]map[string]string{},
		},
		{
			name:  "nil input",
			spans: nil,
			want:  map[string][]map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := groupSpansByID(tt.spans)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsDuplicateSpanID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		spanID        string
		receiverSpans map[string][]map[string]string
		exporterSpans map[string][]map[string]string
		want          bool
	}{
		{
			name:   "is not a duplicate span_id",
			spanID: "s1",
			receiverSpans: map[string][]map[string]string{
				"s1": {{"span_id": "s1", "name": "root"}},
			},
			exporterSpans: map[string][]map[string]string{
				"s1": {{"span_id": "s1", "name": "root"}},
			},
			want: false,
		},
		{
			name:   "is a duplicate span_id (in receiver)",
			spanID: "s1",
			receiverSpans: map[string][]map[string]string{
				"s1": {{"span_id": "s1", "name": "root"}, {"span_id": "s1", "name": "notroot"}},
			},
			exporterSpans: map[string][]map[string]string{
				"s1": {{"span_id": "s1", "name": "root"}},
			},
			want: true,
		},
		{
			name:   "is a duplicate span_id (in exporter)",
			spanID: "s1",
			receiverSpans: map[string][]map[string]string{
				"s1": {{"span_id": "s1", "name": "root"}},
			},
			exporterSpans: map[string][]map[string]string{
				"s1": {{"span_id": "s1", "name": "root"}, {"span_id": "s1", "name": "notroot"}},
			},
			want: true,
		},
		{
			name:   "is a duplicate span_id (in both)",
			spanID: "s1",
			receiverSpans: map[string][]map[string]string{
				"s1": {{"span_id": "s1", "name": "root"}, {"span_id": "s1", "name": "notroot"}},
			},
			exporterSpans: map[string][]map[string]string{
				"s1": {{"span_id": "s1", "name": "root"}, {"span_id": "s1", "name": "notroot"}},
			},
			want: true,
		},
		{
			name:          "span_id not present in either map",
			spanID:        "s1",
			receiverSpans: map[string][]map[string]string{},
			exporterSpans: map[string][]map[string]string{},
			want:          false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isDuplicateSpanID(tt.spanID, tt.receiverSpans, tt.exporterSpans)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDiffSpanPair(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		spanID       string
		receiverSpan map[string]string
		exporterSpan map[string]string
		want         SpanComparison
	}{
		{
			name:         "no difference",
			spanID:       "s1",
			receiverSpan: map[string]string{"name": "root"},
			exporterSpan: map[string]string{"name": "root"},
			want: SpanComparison{
				SpanID:     "s1",
				Matched:    []string{"name"},
				Mismatched: nil,
				MissingIn:  nil,
				Passed:     true,
			},
		},
		{
			name:         "different span name",
			spanID:       "s1",
			receiverSpan: map[string]string{"name": "root"},
			exporterSpan: map[string]string{"name": "notroot"},
			want: SpanComparison{
				SpanID:  "s1",
				OnlyIn:  "",
				Matched: nil,
				Mismatched: []SpanDelta{
					{Attribute: "name", Receiver: "root", Exporter: "notroot"},
				},
				MissingIn: nil,
				Passed:    false,
			},
		},
		{
			name:         "mix of matched and mismatched",
			spanID:       "s1",
			receiverSpan: map[string]string{"name": "root", "service": "foo"},
			exporterSpan: map[string]string{"name": "root", "service": "bar"},
			want: SpanComparison{
				SpanID:  "s1",
				OnlyIn:  "",
				Matched: []string{"name"},
				Mismatched: []SpanDelta{
					{Attribute: "service", Receiver: "foo", Exporter: "bar"},
				},
				MissingIn: nil,
				Passed:    false,
			},
		},
		{
			name:         "field present in receiver, missing in exporter",
			spanID:       "s1",
			receiverSpan: map[string]string{"name": "root", "service": "foo"},
			exporterSpan: map[string]string{"name": "root"},
			want: SpanComparison{
				SpanID:     "s1",
				Matched:    []string{"name"},
				Mismatched: nil,
				MissingIn: []MissingField{
					{Attribute: "service", Side: exporterSide, Value: "foo"},
				},
				Passed: false,
			},
		},
		{
			name:         "field missing in receiver, present in exporter",
			spanID:       "s1",
			receiverSpan: map[string]string{"name": "root"},
			exporterSpan: map[string]string{"name": "root", "service": "bar"},
			want: SpanComparison{
				SpanID:     "s1",
				Matched:    []string{"name"},
				Mismatched: nil,
				MissingIn: []MissingField{
					{Attribute: "service", Side: receiverSide, Value: "bar"},
				},
				Passed: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := diffSpanPair(tt.spanID, tt.receiverSpan, tt.exporterSpan)
			assert.Equal(t, tt.want, got)
		})
	}
}
