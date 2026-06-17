package validator

import (
	"cmp"
	"fmt"
	"maps"
	"strconv"
	"strings"
)

const (
	spanKeyID    = "span_id"
	receiverSide = "receiver"
	exporterSide = "exporter"
)

// compareSpans matches parsed receiver/exporter spans by span_id and returns one
// SpanComparison per matched, unmatched, or invalid span identity.
func compareSpans(receiverSpans, exporterSpans []map[string]string) []SpanComparison {
	receiverSpansBySpanID := groupSpansByID(receiverSpans)
	exporterSpansBySpanID := groupSpansByID(exporterSpans)
	processedSpanIDs := make(map[string]struct{}, len(receiverSpansBySpanID)+len(exporterSpansBySpanID))
	results := make([]SpanComparison, 0, len(receiverSpans)+len(exporterSpans))

	for _, receiverSpan := range receiverSpans {
		spanID := receiverSpan[spanKeyID]
		// span has no ID; cannot be matched or correlated
		if spanID == "" {
			spanComparison := SpanComparison{
				MissingIn: []MissingField{
					{Attribute: spanKeyID, Side: receiverSide},
				},
				Passed: false,
			}
			results = append(results, spanComparison)
			continue
		}
		// skip already processed span IDs to avoid double-reporting
		// spans already handled as matched pairs or duplicates
		if _, seen := processedSpanIDs[spanID]; seen {
			continue
		}
		// ambiguous identity — multiple spans share this ID on at least one side
		if isDuplicateSpanID(spanID, receiverSpansBySpanID, exporterSpansBySpanID) {
			results = append(results, SpanComparison{
				SpanID: spanID,
				Mismatched: []SpanDelta{
					{
						Attribute: spanKeyID,
						Receiver:  fmt.Sprintf("span count: %d", len(receiverSpansBySpanID[spanID])),
						Exporter:  fmt.Sprintf("span count: %d", len(exporterSpansBySpanID[spanID])),
					},
				},
				Passed: false,
			})
			processedSpanIDs[spanID] = struct{}{}
			continue
		}

		// no matching span on exporter side
		if len(exporterSpansBySpanID[spanID]) == 0 {
			results = append(results, SpanComparison{
				SpanID: spanID,
				OnlyIn: receiverSide,
			})
			processedSpanIDs[spanID] = struct{}{}
			continue
		}

		// matched pair; report field-level differences
		results = append(results, diffSpanPair(spanID, receiverSpan, exporterSpansBySpanID[spanID][0]))
		processedSpanIDs[spanID] = struct{}{}
	}

	for _, exporterSpan := range exporterSpans {
		spanID := exporterSpan[spanKeyID]
		// span has no ID; cannot be matched or correlated
		if spanID == "" {
			spanComparison := SpanComparison{
				MissingIn: []MissingField{
					{Attribute: spanKeyID, Side: exporterSide},
				},
				Passed: false,
			}
			results = append(results, spanComparison)
			continue
		}
		// skip already processed span IDs to avoid double-reporting
		// spans already handled as matched pairs or duplicates
		if _, seen := processedSpanIDs[spanID]; seen {
			continue
		}
		// matched pair; report field-level differences
		results = append(results, SpanComparison{
			SpanID: spanID,
			OnlyIn: exporterSide,
		})
		processedSpanIDs[spanID] = struct{}{}
	}

	return results
}

func groupSpansByID(spans []map[string]string) map[string][]map[string]string {
	byID := make(map[string][]map[string]string, len(spans))
	for _, span := range spans {
		// spans with no span_id are excluded; they're reported separately as invalid
		if spanID := span[spanKeyID]; spanID != "" {
			byID[spanID] = append(byID[spanID], span)
		}
	}
	return byID
}

func isDuplicateSpanID(
	id string,
	receiverSpansBySpanID map[string][]map[string]string,
	exporterSpansBySpanID map[string][]map[string]string,
) bool {
	// absent keys return nil slices; len(nil) == 0, so this is safe
	return len(receiverSpansBySpanID[id]) > 1 || len(exporterSpansBySpanID[id]) > 1
}

// spanSkipKeys lists raw span fields excluded from comparison because they carry no fidelity
// signal (correlation_id is a synthetic test key; it will never appear in real exporter traffic).
var spanSkipKeys = map[string]struct{}{ //nolint:gochecknoglobals
	"meta.correlation_id": {},
}

type traceGroup struct {
	spans []map[string]string
	extra map[string]string
}

func newTraceGroup(rawGroup map[string]string) traceGroup {
	if len(rawGroup) == 0 {
		return traceGroup{}
	}

	groups := make(map[int]map[string]string)
	extra := make(map[string]string, len(rawGroup))
	maxIdx := -1

	for key, value := range rawGroup {
		if !strings.HasPrefix(key, "[") {
			extra[key] = value
			continue
		}

		rest := strings.TrimPrefix(key, "[")
		idxStr, after, ok := strings.Cut(rest, "]")
		if !ok {
			continue
		}
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			continue
		}
		subKey := strings.TrimPrefix(after, ".")
		if subKey == "" {
			subKey = key
		}
		if _, ok := groups[idx]; !ok {
			groups[idx] = make(map[string]string)
		}
		groups[idx][subKey] = value
		if idx > maxIdx {
			maxIdx = idx
		}
	}

	if len(groups) == 0 {
		return traceGroup{
			spans: []map[string]string{rawGroup},
			extra: extra,
		}
	}

	spans := make([]map[string]string, 0, len(groups))
	for i := 0; i <= maxIdx; i++ {
		if g, ok := groups[i]; ok {
			spans = append(spans, g)
		}
	}
	return traceGroup{
		spans: spans,
		extra: extra,
	}
}

func (g traceGroup) flatten() map[string]string {
	result := make(map[string]string, len(g.extra)+len(g.spans)*8)
	maps.Copy(result, g.extra)
	for i, span := range g.spans {
		prefix := fmt.Sprintf("[%d].", i)
		for k, v := range span {
			result[prefix+k] = v
		}
	}
	return result
}

func compareSpanIDs(a map[string]string,
	b map[string]string) int {
	aID, aErr := strconv.ParseUint(a[spanKeyID], 10, 64)
	bID, bErr := strconv.ParseUint(b[spanKeyID], 10, 64)
	aNumeric := aErr == nil
	bNumeric := bErr == nil

	switch {
	case aNumeric && bNumeric:
		return cmp.Compare(aID, bID)
	case aNumeric:
		return -1
	case bNumeric:
		return 1
	default:
		return strings.Compare(a[spanKeyID], b[spanKeyID])
	}
}

func diffSpanPair(spanID string,
	receiverSpans map[string]string,
	exporterSpans map[string]string) SpanComparison {
	result := SpanComparison{
		SpanID: spanID,
	}

	for _, key := range mergeAndSortKeys(receiverSpans, exporterSpans) {
		if _, skip := spanSkipKeys[key]; skip {
			continue
		}
		receiverValue, receiverValueExists := receiverSpans[key]
		exporterValue, exporterValueExists := exporterSpans[key]
		switch {
		case receiverValueExists && exporterValueExists && receiverValue == exporterValue:
			result.Matched = append(result.Matched, key)
		case receiverValueExists && exporterValueExists:
			result.Mismatched = append(result.Mismatched, SpanDelta{
				Attribute: key,
				Receiver:  receiverValue,
				Exporter:  exporterValue,
			})
		case receiverValueExists:
			result.MissingIn = append(result.MissingIn, MissingField{
				Attribute: key,
				Side:      exporterSide,
				Value:     receiverValue,
			})
		default:
			result.MissingIn = append(result.MissingIn, MissingField{
				Attribute: key,
				Side:      receiverSide,
				Value:     exporterValue,
			})
		}
	}

	result.Passed = len(result.Mismatched) == 0 && len(result.MissingIn) == 0
	return result
}
