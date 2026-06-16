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
	spanKeyName  = "name"
	receiverSide = "receiver"
	exporterSide = "exporter"
	missing      = "missing"
)

// compareSpans matches parsed receiver/exporter spans by span_id and returns one
// SpanComparison per matched, unmatched, or invalid span identity.
func compareSpans(receiverSpans, exporterSpans []map[string]string) []SpanComparison {
	receiverByID := groupSpansByID(receiverSpans)
	exporterByID := groupSpansByID(exporterSpans)
	processedIDs := make(map[string]struct{}, len(receiverByID)+len(exporterByID))
	results := make([]SpanComparison, 0, len(receiverSpans)+len(exporterSpans))

	for _, receiverSpan := range receiverSpans {
		id := receiverSpan[spanKeyID]
		if id == "" {
			results = append(results, missingSpanID(receiverSide, receiverSpan))
			continue
		}
		if _, seen := processedIDs[id]; seen {
			continue
		}
		if isDuplicateSpanID(id, receiverByID, exporterByID) {
			results = append(results, duplicateSpanID(id, receiverByID[id], exporterByID[id]))
			processedIDs[id] = struct{}{}
			continue
		}
		exporterSpans := exporterByID[id]
		if len(exporterSpans) == 0 {
			results = append(results, SpanComparison{
				SpanID: id,
				Name:   receiverSpan[spanKeyName],
				OnlyIn: receiverSide,
			})
			processedIDs[id] = struct{}{}
			continue
		}
		results = append(results, diffSpanPair(id, receiverSpan, exporterSpans[0]))
		processedIDs[id] = struct{}{}
	}

	for _, expSpan := range exporterSpans {
		id := expSpan[spanKeyID]
		if id == "" {
			results = append(results, missingSpanID(exporterSide, expSpan))
			continue
		}
		if _, seen := processedIDs[id]; seen {
			continue
		}
		if isDuplicateSpanID(id, receiverByID, exporterByID) {
			results = append(results, duplicateSpanID(id, receiverByID[id], exporterByID[id]))
			processedIDs[id] = struct{}{}
			continue
		}
		results = append(results, SpanComparison{
			SpanID: id,
			Name:   expSpan[spanKeyName],
			OnlyIn: exporterSide,
		})
		processedIDs[id] = struct{}{}
	}

	return results
}

func groupSpansByID(spans []map[string]string) map[string][]map[string]string {
	byID := make(map[string][]map[string]string, len(spans))
	for _, span := range spans {
		// spans with no span_id are excluded; they're reported separately as invalid
		if id := span[spanKeyID]; id != "" {
			byID[id] = append(byID[id], span)
		}
	}
	return byID
}

func isDuplicateSpanID(id string, receiverByID, exporterByID map[string][]map[string]string) bool {
	// absent keys return nil slices; len(nil) == 0, so this is safe
	return len(receiverByID[id]) > 1 || len(exporterByID[id]) > 1
}

// duplicateSpanID builds a SpanComparison that flags both sides duplicate counts
func duplicateSpanID(id string, receiverSpans, exporterSpans []map[string]string) SpanComparison {
	return SpanComparison{
		SpanID: id,
		Name:   firstSpanName(receiverSpans, exporterSpans),
		Mismatched: []SpanDelta{
			{
				Attribute: spanKeyID,
				Receiver:  fmt.Sprintf("duplicate count: %d", len(receiverSpans)),
				Exporter:  fmt.Sprintf("duplicate count: %d", len(exporterSpans)),
			},
		},
		Passed: false,
	}
}

// missingSpanID builds a SpanComparison that flags a missing ID
func missingSpanID(side string, span map[string]string) SpanComparison {
	result := SpanComparison{
		Name:   span[spanKeyName],
		OnlyIn: side,
		Mismatched: []SpanDelta{
			{
				Attribute: spanKeyID,
			},
		},
		Passed: false,
	}
	if side == receiverSide {
		result.Mismatched[0].Receiver = missing
		return result
	}
	result.Mismatched[0].Exporter = missing
	return result
}

func firstSpanName(
	receiverSpans []map[string]string,
	exporterSpans []map[string]string,
) string {
	if len(receiverSpans) > 0 {
		return receiverSpans[0][spanKeyName]
	}
	if len(exporterSpans) > 0 {
		return exporterSpans[0][spanKeyName]
	}
	return ""
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
		Name:   receiverSpans[spanKeyName],
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
