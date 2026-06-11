package validator

import (
	"cmp"
	"fmt"
	"maps"
	"strconv"
	"strings"
)

// compareSpans matches parsed receiver/exporter spans by span_id and returns one
// SpanComparison per matched, unmatched, or invalid span identity.
func compareSpans(receiverSpans, exporterSpans []map[string]string) []SpanComparison {
	recvByID := bucketSpansByID(receiverSpans)
	expByID := bucketSpansByID(exporterSpans)
	processedIDs := make(map[string]struct{}, len(recvByID)+len(expByID))
	results := make([]SpanComparison, 0, len(receiverSpans)+len(exporterSpans))

	for _, recvSpan := range receiverSpans {
		id := recvSpan["span_id"]
		if id == "" {
			results = append(results, invalidSpanIDComparison("receiver", recvSpan))
			continue
		}
		if _, seen := processedIDs[id]; seen {
			continue
		}
		if isDuplicateSpanID(id, recvByID, expByID) {
			results = append(results, duplicateSpanIDComparison(id, recvByID[id], expByID[id]))
			processedIDs[id] = struct{}{}
			continue
		}
		expSpans := expByID[id]
		if len(expSpans) == 0 {
			results = append(results, SpanComparison{
				SpanID: id,
				Name:   recvSpan["name"],
				OnlyIn: "receiver",
			})
			processedIDs[id] = struct{}{}
			continue
		}
		results = append(results, diffSpanPair(id, recvSpan, expSpans[0]))
		processedIDs[id] = struct{}{}
	}

	for _, expSpan := range exporterSpans {
		id := expSpan["span_id"]
		if id == "" {
			results = append(results, invalidSpanIDComparison("exporter", expSpan))
			continue
		}
		if _, seen := processedIDs[id]; seen {
			continue
		}
		if isDuplicateSpanID(id, recvByID, expByID) {
			results = append(results, duplicateSpanIDComparison(id, recvByID[id], expByID[id]))
			processedIDs[id] = struct{}{}
			continue
		}
		results = append(results, SpanComparison{
			SpanID: id,
			Name:   expSpan["name"],
			OnlyIn: "exporter",
		})
		processedIDs[id] = struct{}{}
	}

	return results
}

func bucketSpansByID(spans []map[string]string) map[string][]map[string]string {
	byID := make(map[string][]map[string]string, len(spans))
	for _, span := range spans {
		if id := span["span_id"]; id != "" {
			byID[id] = append(byID[id], span)
		}
	}
	return byID
}

func isDuplicateSpanID(id string, receiverByID, exporterByID map[string][]map[string]string) bool {
	return len(receiverByID[id]) > 1 || len(exporterByID[id]) > 1
}

func duplicateSpanIDComparison(id string, receiverSpans, exporterSpans []map[string]string) SpanComparison {
	return SpanComparison{
		SpanID: id,
		Name:   firstSpanName(receiverSpans, exporterSpans),
		Mismatched: []SpanDelta{
			{
				Attribute: "span_id",
				Receiver:  fmt.Sprintf("duplicate count: %d", len(receiverSpans)),
				Exporter:  fmt.Sprintf("duplicate count: %d", len(exporterSpans)),
			},
		},
		Passed: false,
	}
}

func invalidSpanIDComparison(side string, span map[string]string) SpanComparison {
	result := SpanComparison{
		Name:   span["name"],
		OnlyIn: side,
		Mismatched: []SpanDelta{
			{
				Attribute: "span_id",
			},
		},
		Passed: false,
	}
	if side == "receiver" {
		result.Mismatched[0].Receiver = "missing"
		return result
	}
	result.Mismatched[0].Exporter = "missing"
	return result
}

func firstSpanName(receiverSpans, exporterSpans []map[string]string) string {
	if len(receiverSpans) > 0 {
		return receiverSpans[0]["name"]
	}
	if len(exporterSpans) > 0 {
		return exporterSpans[0]["name"]
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

func compareSpanIDs(a, b map[string]string) int {
	aID, aErr := strconv.ParseUint(a["span_id"], 10, 64)
	bID, bErr := strconv.ParseUint(b["span_id"], 10, 64)
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
		return strings.Compare(a["span_id"], b["span_id"])
	}
}

func diffSpanPair(spanID string, recv, exp map[string]string) SpanComparison {
	result := SpanComparison{
		SpanID: spanID,
		Name:   recv["name"],
	}

	for _, key := range mergeAndSortKeys(recv, exp) {
		if _, skip := spanSkipKeys[key]; skip {
			continue
		}
		recvVal, recvOK := recv[key]
		expVal, expOK := exp[key]
		switch {
		case recvOK && expOK && recvVal == expVal:
			result.Matched = append(result.Matched, key)
		case recvOK && expOK:
			result.Mismatched = append(result.Mismatched, SpanDelta{
				Attribute: key,
				Receiver:  recvVal,
				Exporter:  expVal,
			})
		case recvOK:
			result.MissingIn = append(result.MissingIn, MissingField{
				Attribute: key,
				Side:      "exporter",
				Value:     recvVal,
			})
		default:
			result.MissingIn = append(result.MissingIn, MissingField{
				Attribute: key,
				Side:      "receiver",
				Value:     expVal,
			})
		}
	}

	result.Passed = len(result.Mismatched) == 0 && len(result.MissingIn) == 0
	return result
}
