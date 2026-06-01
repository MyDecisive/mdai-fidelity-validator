package validator

import "slices"

// compareSpans partitions both raw trace groups into per-span maps by their [j].* bracket
// index, matches spans across sides by span_id, and returns one SpanComparison per matched
// or unmatched span.
func compareSpans(receiverRaw, exporterRaw map[string]string) []SpanComparison {
	receiverSpans := extractSpanMaps(receiverRaw)
	exporterSpans := extractSpanMaps(exporterRaw)

	expByID := make(map[string]map[string]string, len(exporterSpans))
	for _, span := range exporterSpans {
		if id := span["span_id"]; id != "" {
			expByID[id] = span
		}
	}

	matchedIDs := make(map[string]struct{}, len(receiverSpans))
	results := make([]SpanComparison, 0, len(receiverSpans))

	for _, recvSpan := range receiverSpans {
		id := recvSpan["span_id"]
		expSpan, found := expByID[id]
		if !found {
			results = append(results, SpanComparison{
				SpanID: id,
				Name:   recvSpan["name"],
				OnlyIn: "receiver",
			})
			continue
		}
		matchedIDs[id] = struct{}{}
		results = append(results, diffSpanPair(id, recvSpan, expSpan))
	}

	for _, expSpan := range exporterSpans {
		id := expSpan["span_id"]
		if _, ok := matchedIDs[id]; !ok {
			results = append(results, SpanComparison{
				SpanID: id,
				Name:   expSpan["name"],
				OnlyIn: "exporter",
			})
		}
	}

	return results
}

func extractSpanMaps(rawGroup map[string]string) []map[string]string {
	if len(rawGroup) == 0 {
		return nil
	}
	return extractIndexedGroups(rawGroup, "")
}

// spanSkipKeys lists raw span fields excluded from comparison because they carry no fidelity
// signal (correlation_id is a synthetic test key; it will never appear in real exporter traffic).
var spanSkipKeys = map[string]struct{}{ //nolint:gochecknoglobals
	"meta.correlation_id": {},
}

func diffSpanPair(spanID string, recv, exp map[string]string) SpanComparison {
	result := SpanComparison{
		SpanID: spanID,
		Name:   recv["name"],
	}

	allKeys := make(map[string]struct{}, len(recv)+len(exp))
	for k := range recv {
		allKeys[k] = struct{}{}
	}
	for k := range exp {
		allKeys[k] = struct{}{}
	}
	keys := make([]string, 0, len(allKeys))
	for k := range allKeys {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, key := range keys {
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
