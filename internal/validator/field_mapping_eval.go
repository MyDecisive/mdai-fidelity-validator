package validator

import (
	"encoding/json"
	"path"
	"slices"
	"strings"
)

func extractMappedValue(fields map[string]string, source string) (string, bool) {
	source = strings.TrimSpace(source)
	if source == "" {
		return "", false
	}

	parts := strings.Split(source, "|")
	base := strings.TrimSpace(parts[0])
	ops := parts[1:]

	for _, candidate := range fieldCandidates(fields, base) {
		value, ok := fields[candidate]
		if !ok {
			continue
		}
		current := value
		valid := true
		for _, op := range ops {
			current, valid = applyFieldOp(current, strings.TrimSpace(op))
			if !valid {
				break
			}
		}
		if valid && strings.TrimSpace(current) != "" {
			return current, true
		}
	}

	return "", false
}

func fieldCandidates(fields map[string]string, base string) []string {
	if after, ok := strings.CutPrefix(base, "contains:"); ok {
		needle := strings.TrimSpace(after)
		candidates := make([]string, 0)
		for key := range fields {
			if strings.Contains(key, needle) {
				candidates = append(candidates, key)
			}
		}
		slices.Sort(candidates)
		return candidates
	}
	if after, ok := strings.CutPrefix(base, "suffix:"); ok {
		suffix := strings.TrimSpace(after)
		candidates := make([]string, 0)
		for key := range fields {
			if strings.HasSuffix(key, suffix) {
				candidates = append(candidates, key)
			}
		}
		slices.Sort(candidates)
		return candidates
	}
	if strings.ContainsAny(base, "*?") {
		candidates := make([]string, 0)
		for key := range fields {
			if ok, _ := path.Match(base, key); ok {
				candidates = append(candidates, key)
			}
		}
		slices.Sort(candidates)
		return candidates
	}
	return []string{base}
}

func applyFieldOp(value, op string) (string, bool) {
	switch {
	case op == "":
		return value, true
	case strings.HasPrefix(op, "json:"):
		return applyJSONOp(value, strings.TrimSpace(strings.TrimPrefix(op, "json:")))
	case strings.HasPrefix(op, "tag:"):
		return applyTagOp(value, strings.TrimSpace(strings.TrimPrefix(op, "tag:")))
	case op == "trim":
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return "", false
		}
		return trimmed, true
	default:
		return "", false
	}
}

func applyJSONOp(value, fieldPath string) (string, bool) {
	fieldPath = strings.TrimSpace(fieldPath)
	if fieldPath == "" {
		return "", false
	}

	var payload any
	if err := json.Unmarshal([]byte(strings.TrimSpace(value)), &payload); err != nil {
		return "", false
	}

	current := payload
	for part := range strings.SplitSeq(fieldPath, ".") {
		part = strings.TrimSpace(part)
		if part == "" {
			return "", false
		}
		object, ok := current.(map[string]any)
		if !ok {
			return "", false
		}
		next, ok := object[part]
		if !ok {
			return "", false
		}
		current = next
	}

	switch typed := current.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return "", false
		}
		return typed, true
	default:
		body, err := json.Marshal(typed)
		if err != nil {
			return "", false
		}
		out := strings.Trim(string(body), "\"")
		if strings.TrimSpace(out) == "" {
			return "", false
		}
		return out, true
	}
}

func applyTagOp(value, key string) (string, bool) {
	for part := range strings.SplitSeq(value, ",") {
		tagKey, tagValue, ok := parseTagKV(part)
		if !ok {
			continue
		}
		if tagKey == key {
			if strings.TrimSpace(tagValue) == "" {
				return "", false
			}
			return tagValue, true
		}
	}
	if key == "fidelity_correlation_id" {
		for part := range strings.SplitSeq(value, ",") {
			tagKey, tagValue, ok := parseTagKV(part)
			if !ok {
				continue
			}
			if tagKey == "fidelity.correlation_id" {
				if strings.TrimSpace(tagValue) == "" {
					return "", false
				}
				return tagValue, true
			}
		}
	}
	return "", false
}
