package validator

import "strings"

func parseTagKV(tag string) (string, string, bool) {
	parts := strings.SplitN(tag, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}
