package validator

import (
	"fmt"
	"regexp"
)

var canonicalKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func validateCanonicalAttributes(attributes map[string]string) error {
	for key := range attributes {
		if !canonicalKeyPattern.MatchString(key) {
			return fmt.Errorf("key %q is not canonical (expected snake_case)", key)
		}
	}
	return nil
}
