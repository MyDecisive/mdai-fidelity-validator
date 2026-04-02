package validator

import "testing"

func TestValidateCanonicalAttributes(t *testing.T) {
	if err := validateCanonicalAttributes(map[string]string{"message": "ok", "service_name": "svc"}); err != nil {
		t.Fatalf("expected canonical attributes to pass: %v", err)
	}
	if err := validateCanonicalAttributes(map[string]string{"[0].message": "bad"}); err == nil {
		t.Fatal("expected non-canonical key to fail validation")
	}
}
