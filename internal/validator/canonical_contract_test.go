package validator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateCanonicalAttributes(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateCanonicalAttributes(map[string]string{"message": "ok", "service_name": "svc"}))
	require.Error(t, validateCanonicalAttributes(map[string]string{"[0].message": "bad"}))
}
