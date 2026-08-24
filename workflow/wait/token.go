package wait

import (
	"crypto/subtle"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/values"
)

// DigestToken returns the only representation of a resume token that may be
// persisted. Raw tokens are returned once by the creating host and never
// stored or logged by core.
func DigestToken(token string) (string, error) {
	if !utf8.ValidString(token) || strings.TrimSpace(token) == "" {
		return "", fmt.Errorf("resume token is required and must contain valid UTF-8")
	}
	return values.SHA256Digest([]byte(token)), nil
}

// EqualTokenDigest reports whether two validated canonical token digests are
// equal without using an ordinary credential string comparison. Invalid or
// non-canonical digests never compare equal.
func EqualTokenDigest(left, right string) bool {
	if values.ValidateDigest(left) != nil || values.ValidateDigest(right) != nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}
