package runtime

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// EncodeAttemptIdentity returns a collision-free, transport-safe durable key
// for an attempt whose opaque identity components may contain delimiters. The
// encoding is stable and reversible by component, but callers must treat the
// result as an opaque storage key rather than parse it themselves.
func EncodeAttemptIdentity(id AttemptID) (string, error) {
	if err := id.Validate(); err != nil {
		return "", fmt.Errorf("encode attempt identity: %w", err)
	}
	encode := base64.RawURLEncoding.EncodeToString
	return strings.Join([]string{
		encode([]byte(id.Invocation.RunID)),
		encode([]byte(id.Invocation.NodeID)),
		encode([]byte(id.Invocation.Iteration)),
		strconv.Itoa(id.Number),
	}, "."), nil
}
