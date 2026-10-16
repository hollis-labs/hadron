package eventmap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/hollis-labs/libs/harness/core/eventstage"
)

// MaxPayloadBytes bounds what the sanitizer will look at; larger fails closed.
const MaxPayloadBytes = 64 << 10

// Allowlist is a strict PayloadSanitizer: for each event kind it keeps only the
// listed JSON pointers (a pointer to an object keeps its whole subtree) and drops
// everything else. A kind with no entry keeps nothing. It fails closed on invalid
// JSON, on a payload that is not an object, and on one over MaxPayloadBytes. It is
// the Hadron-local stand-in for the shared redaction adapter, behind the same
// eventstage.PayloadSanitizer seam.
type Allowlist map[string][]string

var _ eventstage.PayloadSanitizer = Allowlist(nil)

// SanitizePayload implements eventstage.PayloadSanitizer.
func (a Allowlist) SanitizePayload(_ context.Context, kind string, payload []byte) ([]byte, eventstage.PayloadReport, error) {
	if len(payload) > MaxPayloadBytes {
		return nil, eventstage.PayloadReport{ErrorClass: "oversized"}, nil
	}
	var doc map[string]any
	if json.Unmarshal(payload, &doc) != nil || doc == nil {
		return nil, eventstage.PayloadReport{ErrorClass: "invalid_json"}, nil //nolint:nilerr // a class on the report, not an error: the adapter fails the event closed
	}
	kept := map[string]any{}
	for _, pointer := range a[kind] {
		if value, ok := lookup(doc, pointer); ok {
			insert(kept, pointer, value)
		}
	}
	dropped := countLeaves(doc) - countLeaves(kept)
	out, err := json.Marshal(kept)
	if err != nil {
		return nil, eventstage.PayloadReport{}, errors.New("marshal sanitized payload")
	}
	if bytes.Equal(out, bytes.TrimSpace(payload)) {
		dropped = 0
	}
	report := eventstage.PayloadReport{Sensitivity: "private"}
	if dropped > 0 {
		report.Redactions = uint32(dropped) // #nosec G115 -- a small non-negative count
	}
	return out, report, nil
}

func lookup(doc map[string]any, pointer string) (any, bool) {
	var cur any = doc
	for _, seg := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = obj[seg]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func insert(dst map[string]any, pointer string, value any) {
	segs := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	cur := dst
	for _, seg := range segs[:len(segs)-1] {
		next, ok := cur[seg].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[seg] = next
		}
		cur = next
	}
	cur[segs[len(segs)-1]] = value
}

func countLeaves(v any) int {
	switch t := v.(type) {
	case map[string]any:
		n := 0
		for _, c := range t {
			n += countLeaves(c)
		}
		return n
	case []any:
		n := 0
		for _, c := range t {
			n += countLeaves(c)
		}
		return n
	}
	return 1
}
