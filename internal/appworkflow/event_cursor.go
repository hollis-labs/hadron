package appworkflow

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// eventCursorVersion is the layout of an event cursor. A cursor names a place
// in one run's event log; it is opaque to callers and unrelated to any hub
// cursor.
const eventCursorVersion = 1

type eventCursor struct {
	Version  int    `json:"v"`
	Run      string `json:"run"`
	Sequence uint64 `json:"seq"`
}

// EncodeEventCursor returns the opaque cursor that resumes after the event
// with the given per-run sequence.
func EncodeEventCursor(run RunID, sequence uint64) string {
	data, _ := json.Marshal(eventCursor{Version: eventCursorVersion, Run: string(run), Sequence: sequence})
	return base64.RawURLEncoding.EncodeToString(data)
}

// decodeEventCursor returns the sequence an event cursor resumes after. An
// empty cursor starts from the beginning. A cursor that is malformed, of an
// unknown version, or issued for another run is rejected: resuming one run
// from another run's position would silently skip or repeat events.
func decodeEventCursor(run RunID, cursor string) (uint64, error) {
	if cursor == "" {
		return 0, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, fmt.Errorf("%w: malformed event cursor", ErrWorkflowInvalidRequest)
	}
	var decoded eventCursor
	if err := json.Unmarshal(data, &decoded); err != nil || decoded.Version != eventCursorVersion {
		return 0, fmt.Errorf("%w: unsupported event cursor", ErrWorkflowInvalidRequest)
	}
	if decoded.Run != string(run) {
		return 0, fmt.Errorf("%w: event cursor belongs to another run", ErrWorkflowInvalidRequest)
	}
	return decoded.Sequence, nil
}
