package appworkflow

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// eventCursorVersion is the layout of an event cursor. A cursor names a place
// in one run's event log; it is opaque to callers and unrelated to any hub
// cursor.
//
// Version 1 names a run and a sequence. Version 2 adds the generation of the
// event log it was issued under, so a cursor cannot be resumed against a
// different database. Both are accepted; a version 1 cursor carries no
// generation and is trusted to the log it meets.
const eventCursorVersion = 2

type eventCursor struct {
	Version    int    `json:"v"`
	Run        string `json:"run"`
	Sequence   uint64 `json:"seq"`
	Generation string `json:"gen,omitempty"`
}

// EncodeEventCursor returns the opaque cursor that resumes after the event
// with the given per-run sequence in the event log named by generation (empty
// when the store does not name its log).
func EncodeEventCursor(run RunID, sequence uint64, generation string) string {
	version := eventCursorVersion
	if generation == "" {
		version = 1
	}
	data, _ := json.Marshal(eventCursor{Version: version, Run: string(run), Sequence: sequence, Generation: generation})
	return base64.RawURLEncoding.EncodeToString(data)
}

// decodeEventCursor returns the sequence an event cursor resumes after and the
// log generation it was issued under (empty for a version 1 cursor). An empty
// cursor starts from the beginning. A cursor that is malformed, of an
// unknown version, or issued for another run is rejected: resuming one run
// from another run's position would silently skip or repeat events.
func decodeEventCursor(run RunID, cursor string) (uint64, string, error) {
	if cursor == "" {
		return 0, "", nil
	}
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, "", fmt.Errorf("%w: malformed event cursor", ErrWorkflowInvalidRequest)
	}
	var decoded eventCursor
	if err := json.Unmarshal(data, &decoded); err != nil || decoded.Version < 1 || decoded.Version > eventCursorVersion || (decoded.Version == 1 && decoded.Generation != "") {
		return 0, "", fmt.Errorf("%w: unsupported event cursor", ErrWorkflowInvalidRequest)
	}
	if decoded.Run != string(run) {
		return 0, "", fmt.Errorf("%w: event cursor belongs to another run", ErrWorkflowInvalidRequest)
	}
	return decoded.Sequence, decoded.Generation, nil
}
