// Package calltest supplies extraction-safe call adapter conformance fixtures.
package calltest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
)

const EventDefinitionResolved = "call.definition_resolved"

// ResolutionEvent is appended atomically with its matching resolution. The
// event contains no effective input values, only their digest.
type ResolutionEvent struct {
	Sequence uint64                       `json:"sequence"`
	Type     string                       `json:"type"`
	Record   calladapter.ResolutionRecord `json:"record"`
}

// Journal is a restart-serializable reference ResolutionStore. It exists for
// adapter/conformance tests and embedded hosts; Hadron's production host binds
// the same atomic contract to its durable application store in W05.
type Journal struct {
	mu      sync.RWMutex
	records map[string]calladapter.ResolutionRecord
	events  []ResolutionEvent
}

type journalSnapshot struct {
	Records map[string]calladapter.ResolutionRecord `json:"records"`
	Events  []ResolutionEvent                       `json:"events"`
}

// NewJournal creates an empty exact-replay resolution journal.
func NewJournal() *Journal {
	return &Journal{records: make(map[string]calladapter.ResolutionRecord)}
}

// RestoreJournal constructs a fresh process instance from a durable Snapshot.
func RestoreJournal(snapshot []byte) (*Journal, error) {
	var persisted journalSnapshot
	if err := decodeJSON(snapshot, &persisted); err != nil {
		return nil, fmt.Errorf("restore call resolution journal: %w", err)
	}
	journal := NewJournal()
	for key, record := range persisted.Records {
		if key != record.Key {
			return nil, fmt.Errorf("restore call resolution journal: record key mismatch")
		}
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("restore call resolution journal: %w", err)
		}
		journal.records[key] = cloneRecord(record)
	}
	seenEvents := make(map[string]struct{}, len(persisted.Events))
	for index, event := range persisted.Events {
		if event.Sequence != uint64(index+1) || event.Type != EventDefinitionResolved {
			return nil, fmt.Errorf("restore call resolution journal: invalid event sequence or type")
		}
		record, ok := journal.records[event.Record.Key]
		if !ok || !equalRecord(record, event.Record) {
			return nil, fmt.Errorf("restore call resolution journal: event diverges from record")
		}
		if _, duplicate := seenEvents[event.Record.Key]; duplicate {
			return nil, fmt.Errorf("restore call resolution journal: duplicate event for record")
		}
		seenEvents[event.Record.Key] = struct{}{}
		journal.events = append(journal.events, cloneEvent(event))
	}
	if len(journal.records) != len(journal.events) {
		return nil, fmt.Errorf("restore call resolution journal: record/event cardinality differs")
	}
	return journal, nil
}

// RecordCallResolution implements call.ResolutionStore. Record and event are
// committed under one lock; validation completes before either mutation.
func (j *Journal) RecordCallResolution(ctx context.Context, request calladapter.RecordResolutionRequest) (calladapter.ResolutionRecord, calladapter.ResolutionOutcome, error) {
	if ctx == nil {
		return calladapter.ResolutionRecord{}, "", errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return calladapter.ResolutionRecord{}, "", err
	}
	if j == nil {
		return calladapter.ResolutionRecord{}, "", errors.New("call resolution journal is nil")
	}
	if err := request.Record.Validate(); err != nil {
		return calladapter.ResolutionRecord{}, "", err
	}
	candidate := cloneRecord(request.Record)
	event := ResolutionEvent{Type: EventDefinitionResolved, Record: cloneRecord(candidate)}

	j.mu.Lock()
	defer j.mu.Unlock()
	if prior, ok := j.records[candidate.Key]; ok {
		if !equalRecord(prior, candidate) {
			return calladapter.ResolutionRecord{}, "", fmt.Errorf("%w: key %q", calladapter.ErrResolutionConflict, candidate.Key)
		}
		return cloneRecord(prior), calladapter.ResolutionReplayed, nil
	}
	event.Sequence = uint64(len(j.events) + 1)
	j.records[candidate.Key] = candidate
	j.events = append(j.events, event)
	return cloneRecord(candidate), calladapter.ResolutionApplied, nil
}

// Snapshot returns a deterministic restart payload containing records and
// their atomically paired events.
func (j *Journal) Snapshot() ([]byte, error) {
	if j == nil {
		return nil, errors.New("call resolution journal is nil")
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	records := make(map[string]calladapter.ResolutionRecord, len(j.records))
	for key, record := range j.records {
		records[key] = cloneRecord(record)
	}
	events := make([]ResolutionEvent, len(j.events))
	for index, event := range j.events {
		events[index] = cloneEvent(event)
	}
	return json.Marshal(journalSnapshot{Records: records, Events: events})
}

// Events returns defensive copies in append order.
func (j *Journal) Events() []ResolutionEvent {
	if j == nil {
		return nil
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	result := make([]ResolutionEvent, len(j.events))
	for index, event := range j.events {
		result[index] = cloneEvent(event)
	}
	return result
}

func cloneRecord(record calladapter.ResolutionRecord) calladapter.ResolutionRecord {
	var cloned calladapter.ResolutionRecord
	encoded, _ := json.Marshal(record)
	_ = decodeJSON(encoded, &cloned)
	return cloned
}

func cloneEvent(event ResolutionEvent) ResolutionEvent {
	return ResolutionEvent{Sequence: event.Sequence, Type: event.Type, Record: cloneRecord(event.Record)}
}

func equalRecord(left, right calladapter.ResolutionRecord) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func decodeJSON(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

var _ calladapter.ResolutionStore = (*Journal)(nil)
