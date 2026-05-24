package rundiagnostics

import (
	"database/sql"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/persistence"
)

func TestFilterAndPageOperations(t *testing.T) {
	items := []OperationDiagnostic{
		{Sequence: 1, Kind: "http_call"},
		{Sequence: 2, Kind: "message_wait"},
		{Sequence: 3, Kind: "http_call"},
	}

	page, next, total, err := FilterAndPageOperations(items, "http_call", 1, "")
	if err != nil {
		t.Fatalf("filter/page: %v", err)
	}
	if len(page) != 1 || page[0].Sequence != 1 || next != "1" || total != 2 {
		t.Fatalf("unexpected first page: page=%#v next=%q total=%d", page, next, total)
	}

	page2, next2, total2, err := FilterAndPageOperations(items, "http_call", 1, next)
	if err != nil {
		t.Fatalf("filter/page second: %v", err)
	}
	if len(page2) != 1 || page2[0].Sequence != 3 || next2 != "" || total2 != 2 {
		t.Fatalf("unexpected second page: page=%#v next=%q total=%d", page2, next2, total2)
	}
}

func TestFilterAndPageOperationsRejectsInvalidCursor(t *testing.T) {
	_, _, _, err := FilterAndPageOperations([]OperationDiagnostic{{Sequence: 1, Kind: "http_call"}}, "", 10, "bad")
	if err == nil {
		t.Fatal("expected invalid cursor error")
	}
}

func TestSummarizeOperations_HumanGate(t *testing.T) {
	now := time.Now().UTC()
	events := []persistence.RunEventRecord{
		{
			ID:        1,
			RunID:     "run-1",
			StepName:  sql.NullString{String: "approve", Valid: true},
			EventType: "human_gate_waiting",
			Message:   sql.NullString{String: `{"gate_id":"gate-1","prompt":"Approve?","timeout_seconds":60}`, Valid: true},
			CreatedAt: now,
		},
		{
			ID:        2,
			RunID:     "run-1",
			StepName:  sql.NullString{String: "approve", Valid: true},
			EventType: "human_gate_decision",
			Message:   sql.NullString{String: `{"gate_id":"gate-1","decision":"approve"}`, Valid: true},
			CreatedAt: now.Add(time.Second),
		},
	}
	items := SummarizeOperations(events)
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	item := items[0]
	if item.Kind != "human_gate" || item.GateID != "gate-1" || item.Decision != "approve" || item.Status != "success" {
		t.Fatalf("unexpected human gate diagnostic: %#v", item)
	}
}
