package appworkflow

import (
	"testing"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

func TestCompensationInspectionProjectionDefaultsAndCapsAtHostBatchLimit(t *testing.T) {
	host := &Host{batchLimit: 2}
	ledger := workflowruntime.CompensationLedgerSnapshot{RunID: "bounded-compensation", Cycles: []workflowruntime.CompensationCycle{{Number: 1}, {Number: 2}, {Number: 3}}}
	entries := []workflowruntime.CompensationEntrySnapshot{{ID: "one", History: []workflowruntime.CompensationEntryHistory{{Cycle: 1}, {Cycle: 2}, {Cycle: 3}}}, {ID: "two"}, {ID: "three"}}

	for name, requested := range map[string]int{"zero defaults": 0, "oversized caps": 1000} {
		t.Run(name, func(t *testing.T) {
			limit := requested
			maximum := host.compensationResultLimit()
			if limit == 0 || limit > maximum {
				limit = maximum
			}
			result := boundedCompensationProjection(ledger, entries, limit)
			if len(result.Entries) != 2 || len(result.Ledger.Cycles) != 2 || result.Ledger.Cycles[0].Number != 2 || len(result.Entries[0].History) != 2 || result.Entries[0].History[0].Cycle != 2 || !result.Truncated || result.Entries[0].ID != "one" || result.Entries[1].ID != "two" {
				t.Fatalf("bounded projection = %#v", result)
			}
		})
	}

	limited := boundedCompensationProjection(ledger, entries, 1)
	if len(limited.Entries) != 1 || !limited.Truncated {
		t.Fatalf("explicit smaller limit = %#v", limited)
	}
}
