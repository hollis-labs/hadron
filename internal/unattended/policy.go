package unattended

import (
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

// Decision attribute keys an Allow from the list carries. They land in the
// persisted policy evaluation, so every unattended start names the entry
// that let it run and when that entry and the file were written.
const (
	AttrEntry     = "allowlist_entry"
	AttrReason    = "allowlist_reason"
	AttrAddedBy   = "allowlist_added_by"
	AttrAddedAt   = "allowlist_added_at"
	AttrFileMTime = "allowlist_file_mtime"
)

// Apply upgrades a Confirm decision to Allow when a live entry covers the
// start. Allow and Deny pass through untouched: the list can only waive a
// confirmation, never override a denial. A nil store changes nothing.
func Apply(store *Store, facts hoststate.PolicyFacts, decision hoststate.PolicyDecision, now time.Time) hoststate.PolicyDecision {
	if store == nil || decision.Outcome != hoststate.PolicyConfirm {
		return decision
	}
	snapshot := store.Snapshot()
	entry, ok := Match(snapshot.Entries, StartFacts{
		PlanID: facts.Plan.ID, Digest: facts.Plan.Digest,
		ActivationID: facts.ActivationID, Principal: facts.Identity.Principal,
	}, now)
	if !ok {
		return decision
	}
	attributes := make(map[string]string, len(decision.Attributes)+5)
	for key, value := range decision.Attributes {
		attributes[key] = value
	}
	attributes[AttrEntry] = entry.ID
	attributes[AttrReason] = entry.Reason
	attributes[AttrAddedBy] = entry.AddedBy
	attributes[AttrAddedAt] = entry.AddedAt.UTC().Format(time.RFC3339Nano)
	if !snapshot.ModTime.IsZero() {
		attributes[AttrFileMTime] = snapshot.ModTime.UTC().Format(time.RFC3339Nano)
	}
	return hoststate.PolicyDecision{
		ID: decision.ID, RunID: decision.RunID, Operation: decision.Operation,
		Outcome:    hoststate.PolicyAllow,
		Reason:     "unattended allow-list entry " + entry.ID + " waives confirmation: " + entry.Reason,
		Attributes: attributes, DecidedAt: decision.DecidedAt,
	}
}
