package persistence

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func coauthorCredentialRequest(r *credentialTestRig, key string, generation uint64) hoststate.IssueCredentialRequest {
	return hoststate.IssueCredentialRequest{
		PrincipalID: r.principal.ID, CredentialID: r.initial.CredentialID,
		ExpectedGeneration: generation, IdempotencyKey: key,
	}
}

func coauthorCredentialPeer(t *testing.T, r *credentialTestRig) (*Store, *WorkflowExposureStore) {
	t.Helper()
	store, err := Open(r.path)
	if err != nil {
		t.Fatal("independent credential store unavailable")
	}
	t.Cleanup(func() { _ = store.Close() })
	exposure, err := NewWorkflowExposureStore(store)
	if err != nil {
		t.Fatal("independent credential issuer unavailable")
	}
	now := r.now
	exposure.now = func() time.Time { return now }
	return store, exposure
}

type coauthorCredentialSnapshot struct {
	list     hoststate.CredentialList
	audits   int
	receipts int
}

func coauthorCredentialState(t *testing.T, r *credentialTestRig) coauthorCredentialSnapshot {
	t.Helper()
	list, err := r.exposure.ListMCPCredentials(t.Context(), r.principal.ID, r.admin)
	if err != nil {
		t.Fatal("credential metadata unavailable")
	}
	principal, err := r.exposure.GetMCPPrincipal(t.Context(), r.principal.ID)
	if err != nil || principal.Generation != list.Generation || !reflect.DeepEqual(principal.Record, r.principal) {
		t.Fatal("credential mutation changed principal authority")
	}
	snapshot := coauthorCredentialSnapshot{list: list}
	if err := r.store.DB().QueryRowContext(t.Context(), `SELECT COUNT(*) FROM workflow_credential_audit WHERE principal_id=?`, r.principal.ID).Scan(&snapshot.audits); err != nil {
		t.Fatal("credential audit unavailable")
	}
	if err := r.store.DB().QueryRowContext(t.Context(), `SELECT COUNT(*) FROM workflow_credential_operations WHERE principal_id=?`, r.principal.ID).Scan(&snapshot.receipts); err != nil {
		t.Fatal("credential receipt unavailable")
	}
	return snapshot
}

func coauthorAssertIssueReplay(t *testing.T, issued hoststate.CredentialIssue, replay hoststate.CredentialIssue, generation uint64, status string) {
	t.Helper()
	if !replay.Replayed || replay.SecretAvailable || replay.Secret != "" || replay.CredentialID != issued.CredentialID || replay.Generation != generation || replay.OperationGeneration != issued.OperationGeneration || replay.Status != status {
		t.Fatal("issuance replay lost current metadata or repeated private delivery")
	}
}

// Independent database handles exercise the durable CAS, rather than only the
// single-connection serialization within one Store.
func TestCredentialCoauthorConcurrentMutations(t *testing.T) {
	for _, mode := range []string{"different-issue-keys", "same-issue-key", "issue-and-revoke"} {
		t.Run(mode, func(t *testing.T) {
			r := newCredentialTestRig(t)
			_, peer := coauthorCredentialPeer(t, r)
			request := coauthorCredentialRequest(r, "race-left", 1)
			type outcome struct {
				metadata hoststate.CredentialMetadata
				secret   bool
				err      error
			}
			start := make(chan struct{})
			results := make(chan outcome, 2)
			go func() {
				<-start
				issued, err := r.exposure.IssueMCPCredential(t.Context(), request, r.admin)
				results <- outcome{metadata: issued.CredentialMetadata, secret: issued.Secret != "", err: err}
			}()
			go func() {
				<-start
				if mode == "issue-and-revoke" {
					metadata, err := peer.RevokeMCPCredential(t.Context(), hoststate.RevokeCredentialRequest{
						PrincipalID: r.principal.ID, CredentialID: r.initial.CredentialID,
						ExpectedGeneration: 1, IdempotencyKey: "race-revoke",
					}, r.admin)
					results <- outcome{metadata: metadata, err: err}
					return
				}
				other := request
				if mode == "different-issue-keys" {
					other.IdempotencyKey = "race-right"
				}
				issued, err := peer.IssueMCPCredential(t.Context(), other, r.admin)
				results <- outcome{metadata: issued.CredentialMetadata, secret: issued.Secret != "", err: err}
			}()
			close(start)
			first, second := <-results, <-results
			if mode == "same-issue-key" {
				if first.err != nil || second.err != nil || first.secret == second.secret || first.metadata.Replayed == second.metadata.Replayed || first.metadata.CredentialID != second.metadata.CredentialID {
					t.Fatal("concurrent exact retry did not produce one issuance and one metadata-only replay")
				}
				for _, result := range []outcome{first, second} {
					if result.metadata.Generation != 2 || result.metadata.OperationGeneration != 2 || result.metadata.SecretAvailable != result.secret {
						t.Fatal("concurrent retry metadata disagreed with its durable operation")
					}
				}
			} else {
				winner, loser := first, second
				if winner.err != nil {
					winner, loser = second, first
				}
				if winner.err != nil || !errors.Is(loser.err, hoststate.ErrConflict) || loser.secret || !reflect.DeepEqual(loser.metadata, hoststate.CredentialMetadata{}) {
					t.Fatal("competing expected-generation mutations did not produce one clean conflict")
				}
				if winner.metadata.Generation != 2 || winner.metadata.OperationGeneration != 2 || winner.metadata.Replayed || winner.metadata.SecretAvailable != winner.secret {
					t.Fatal("winning mutation did not advance exactly once")
				}
			}
			state := coauthorCredentialState(t, r)
			if state.list.Generation != 2 || state.audits != 1 || state.receipts != 1 {
				t.Fatal("competing mutations committed multiple operations")
			}
			if mode == "issue-and-revoke" && !first.secret && !second.secret {
				if len(state.list.Credentials) != 1 || state.list.Credentials[0].Status != "revoked" {
					t.Fatal("revoke winner left a losing issuance behind")
				}
			} else if len(state.list.Credentials) != 2 {
				t.Fatal("issue winner did not retain exactly one new credential")
			}
		})
	}
}

func TestCredentialCoauthorAuditFailureRollsBackEntireMutation(t *testing.T) {
	for _, operation := range []string{"issue", "revoke"} {
		t.Run(operation, func(t *testing.T) {
			r := newCredentialTestRig(t)
			generation := uint64(1)
			credentialID := r.initial.CredentialID
			if operation == "revoke" {
				issued := r.issue(t, "seed", generation, nil, nil)
				generation, credentialID = issued.Generation, issued.CredentialID
			}
			before := coauthorCredentialState(t, r)
			if _, err := r.store.DB().ExecContext(t.Context(), `CREATE TRIGGER coauthor_reject_audit BEFORE INSERT ON workflow_credential_audit BEGIN SELECT RAISE(ABORT,'synthetic audit refusal'); END`); err != nil {
				t.Fatal("audit fault injection unavailable")
			}
			issueRequest := coauthorCredentialRequest(r, "audit-fault", generation)
			revokeRequest := hoststate.RevokeCredentialRequest{
				PrincipalID: r.principal.ID, CredentialID: credentialID,
				ExpectedGeneration: generation, IdempotencyKey: "audit-fault",
			}
			if operation == "issue" {
				result, err := r.exposure.IssueMCPCredential(t.Context(), issueRequest, r.admin)
				if err == nil || !reflect.DeepEqual(result, hoststate.CredentialIssue{}) {
					t.Fatal("failed issuance exposed a credential result")
				}
			} else {
				result, err := r.exposure.RevokeMCPCredential(t.Context(), revokeRequest, r.admin)
				if err == nil || !reflect.DeepEqual(result, hoststate.CredentialMetadata{}) {
					t.Fatal("failed revocation returned committed metadata")
				}
			}
			if after := coauthorCredentialState(t, r); !reflect.DeepEqual(after, before) {
				t.Fatal("audit failure left generation, credential, overlap, receipt or audit effects")
			}
			if _, err := r.store.DB().ExecContext(t.Context(), `DROP TRIGGER coauthor_reject_audit`); err != nil {
				t.Fatal("audit fault removal unavailable")
			}
			if operation == "issue" {
				result, err := r.exposure.IssueMCPCredential(t.Context(), issueRequest, r.admin)
				if err != nil || result.Replayed || !result.SecretAvailable || result.Secret == "" || result.Generation != generation+1 {
					t.Fatal("rolled-back issuance prevented a fresh same-request retry")
				}
			} else {
				result, err := r.exposure.RevokeMCPCredential(t.Context(), revokeRequest, r.admin)
				if err != nil || result.Replayed || result.Status != "revoked" || result.Generation != generation+1 {
					t.Fatal("rolled-back revocation prevented a fresh same-request retry")
				}
			}
			after := coauthorCredentialState(t, r)
			if after.audits != before.audits+1 || after.receipts != before.receipts+1 {
				t.Fatal("successful retry did not commit its audit and receipt together")
			}
		})
	}
}

func TestCredentialCoauthorLostDeliveryReplayTracksCurrentFamilyAndRevocation(t *testing.T) {
	r := newCredentialTestRig(t)
	request := coauthorCredentialRequest(r, "lost-delivery", 1)
	issued, err := r.exposure.IssueMCPCredential(t.Context(), request, r.admin)
	if err != nil || !issued.SecretAvailable || issued.Secret == "" {
		t.Fatal("initial private issuance unavailable")
	}
	// A different authorized operation is required for a replacement secret.
	replacement := r.issue(t, "new-authorized-delivery", 2, nil, nil)
	r.authenticates(t, replacement.Secret, true)
	before := coauthorCredentialState(t, r)
	explicitDefault := request
	explicitDefault.OverlapSeconds = credentialInt(hoststate.DefaultCredentialOverlapSeconds)
	replay, err := r.exposure.IssueMCPCredential(t.Context(), explicitDefault, r.admin)
	if err != nil {
		t.Fatal("normalized exact retry was refused by stale generation")
	}
	coauthorAssertIssueReplay(t, issued, replay, 3, "active")
	if after := coauthorCredentialState(t, r); !reflect.DeepEqual(after, before) {
		t.Fatal("lost delivery replay repeated the mutation")
	}
	revokeRequest := hoststate.RevokeCredentialRequest{
		PrincipalID: r.principal.ID, CredentialID: issued.CredentialID,
		ExpectedGeneration: 3, IdempotencyKey: "revoke-outstanding",
	}
	revoked, err := r.exposure.RevokeMCPCredential(t.Context(), revokeRequest, r.admin)
	if err != nil || revoked.Status != "revoked" || revoked.Generation != 4 {
		t.Fatal("outstanding credential could not be revoked by ID")
	}
	r.authenticates(t, issued.Secret, false)
	r.authenticates(t, replacement.Secret, true)
	if _, err = r.exposure.PutMCPPrincipal(t.Context(), r.principal, 4); err != nil {
		t.Fatal("independent family mutation unavailable")
	}
	before = coauthorCredentialState(t, r)
	replay, err = r.exposure.IssueMCPCredential(t.Context(), request, r.admin)
	if err != nil {
		t.Fatal("retained issuance receipt unavailable after revocation")
	}
	coauthorAssertIssueReplay(t, issued, replay, 5, "revoked")
	revocationReplay, err := r.exposure.RevokeMCPCredential(t.Context(), revokeRequest, r.admin)
	if err != nil || !revocationReplay.Replayed || revocationReplay.SecretAvailable || revocationReplay.CredentialID != issued.CredentialID || revocationReplay.Generation != 5 || revocationReplay.OperationGeneration != 4 || revocationReplay.Status != "revoked" {
		t.Fatal("revocation replay did not return original operation and current family metadata")
	}
	if after := coauthorCredentialState(t, r); !reflect.DeepEqual(after, before) {
		t.Fatal("retained operation replay changed durable state")
	}
}

func TestCredentialCoauthorExpiredIssuanceReplayRemainsMetadataOnly(t *testing.T) {
	r := newCredentialTestRig(t)
	request := coauthorCredentialRequest(r, "expires-before-retry", 1)
	request.TTLSeconds = credentialInt(2)
	issued, err := r.exposure.IssueMCPCredential(t.Context(), request, r.admin)
	if err != nil {
		t.Fatal("expiring issuance unavailable")
	}
	r.now = r.now.Add(2 * time.Second)
	before := coauthorCredentialState(t, r)
	replay, err := r.exposure.IssueMCPCredential(t.Context(), request, r.admin)
	if err != nil {
		t.Fatal("expired issuance receipt unavailable")
	}
	coauthorAssertIssueReplay(t, issued, replay, 2, "expired")
	r.authenticates(t, issued.Secret, false)
	if after := coauthorCredentialState(t, r); !reflect.DeepEqual(after, before) {
		t.Fatal("expiry replay revived or replaced the credential")
	}
}

func TestCredentialCoauthorReplayRejectsChangedActorRequestAndOperation(t *testing.T) {
	r := newCredentialTestRig(t)
	request := coauthorCredentialRequest(r, "bound-receipt", 1)
	issued, err := r.exposure.IssueMCPCredential(t.Context(), request, r.admin)
	if err != nil {
		t.Fatal("receipt fixture unavailable")
	}
	before := coauthorCredentialState(t, r)
	for _, field := range []string{"actor", "generation", "overlap", "ttl", "credential"} {
		t.Run(field, func(t *testing.T) {
			changed := request
			admin := r.admin
			switch field {
			case "actor":
				admin.Actor = "operator:other"
			case "generation":
				changed.ExpectedGeneration = 2
			case "overlap":
				changed.OverlapSeconds = credentialInt(0)
			case "ttl":
				changed.TTLSeconds = credentialInt(60)
			case "credential":
				changed.CredentialID = issued.CredentialID
			}
			result, issueErr := r.exposure.IssueMCPCredential(t.Context(), changed, admin)
			if !errors.Is(issueErr, hoststate.ErrConflict) || !reflect.DeepEqual(result, hoststate.CredentialIssue{}) {
				t.Fatal("changed receipt binding was admitted")
			}
		})
	}
	metadata, err := r.exposure.RevokeMCPCredential(t.Context(), hoststate.RevokeCredentialRequest{
		PrincipalID: r.principal.ID, CredentialID: issued.CredentialID,
		ExpectedGeneration: 2, IdempotencyKey: request.IdempotencyKey,
	}, r.admin)
	if !errors.Is(err, hoststate.ErrConflict) || !reflect.DeepEqual(metadata, hoststate.CredentialMetadata{}) {
		t.Fatal("issuance key was reused for revocation")
	}
	replay, err := r.exposure.IssueMCPCredential(t.Context(), request, r.admin)
	if err != nil {
		t.Fatal("refused mismatches damaged original receipt")
	}
	coauthorAssertIssueReplay(t, issued, replay, 2, "active")
	if after := coauthorCredentialState(t, r); !reflect.DeepEqual(after, before) {
		t.Fatal("receipt mismatch mutated the family")
	}
}

func TestCredentialCoauthorRechecksAuthorizationInsideTransactionBeforeReplay(t *testing.T) {
	r := newCredentialTestRig(t)
	probe, _ := coauthorCredentialPeer(t, r)
	if _, err := probe.DB().ExecContext(t.Context(), `PRAGMA busy_timeout=0`); err != nil {
		t.Fatal("transaction witness unavailable")
	}
	var allowed atomic.Bool
	var checks atomic.Int32
	var heldWriter atomic.Int32
	allowed.Store(true)
	admin := r.admin
	admin.Recheck = func(ctx context.Context) error {
		checks.Add(1)
		_, err := probe.DB().ExecContext(ctx, `BEGIN IMMEDIATE`)
		if err == nil {
			_, _ = probe.DB().ExecContext(ctx, `ROLLBACK`)
			return errors.New("authorization ran outside the writer transaction")
		}
		var sqliteError *sqlite.Error
		if !errors.As(err, &sqliteError) || sqliteError.Code()&0xff != sqlite3.SQLITE_BUSY {
			return errors.New("writer transaction witness unavailable")
		}
		heldWriter.Add(1)
		if !allowed.Load() {
			return errors.New("synthetic administrator revoked")
		}
		return nil
	}
	request := coauthorCredentialRequest(r, "fresh-admin", 1)
	issued, err := r.exposure.IssueMCPCredential(t.Context(), request, admin)
	if err != nil || issued.Secret == "" || !issued.SecretAvailable {
		t.Fatal("authorized transaction-bound issuance unavailable")
	}
	revokeRequest := hoststate.RevokeCredentialRequest{
		PrincipalID: r.principal.ID, CredentialID: issued.CredentialID,
		ExpectedGeneration: 2, IdempotencyKey: "fresh-admin-revoke",
	}
	if _, err = r.exposure.RevokeMCPCredential(t.Context(), revokeRequest, admin); err != nil {
		t.Fatal("authorized transaction-bound revocation unavailable")
	}
	before := coauthorCredentialState(t, r)
	allowed.Store(false)
	replay, err := r.exposure.IssueMCPCredential(t.Context(), request, admin)
	if err == nil || !reflect.DeepEqual(replay, hoststate.CredentialIssue{}) {
		t.Fatal("revoked administrator received an issuance replay")
	}
	if metadata, revokeErr := r.exposure.RevokeMCPCredential(t.Context(), revokeRequest, admin); revokeErr == nil || !reflect.DeepEqual(metadata, hoststate.CredentialMetadata{}) {
		t.Fatal("revoked administrator received a revocation replay")
	}
	if _, err := r.exposure.ListMCPCredentials(t.Context(), r.principal.ID, admin); err == nil {
		t.Fatal("revoked administrator received retained credential metadata")
	}
	if checks.Load() != 5 || heldWriter.Load() != 5 {
		t.Fatal("administrator was not freshly checked under the writer reservation for every operation")
	}
	if after := coauthorCredentialState(t, r); !reflect.DeepEqual(after, before) {
		t.Fatal("authorization refusal changed credentials or receipts")
	}
}
