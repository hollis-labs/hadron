package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowruntime "github.com/hollis-labs/libs/workflow/runtime"
)

type credentialTestRig struct {
	store     *Store
	exposure  *WorkflowExposureStore
	principal hoststate.MCPPrincipalRecord
	initial   hoststate.CredentialMetadata
	admin     hoststate.CredentialAdministrator
	now       time.Time
	path      string
}

func newCredentialTestRig(t *testing.T) *credentialTestRig {
	t.Helper()
	r := &credentialTestRig{now: workflowTestTime(), path: filepath.Join(t.TempDir(), "rotation.db")}
	var err error
	r.store, err = Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.store.Close() })
	r.exposure, err = NewWorkflowExposureStore(r.store)
	if err != nil {
		t.Fatal(err)
	}
	r.exposure.now = func() time.Time { return r.now }
	if _, err = r.exposure.PutExposureProfile(t.Context(), exposureTestProfile(), 0); err != nil {
		t.Fatal(err)
	}
	digest, err := hoststate.DigestMCPToken("synthetic-initial-credential")
	if err != nil {
		t.Fatal(err)
	}
	r.principal = exposureTestPrincipal(digest)
	// Empty grants must remain empty, never defaulted by rotation.
	r.principal.Identity.Grants = []string{}
	if _, err = r.exposure.PutMCPPrincipal(t.Context(), r.principal, 0); err != nil {
		t.Fatal(err)
	}
	baseline, err := r.exposure.GetMCPPrincipal(t.Context(), r.principal.ID)
	if err != nil {
		t.Fatal(err)
	}
	r.principal = baseline.Record
	r.admin = hoststate.CredentialAdministrator{Actor: "operator:local", Source: "owned-test", Recheck: func(context.Context) error { return nil }}
	list, err := r.exposure.ListMCPCredentials(t.Context(), r.principal.ID, r.admin)
	if err != nil || len(list.Credentials) != 1 {
		t.Fatal("initial credential unavailable")
	}
	r.initial = list.Credentials[0]
	return r
}

func (r *credentialTestRig) issue(t *testing.T, key string, generation uint64, overlap, ttl *int64) hoststate.CredentialIssue {
	t.Helper()
	result, err := r.exposure.IssueMCPCredential(t.Context(), hoststate.IssueCredentialRequest{PrincipalID: r.principal.ID, CredentialID: r.initial.CredentialID, ExpectedGeneration: generation, IdempotencyKey: key, OverlapSeconds: overlap, TTLSeconds: ttl}, r.admin)
	if err != nil {
		t.Fatal(err)
	}
	if !result.SecretAvailable || result.Secret == "" || result.Replayed {
		t.Fatal("first private issuance unavailable")
	}
	return result
}

func (r *credentialTestRig) authenticates(t *testing.T, token string, want bool) {
	t.Helper()
	digest, err := hoststate.DigestMCPToken(token)
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.exposure.ResolveMCPPrincipalDigest(t.Context(), digest)
	if want && err != nil || !want && !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatal("unexpected credential validation result")
	}
}

func TestCredentialOverlapCapsAllPredecessorsWithoutExtendingExpiryOrGrants(t *testing.T) {
	r := newCredentialTestRig(t)
	shortTTL := int64(30)
	first := r.issue(t, "first", 1, nil, &shortTTL)
	r.authenticates(t, "synthetic-initial-credential", true)
	r.authenticates(t, first.Secret, true)
	longOverlap := int64(86400)
	second := r.issue(t, "second", 2, &longOverlap, nil)
	current, err := r.exposure.GetMCPPrincipal(t.Context(), r.principal.ID)
	if err != nil || !reflect.DeepEqual(current.Record, r.principal) || current.Generation != 3 {
		t.Fatal("rotation changed principal authority")
	}
	list, err := r.exposure.ListMCPCredentials(t.Context(), r.principal.ID, r.admin)
	if err != nil || len(list.Credentials) != 3 {
		t.Fatal("retained credentials missing")
	}
	for _, c := range list.Credentials {
		if c.CredentialID == r.initial.CredentialID && (c.ExpiresAt == nil || !c.ExpiresAt.Equal(r.now.Add(900*time.Second))) {
			t.Fatal("initial overlap was extended")
		}
		if c.CredentialID == first.CredentialID && (c.ExpiresAt == nil || !c.ExpiresAt.Equal(r.now.Add(30*time.Second)) || c.LastUsedAt == nil) {
			t.Fatal("earlier expiry or last-used observation lost")
		}
	}
	r.now = r.now.Add(30 * time.Second)
	r.authenticates(t, first.Secret, false)
	r.authenticates(t, second.Secret, true)
	zero := int64(0)
	third := r.issue(t, "zero-overlap", 3, &zero, nil)
	r.authenticates(t, "synthetic-initial-credential", false)
	r.authenticates(t, second.Secret, false)
	r.authenticates(t, third.Secret, true)
	list, err = r.exposure.ListMCPCredentials(t.Context(), r.principal.ID, r.admin)
	if err != nil || list.Generation != 4 {
		t.Fatal("usage advanced generation")
	}
	for _, c := range list.Credentials {
		if c.CredentialID == first.CredentialID && c.Status != "expired" {
			t.Fatal("expired predecessor revived")
		}
	}
	encoded, _ := json.Marshal(third.CredentialMetadata)
	if strings.Contains(string(encoded), third.Secret) {
		t.Fatal("secret entered metadata")
	}
	for _, query := range []string{`SELECT credential_digest FROM workflow_mcp_credentials`, `SELECT actor||source||operation FROM workflow_credential_audit`, `SELECT key_digest||request_digest||actor FROM workflow_credential_operations`, `SELECT record_json FROM workflow_mcp_principals`} {
		rows, err := r.store.DB().Query(query)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var text string
			if err := rows.Scan(&text); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(text, first.Secret) || strings.Contains(text, second.Secret) || strings.Contains(text, third.Secret) || strings.Contains(text, "synthetic-initial-credential") {
				t.Fatal("plaintext entered durable storage")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
	}
}

func TestCredentialBoundsFamilyMismatchAndLegacyCredentialBypassRefuse(t *testing.T) {
	r := newCredentialTestRig(t)
	for _, test := range []struct {
		overlap, ttl *int64
		generation   uint64
	}{{credentialInt(-1), nil, 1}, {credentialInt(86401), nil, 1}, {nil, credentialInt(0), 1}, {nil, credentialInt(31536001), 1}, {nil, nil, 0}} {
		_, err := r.exposure.IssueMCPCredential(t.Context(), hoststate.IssueCredentialRequest{PrincipalID: r.principal.ID, CredentialID: r.initial.CredentialID, ExpectedGeneration: test.generation, IdempotencyKey: "invalid", OverlapSeconds: test.overlap, TTLSeconds: test.ttl}, r.admin)
		if !errors.Is(err, hoststate.ErrInvalidRecord) {
			t.Fatal("invalid bound admitted")
		}
	}
	_, err := r.exposure.IssueMCPCredential(t.Context(), hoststate.IssueCredentialRequest{PrincipalID: "other-principal", CredentialID: r.initial.CredentialID, ExpectedGeneration: 1, IdempotencyKey: "foreign"}, r.admin)
	if !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatal("foreign credential admitted")
	}
	changed := r.principal.Clone()
	changed.CredentialDigest, _ = hoststate.DigestMCPToken("synthetic-bypass")
	if _, err = r.exposure.PutMCPPrincipal(t.Context(), changed, 1); !errors.Is(err, hoststate.ErrConflict) {
		t.Fatal("legacy setter bypassed issuer")
	}
	list, err := r.exposure.ListMCPCredentials(t.Context(), r.principal.ID, r.admin)
	if err != nil || list.Generation != 1 || len(list.Credentials) != 1 {
		t.Fatal("refusal changed family")
	}
}

func credentialInt(v int64) *int64 { return &v }

func TestCredentialClockRegressionCannotInvalidatePrincipalAuthority(t *testing.T) {
	r := newCredentialTestRig(t)
	before, err := r.exposure.GetMCPPrincipal(t.Context(), r.principal.ID)
	if err != nil {
		t.Fatal(err)
	}
	r.now = r.now.Add(-time.Hour)
	issued := r.issue(t, "backwards-clock", 1, nil, nil)
	after, err := r.exposure.GetMCPPrincipal(t.Context(), r.principal.ID)
	if err != nil || after.Validate() != nil || !after.UpdatedAt.Equal(before.UpdatedAt) || !reflect.DeepEqual(after.Record, before.Record) {
		t.Fatal("wall clock regression invalidated retained principal")
	}
	r.authenticates(t, issued.Secret, true)
	if _, revokeErr := r.exposure.RevokeMCPCredential(t.Context(), hoststate.RevokeCredentialRequest{PrincipalID: r.principal.ID, CredentialID: issued.CredentialID, ExpectedGeneration: 2, IdempotencyKey: "backwards-revoke"}, r.admin); revokeErr != nil {
		t.Fatal(revokeErr)
	}
	r.authenticates(t, issued.Secret, false)
	if _, err = r.exposure.GetMCPPrincipal(t.Context(), r.principal.ID); err != nil {
		t.Fatal("revocation invalidated principal")
	}
}

func TestCredentialMigrationBackfillsLegacyDigestAndReopenRetainsRevocation(t *testing.T) {
	r := newCredentialTestRig(t)
	// Reconstruct the real pre-0031 shape around an already-persisted principal.
	for _, query := range []string{`DROP TABLE workflow_credential_audit`, `DROP TABLE workflow_credential_operations`, `DROP TABLE workflow_mcp_credentials`, `DELETE FROM schema_migrations WHERE version=31`} {
		if _, err := r.store.DB().Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if closeErr := r.store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	var err error
	r.store, err = Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	r.exposure, err = NewWorkflowExposureStore(r.store)
	if err != nil {
		t.Fatal(err)
	}
	r.exposure.now = func() time.Time { return r.now }
	list, err := r.exposure.ListMCPCredentials(t.Context(), r.principal.ID, r.admin)
	if err != nil || list.Generation != 1 || len(list.Credentials) != 1 {
		t.Fatal("legacy migration failed")
	}
	r.initial = list.Credentials[0]
	r.authenticates(t, "synthetic-initial-credential", true)
	issued := r.issue(t, "after-migration", 1, nil, nil)
	if _, revokeErr := r.exposure.RevokeMCPCredential(t.Context(), hoststate.RevokeCredentialRequest{PrincipalID: r.principal.ID, CredentialID: r.initial.CredentialID, ExpectedGeneration: 2, IdempotencyKey: "revoke-old"}, r.admin); revokeErr != nil {
		t.Fatal(revokeErr)
	}
	if closeErr := r.store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	r.store, err = Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	r.exposure, err = NewWorkflowExposureStore(r.store)
	if err != nil {
		t.Fatal(err)
	}
	r.exposure.now = func() time.Time { return r.now }
	r.authenticates(t, "synthetic-initial-credential", false)
	r.authenticates(t, issued.Secret, true)
}
