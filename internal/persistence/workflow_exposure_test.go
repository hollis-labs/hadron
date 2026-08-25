package persistence

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestWorkflowExposureStoreRoundTripReopenAndNoRawToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exposure.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	exposure, err := NewWorkflowExposureStore(store)
	if err != nil {
		t.Fatal(err)
	}
	exposure.now = workflowTestTime

	profile, err := exposure.PutExposureProfile(t.Context(), exposureTestProfile(), 0)
	if err != nil {
		t.Fatal(err)
	}
	token := "not-persisted-mcp-token"
	digest, err := hoststate.DigestMCPToken(token)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := exposure.PutMCPPrincipal(t.Context(), exposureTestPrincipal(digest), 0)
	if err != nil {
		t.Fatal(err)
	}
	if principal.Generation != 1 || principal.Record.CredentialDigest != digest {
		t.Fatalf("principal = %#v", principal)
	}
	var rawRows string
	if queryErr := store.DB().QueryRow(`SELECT record_json || credential_digest FROM workflow_mcp_principals WHERE principal_id = ?`, principal.Record.ID).Scan(&rawRows); queryErr != nil {
		t.Fatal(queryErr)
	}
	if strings.Contains(rawRows, token) {
		t.Fatal("raw credential entered durable storage")
	}
	listed, err := exposure.ListMCPPrincipals(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Record.CredentialDigest != "" {
		t.Fatalf("public principal projection = %#v", listed)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedExposure, err := NewWorkflowExposureStore(reopened)
	if err != nil {
		t.Fatal(err)
	}
	gotProfile, err := reopenedExposure.GetExposureProfile(t.Context(), profile.Record.ID)
	if err != nil || gotProfile.Generation != 1 {
		t.Fatalf("reopened profile = %#v, %v", gotProfile, err)
	}
	gotPrincipal, err := reopenedExposure.ResolveMCPPrincipalDigest(t.Context(), digest)
	if err != nil || gotPrincipal.Record.ID != principal.Record.ID {
		t.Fatalf("reopened principal = %#v, %v", gotPrincipal, err)
	}
	wrong, err := hoststate.DigestMCPToken("wrong-token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopenedExposure.ResolveMCPPrincipalDigest(t.Context(), wrong); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("wrong token resolution = %v", err)
	}
}

func TestWorkflowExposureStoreCASAndAssignedProfileGuardAcrossHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exposure.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	left, err := NewWorkflowExposureStore(first)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewWorkflowExposureStore(second)
	if err != nil {
		t.Fatal(err)
	}
	if _, putErr := left.PutExposureProfile(t.Context(), exposureTestProfile(), 0); putErr != nil {
		t.Fatal(putErr)
	}

	var wait sync.WaitGroup
	start := make(chan struct{})
	errorsCh := make(chan error, 2)
	for _, store := range []*WorkflowExposureStore{left, right} {
		wait.Add(1)
		go func(store *WorkflowExposureStore) {
			defer wait.Done()
			<-start
			record := exposureTestProfile()
			record.LazyLoad = true
			_, putErr := store.PutExposureProfile(context.Background(), record, 1)
			errorsCh <- putErr
		}(store)
	}
	close(start)
	wait.Wait()
	close(errorsCh)
	successes, conflicts := 0, 0
	for err := range errorsCh {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, hoststate.ErrConflict):
			conflicts++
		default:
			t.Fatalf("concurrent update error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}

	digest, err := hoststate.DigestMCPToken("assigned")
	if err != nil {
		t.Fatal(err)
	}
	if _, putErr := left.PutMCPPrincipal(t.Context(), exposureTestPrincipal(digest), 0); putErr != nil {
		t.Fatal(putErr)
	}
	current, err := left.GetExposureProfile(t.Context(), "profile:operators")
	if err != nil {
		t.Fatal(err)
	}
	if err := right.DeleteExposureProfile(t.Context(), current.Record.ID, current.Generation); !errors.Is(err, hoststate.ErrConflict) {
		t.Fatalf("delete assigned profile = %v", err)
	}
}

func TestWorkflowExposureStoreRejectsMissingProfileAndDuplicateCredential(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "exposure.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	exposure, err := NewWorkflowExposureStore(store)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := hoststate.DigestMCPToken("duplicate")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := exposure.PutMCPPrincipal(t.Context(), exposureTestPrincipal(digest), 0); !errors.Is(err, hoststate.ErrInvalidRecord) {
		t.Fatalf("missing profile = %v", err)
	}
	if _, err := exposure.PutExposureProfile(t.Context(), exposureTestProfile(), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := exposure.PutMCPPrincipal(t.Context(), exposureTestPrincipal(digest), 0); err != nil {
		t.Fatal(err)
	}
	other := exposureTestPrincipal(digest)
	other.ID = "user:other"
	other.Identity.Principal = other.ID
	if _, err := exposure.PutMCPPrincipal(t.Context(), other, 0); !errors.Is(err, hoststate.ErrConflict) {
		t.Fatalf("duplicate credential = %v", err)
	}
}

func exposureTestProfile() hoststate.ExposureProfileRecord {
	return hoststate.ExposureProfileRecord{
		ID:             "profile:operators",
		Namespaces:     []string{"team/ops"},
		Pins:           []graph.DefinitionRef{{Kind: "registry", ID: "team/ops/deploy", Version: "v1", Digest: values.SHA256Digest([]byte("deploy"))}},
		DeniedEffects:  graph.EffectSet{graph.EffectDestructive},
		MaxDirectTools: 12,
		SearchScope:    hoststate.ExposureSearchNamespaces,
		LazyLoad:       false,
	}
}

func exposureTestPrincipal(digest string) hoststate.MCPPrincipalRecord {
	id := "user:mcp-operator"
	return hoststate.MCPPrincipalRecord{
		ID: id, ProfileID: "profile:operators", CredentialDigest: digest,
		Identity: hoststate.IdentityBinding{
			Principal: id, SourceAuthority: "mcp", Trust: "local", Grants: []string{"workflow.run"},
			RunScope: hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeUser, ID: id},
		},
	}
}
