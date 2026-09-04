package appworkflow

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

type a2aTestIdentityKey struct{}

type a2aTestIdentityProvider struct{}

func (a2aTestIdentityProvider) BindIdentity(ctx context.Context, _ IdentityRequest) (hoststate.IdentityBinding, error) {
	binding, ok := ctx.Value(a2aTestIdentityKey{}).(hoststate.IdentityBinding)
	if !ok {
		return hoststate.IdentityBinding{}, ErrWorkflowUnauthenticated
	}
	return binding.Clone(), nil
}

type a2aTestCorrelationStore struct {
	mu      sync.Mutex
	records map[string]hoststate.A2ATaskCorrelation
	puts    int
}

func (s *a2aTestCorrelationStore) PutA2ATaskCorrelation(_ context.Context, input hoststate.A2ATaskCorrelation) (hoststate.A2ATaskCorrelation, workflowruntime.IdempotencyOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	prior, ok := s.records[input.TaskID]
	if ok {
		if !sameA2ATestIntent(prior, input) {
			return hoststate.A2ATaskCorrelation{}, "", workflowruntime.ErrIdempotencyConflict
		}
		return prior.Clone(), workflowruntime.IdempotencyReplayed, nil
	}
	s.records[input.TaskID] = input.Clone()
	return input.Clone(), workflowruntime.IdempotencyApplied, nil
}

func (s *a2aTestCorrelationStore) GetA2ATaskCorrelation(_ context.Context, taskID string) (hoststate.A2ATaskCorrelation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[taskID]
	if !ok {
		return hoststate.A2ATaskCorrelation{}, workflowruntime.ErrNotFound
	}
	return record.Clone(), nil
}

func TestA2ATaskCorrelationsAuthorizeBeforeMutationAndHideCrossOwner(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	store := &a2aTestCorrelationStore{records: make(map[string]hoststate.A2ATaskCorrelation)}
	host := &Host{identity: a2aTestIdentityProvider{}, clock: ClockFunc(func() time.Time { return now })}
	service, err := NewA2ATaskCorrelations(A2ATaskCorrelationsOptions{
		Host: host, Store: store,
		Access: A2ATaskOperationAuthorizerFunc(func(_ context.Context, request A2ATaskAuthorization) error {
			if request.Caller.Principal == "principal:denied" {
				return ErrWorkflowHidden
			}
			if !sameIdentity(request.Caller, request.Owner) {
				return ErrWorkflowHidden
			}
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	correlation := a2aTestCorrelation()
	denied := context.WithValue(t.Context(), a2aTestIdentityKey{}, a2aTestIdentity("principal:denied", "scope-one", "target-one", now))
	if _, _, putErr := service.Put(denied, IdentityRequest{SourceAuthority: "a2a"}, correlation); !errors.Is(putErr, ErrWorkflowHidden) {
		t.Fatalf("denied put error = %v", putErr)
	}
	if store.puts != 0 || len(store.records) != 0 {
		t.Fatalf("denied put mutated store: puts=%d records=%#v", store.puts, store.records)
	}

	owner := context.WithValue(t.Context(), a2aTestIdentityKey{}, a2aTestIdentity("principal:owner", "scope-one", "target-one", now))
	selected := IdentityRequest{
		SourceAuthority: "a2a",
		RunScope:        &hoststate.RunScopeSelector{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeUser, ID: "scope-one"},
		ExecutionTarget: &hoststate.ExecutionTargetSelector{Version: hoststate.ScopeTargetVersionV1, ID: "target-one"},
	}
	stored, outcome, err := service.Put(owner, selected, correlation)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || stored.Owner.Principal != "principal:owner" {
		t.Fatalf("owner put = %#v, %q, %v", stored, outcome, err)
	}
	if _, replay, replayErr := service.Put(owner, selected, correlation); replayErr != nil || replay != workflowruntime.IdempotencyReplayed {
		t.Fatalf("owner replay = %q, %v", replay, replayErr)
	}
	if resolved, getErr := service.Get(owner, IdentityRequest{SourceAuthority: "a2a"}, correlation.TaskID, A2ATaskInspect); getErr != nil || resolved.Owner.ExecutionTarget == nil || resolved.Owner.ExecutionTarget.ID != "target-one" {
		t.Fatalf("selected owner get = %#v, %v", resolved, getErr)
	}

	for name, binding := range map[string]hoststate.IdentityBinding{
		"principal": a2aTestIdentity("principal:other", "scope-one", "target-one", now),
		"scope":     a2aTestIdentity("principal:owner", "scope-two", "target-one", now),
		"target":    a2aTestIdentity("principal:owner", "scope-one", "target-two", now),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.WithValue(t.Context(), a2aTestIdentityKey{}, binding)
			changed := correlation
			changed.IdempotencyKey = "different"
			_, _, putErr := service.Put(ctx, IdentityRequest{SourceAuthority: "a2a"}, changed)
			_, getErr := service.Get(ctx, IdentityRequest{SourceAuthority: "a2a"}, correlation.TaskID, A2ATaskInspect)
			if !errors.Is(putErr, ErrWorkflowHidden) || !errors.Is(getErr, ErrWorkflowHidden) {
				t.Fatalf("cross-owner put/get = %v / %v", putErr, getErr)
			}
			missing := SafeWorkflowOperationError(workflowruntime.ErrNotFound, nil)
			hidden := SafeWorkflowOperationError(getErr, nil)
			if !reflect.DeepEqual(missing, hidden) {
				t.Fatalf("hidden envelope %#v differs from missing %#v", hidden, missing)
			}
		})
	}

	responder := context.WithValue(t.Context(), a2aTestIdentityKey{}, a2aTestIdentity("principal:approver", "approval", "target-two", now))
	if resolved, resumeErr := service.Get(responder, IdentityRequest{SourceAuthority: "a2a"}, correlation.TaskID, A2ATaskResume); resumeErr != nil || resolved.RunID != stored.RunID {
		t.Fatalf("delegated responder correlation = %#v, %v", resolved, resumeErr)
	}
}

func TestA2ATaskCorrelationsDeriveOwnerScopedRestartStableIdentity(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "a2a-generated.db")
	firstDB, err := persistence.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	secondDB, err := persistence.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	firstStore, err := persistence.NewWorkflowA2ATaskStore(firstDB)
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := persistence.NewWorkflowA2ATaskStore(secondDB)
	if err != nil {
		t.Fatal(err)
	}
	host := &Host{identity: a2aTestIdentityProvider{}, clock: ClockFunc(func() time.Time { return now })}
	firstService, err := NewA2ATaskCorrelations(A2ATaskCorrelationsOptions{Host: host, Store: firstStore})
	if err != nil {
		t.Fatal(err)
	}
	secondService, err := NewA2ATaskCorrelations(A2ATaskCorrelationsOptions{Host: host, Store: secondStore})
	if err != nil {
		t.Fatal(err)
	}
	owner := context.WithValue(t.Context(), a2aTestIdentityKey{}, a2aTestIdentity("principal:owner", "scope-one", "target-one", now))
	intent := a2aTestCorrelation()
	intent.TaskID = ""

	type result struct {
		correlation hoststate.A2ATaskCorrelation
		outcome     workflowruntime.IdempotencyOutcome
		err         error
	}
	results := make(chan result, 2)
	for _, service := range []*A2ATaskCorrelations{firstService, secondService} {
		go func(current *A2ATaskCorrelations) {
			correlation, outcome, putErr := current.Put(owner, IdentityRequest{SourceAuthority: "a2a"}, intent)
			results <- result{correlation: correlation, outcome: outcome, err: putErr}
		}(service)
	}
	first, second := <-results, <-results
	for _, current := range []result{first, second} {
		if current.err != nil {
			t.Fatal(current.err)
		}
	}
	if first.correlation.TaskID == "" || first.correlation.TaskID != second.correlation.TaskID || first.correlation.RunID == "" || first.correlation.RunID != second.correlation.RunID || first.correlation.HostStartKey == "" || first.correlation.HostStartKey != second.correlation.HostStartKey {
		t.Fatalf("restart identities = %#v / %#v", first.correlation, second.correlation)
	}
	outcomes := map[workflowruntime.IdempotencyOutcome]int{first.outcome: 1}
	outcomes[second.outcome]++
	if outcomes[workflowruntime.IdempotencyApplied] != 1 || outcomes[workflowruntime.IdempotencyReplayed] != 1 {
		t.Fatalf("concurrent restart outcomes = %#v", outcomes)
	}
	if closeErr := firstDB.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if closeErr := secondDB.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopenedDB, err := persistence.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedDB.Close() })
	reopenedStore, err := persistence.NewWorkflowA2ATaskStore(reopenedDB)
	if err != nil {
		t.Fatal(err)
	}
	reopenedHost := &Host{identity: a2aTestIdentityProvider{}, clock: ClockFunc(func() time.Time { return now.Add(time.Hour) })}
	reopenedService, err := NewA2ATaskCorrelations(A2ATaskCorrelationsOptions{Host: reopenedHost, Store: reopenedStore})
	if err != nil {
		t.Fatal(err)
	}
	replayed, outcome, replayErr := reopenedService.Put(owner, IdentityRequest{SourceAuthority: "a2a"}, intent)
	if replayErr != nil || outcome != workflowruntime.IdempotencyReplayed || replayed.TaskID != first.correlation.TaskID || replayed.RunID != first.correlation.RunID {
		t.Fatalf("reopened generated replay = %#v, %q, %v", replayed, outcome, replayErr)
	}

	changed := intent
	changed.RequestDigest = values.SHA256Digest([]byte("changed request"))
	if _, _, conflictErr := reopenedService.Put(owner, IdentityRequest{SourceAuthority: "a2a"}, changed); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed generated-id intent error = %v", conflictErr)
	}
	seenTasks := map[string]struct{}{first.correlation.TaskID: {}}
	for name, binding := range map[string]hoststate.IdentityBinding{
		"principal": a2aTestIdentity("principal:other", "scope-one", "target-one", now),
		"scope":     a2aTestIdentity("principal:owner", "scope-two", "target-one", now),
		"target":    a2aTestIdentity("principal:owner", "scope-one", "target-two", now),
	} {
		otherOwner := context.WithValue(t.Context(), a2aTestIdentityKey{}, binding)
		other, outcome, putErr := reopenedService.Put(otherOwner, IdentityRequest{SourceAuthority: "a2a"}, intent)
		_, collision := seenTasks[other.TaskID]
		if putErr != nil || outcome != workflowruntime.IdempotencyApplied || collision || other.RunID == first.correlation.RunID || other.HostStartKey == first.correlation.HostStartKey {
			t.Fatalf("%s-scoped generated identity = %#v, %q, %v", name, other, outcome, putErr)
		}
		seenTasks[other.TaskID] = struct{}{}
	}
}

type unresolvedA2AConflictStore struct{}

func (*unresolvedA2AConflictStore) PutA2ATaskCorrelation(context.Context, hoststate.A2ATaskCorrelation) (hoststate.A2ATaskCorrelation, workflowruntime.IdempotencyOutcome, error) {
	return hoststate.A2ATaskCorrelation{}, "", workflowruntime.ErrIdempotencyConflict
}

func (*unresolvedA2AConflictStore) GetA2ATaskCorrelation(context.Context, string) (hoststate.A2ATaskCorrelation, error) {
	return hoststate.A2ATaskCorrelation{}, workflowruntime.ErrNotFound
}

func TestA2ATaskCorrelationUnresolvedInsertConflictDoesNotExposeIdempotency(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	host := &Host{identity: a2aTestIdentityProvider{}, clock: ClockFunc(func() time.Time { return now })}
	service, err := NewA2ATaskCorrelations(A2ATaskCorrelationsOptions{Host: host, Store: &unresolvedA2AConflictStore{}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(t.Context(), a2aTestIdentityKey{}, a2aTestIdentity("principal:owner", "scope-one", "target-one", now))
	_, _, putErr := service.Put(ctx, IdentityRequest{SourceAuthority: "a2a"}, a2aTestCorrelation())
	if errors.Is(putErr, workflowruntime.ErrIdempotencyConflict) || !errors.Is(putErr, ErrHostNotReady) {
		t.Fatalf("unresolved conflict error = %v", putErr)
	}
	if safe := SafeWorkflowOperationError(putErr, nil); safe.Code != WorkflowErrorCodeUnavailable {
		t.Fatalf("safe unresolved conflict = %#v", safe)
	}
}

func a2aTestCorrelation() hoststate.A2ATaskCorrelation {
	return hoststate.A2ATaskCorrelation{
		TaskID:        "task-one",
		Definition:    graph.DefinitionRef{Kind: "registry", ID: "team/workflow", Version: "v1", Digest: values.SHA256Digest([]byte("workflow"))},
		RequestDigest: values.SHA256Digest([]byte("request")), IdempotencyKey: "start-one",
	}
}

func a2aTestIdentity(principal, scopeID, targetID string, now time.Time) hoststate.IdentityBinding {
	return hoststate.IdentityBinding{
		Principal: principal, SourceAuthority: "a2a", Trust: "local", Grants: []string{"workflow.run"},
		RunScope: hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeUser, ID: scopeID},
		ExecutionTarget: &hoststate.ExecutionTarget{
			Version: hoststate.ScopeTargetVersionV1, ID: targetID, Kind: hoststate.ExecutionTargetLocal,
			Capabilities: []string{}, Sandbox: hoststate.SandboxPolicy{Mode: hoststate.SandboxHostDefault},
			Readiness:  hoststate.TargetReadiness{State: hoststate.TargetReady, CheckedAt: now},
			Provenance: hoststate.TargetProvenance{Authority: "scheduler", Reference: "local/default"},
		},
	}
}

func sameA2ATestIntent(left, right hoststate.A2ATaskCorrelation) bool {
	left.CreatedAt, right.CreatedAt = time.Time{}, time.Time{}
	return reflect.DeepEqual(left, right)
}
