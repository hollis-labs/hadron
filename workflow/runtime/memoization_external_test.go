package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestMemoizedSafeNodeSkipsExecutorAndJournalsOrigin(t *testing.T) {
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("memo-fixture", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	executions := 0
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executions++
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": memoValue(t, "result", "cached", values.RedactionPublic, values.RetentionProject)}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	node := graph.Node{ID: "node", Kind: "memo-fixture", KindVersion: "v1", Memoization: &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.input"}, MaxAge: "1h"}}
	keyDigest, _ := values.DigestInline("hello")

	store := inmemory.NewStore()
	base := time.Date(2026, time.August, 24, 15, 0, 0, 0, time.UTC)
	firstClaim := memoClaimedNode(t, store, "memo-first", keyDigest, base)
	firstDispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	first, err := firstDispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: firstClaim, Node: node, Target: "embedded"})
	if err != nil || first.Node.Origin != workflowruntime.OriginExecuted || executions != 1 || first.Outputs == nil {
		t.Fatalf("first dispatch = %#v, %v executions=%d", first, err, executions)
	}

	secondClaim := memoClaimedNode(t, store, "memo-second", keyDigest, base.Add(time.Minute))
	claimedSecond, _ := store.LoadNodeInvocation(context.Background(), secondClaim.Candidate.InvocationID)
	forgedKey := values.SHA256Digest([]byte("absent-publication"))
	sourceAttempt := first.Attempt.ID
	forged, forgedErr := store.ReuseNodeOutputs(context.Background(), workflowruntime.ReuseNodeOutputsRequest{
		InvocationID: secondClaim.Candidate.InvocationID, ExpectedGeneration: claimedSecond.Generation,
		Claim:  workflowruntime.ClaimProof{Owner: secondClaim.Lease.Owner, Token: secondClaim.Lease.Token, Generation: secondClaim.Lease.Generation},
		Origin: workflowruntime.OriginMemoized, Outputs: *first.Outputs, Source: first.Node.ID, MemoEntryKey: forgedKey, SourceAttempt: &sourceAttempt, SourceOrigin: workflowruntime.OriginExecuted,
		PlanDigest: testPlan().Digest, Policy: workflowruntime.ReusePolicyDecision{Allow: true, Code: "forged", Reason: "must not bypass publication"}, IdempotencyKey: "forged-reuse", At: base.Add(90 * time.Second),
	})
	if !errors.Is(forgedErr, workflowruntime.ErrInvalidRecord) || forged.Outcome != "" {
		t.Fatalf("forged reuse = %#v, %v", forged, forgedErr)
	}
	secondDispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(2 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	second, err := secondDispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: secondClaim, Node: node, Target: "embedded"})
	if err != nil || second.Node.Origin != workflowruntime.OriginMemoized || second.Outputs == nil || *second.Outputs != *first.Outputs || executions != 1 || second.Attempt.ID.Number != 0 {
		t.Fatalf("memo dispatch = %#v, %v executions=%d", second, err, executions)
	}
	attempts, _ := store.ListAttempts(context.Background(), second.Node.ID)
	events, _ := store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: second.Node.ID.RunID})
	reuseEvent := findEvent(events, workflowruntime.EventNodeOutcomeReused)
	if len(attempts) != 0 || reuseEvent == nil || reuseEvent.Values == nil || *reuseEvent.Values != *first.Outputs || reuseEvent.Attributes["origin"] != string(workflowruntime.OriginMemoized) || reuseEvent.Attributes["source_origin"] != string(workflowruntime.OriginExecuted) || reuseEvent.Attributes["source_attempt"] != "1" || reuseEvent.Attributes["output_digest"] != first.Outputs.Digest {
		t.Fatalf("memo outcome attempts/events = %#v/%#v", attempts, events)
	}
	expression, expressionErr := workflowruntime.BuildExpressionContext(context.Background(), store, store, graph.Graph{Nodes: []graph.Node{node}}, second.Node.ID.RunID)
	if expressionErr != nil || expression.Steps["node"].Outputs["result"].Inline != "cached" {
		t.Fatalf("downstream expression context = %#v, %v", expression.Steps["node"], expressionErr)
	}
}

func TestMemoizationExpirySchemaAuthorizationAndUnsafeEffectsAreStructured(t *testing.T) {
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("memo-policy", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	kind.SpecValue.Effects = graph.EffectSet{graph.EffectCompute}
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": memoValue(t, "result", "fresh", values.RedactionPublic, values.RetentionProject)}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	store := inmemory.NewStore()
	base := time.Date(2026, time.August, 24, 16, 0, 0, 0, time.UTC)
	keyDigest, _ := values.DigestInline("hello")
	node := graph.Node{ID: "node", Kind: "memo-policy", KindVersion: "v1", Memoization: &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.input"}, MaxAge: "1s"}}
	first, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if _, err := first.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedNode(t, store, "memo-expire-source", keyDigest, base), Node: node}); err != nil {
		t.Fatal(err)
	}
	second, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(time.Minute) }})
	result, err := second.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedNode(t, store, "memo-expire-target", keyDigest, base.Add(10*time.Second)), Node: node})
	if err != nil || result.Node.Origin != workflowruntime.OriginExecuted || !hasDiagnostic(result.Diagnostics, workflowruntime.CodeMemoExpired) {
		t.Fatalf("expired dispatch = %#v, %v", result, err)
	}

	unsafe := node
	unsafe.Effects = graph.EffectSet{graph.EffectMutate}
	unsafeResult, unsafeErr := second.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedNode(t, store, "memo-unsafe", keyDigest, base.Add(2*time.Minute)), Node: unsafe})
	if !errors.Is(unsafeErr, workflowruntime.ErrReuseDenied) || !hasDiagnostic(unsafeResult.Diagnostics, workflowruntime.CodeMemoRejected) {
		t.Fatalf("unsafe memo = %#v, %v", unsafeResult, unsafeErr)
	}
}

func TestPinCoordinatorValidatesAndDispatcherConsumesOrdinaryTypedOutputs(t *testing.T) {
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("pin-fixture", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	executions := 0
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executions++
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": memoValue(t, "result", "pinned", values.RedactionPrivate, values.RetentionProject)}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	store := inmemory.NewStore()
	base := time.Date(2026, time.August, 24, 17, 0, 0, 0, time.UTC)
	sourceClaim := memoClaimedNode(t, store, "pin-source", "", base)
	dispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	source, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: sourceClaim, Node: graph.Node{ID: "node", Kind: "pin-fixture", KindVersion: "v1"}})
	if err != nil || source.Outputs == nil {
		t.Fatalf("source dispatch = %#v, %v", source, err)
	}
	target := memoPendingNode(t, store, "pin-target", "", base.Add(time.Minute))
	ref := testPlan()
	graphPlan := graph.Graph{ID: ref.ID, Version: ref.Version, Digest: ref.Digest, Nodes: []graph.Node{{ID: "node", Kind: "pin-fixture", KindVersion: "v1"}}}
	authorizations := 0
	authorizer := workflowruntime.ReuseAuthorizerFunc(func(_ context.Context, candidate workflowruntime.ReuseCandidate) (workflowruntime.ReusePolicyDecision, error) {
		authorizations++
		if candidate.Authority.Principal != "developer" {
			return workflowruntime.ReusePolicyDecision{}, errors.New("wrong principal")
		}
		return workflowruntime.ReusePolicyDecision{Allow: true, Code: "pin_allowed", Reason: "developer selected trusted output"}, nil
	})
	coordinator := workflowruntime.PinCoordinator{Store: store, Pins: store, Values: store, Plans: staticRecoveryPlans{graph: graphPlan}, Registry: registry, Authorizer: authorizer}
	bound, err := coordinator.Bind(context.Background(), workflowruntime.PinNodeRequest{Target: target.ID, Outputs: *source.Outputs, Authority: workflowruntime.ReuseAuthority{Principal: "developer", Scope: "project"}, IdempotencyKey: "pin-bind", At: base.Add(2 * time.Minute)})
	if err != nil || bound.Outcome != workflowruntime.IdempotencyApplied || authorizations != 1 {
		t.Fatalf("Bind(pin) = %#v, %v auth=%d", bound, err, authorizations)
	}
	replayed, err := coordinator.Bind(context.Background(), workflowruntime.PinNodeRequest{Target: target.ID, Outputs: *source.Outputs, Authority: workflowruntime.ReuseAuthority{Principal: "developer", Scope: "project"}, IdempotencyKey: "pin-bind", At: base.Add(2 * time.Minute)})
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || authorizations != 2 {
		t.Fatalf("Bind(pin replay) = %#v, %v auth=%d", replayed, err, authorizations)
	}
	unsafeCoordinator := coordinator
	unsafeCoordinator.Authorizer = workflowruntime.ReuseAuthorizerFunc(func(context.Context, workflowruntime.ReuseCandidate) (workflowruntime.ReusePolicyDecision, error) {
		return workflowruntime.ReusePolicyDecision{Allow: true, Code: "pin_allowed", Reason: "Bearer supersecret"}, nil
	})
	if _, unsafeErr := unsafeCoordinator.Bind(context.Background(), workflowruntime.PinNodeRequest{Target: target.ID, Outputs: *source.Outputs, Authority: workflowruntime.ReuseAuthority{Principal: "developer", Scope: "project"}, IdempotencyKey: "pin-bind", At: base.Add(2 * time.Minute)}); unsafeErr == nil || strings.Contains(unsafeErr.Error(), "supersecret") {
		t.Fatalf("credential-shaped policy decision was accepted or leaked: %v", unsafeErr)
	}
	ready, err := store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{InvocationID: target.ID, ExpectedGeneration: bound.Node.Generation, To: workflowruntime.NodeReady, At: base.Add(3 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(context.Background(), workflowruntime.ReadyClaimRequest{Owner: "worker", Token: "pin-token", IdempotencyKey: "pin-claim", Now: base.Add(4 * time.Minute), LeaseUntil: base.Add(time.Hour)})
	if err != nil || !ok || claim.Candidate.InvocationID != ready.Snapshot.ID {
		t.Fatalf("claim pin = %#v %v %v", claim, ok, err)
	}
	claimedTarget, err := store.LoadNodeInvocation(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	forgedPolicy := bound.Binding.Policy
	forgedPolicy.Reason = "forged replacement decision"
	if _, forgedErr := store.ReuseNodeOutputs(context.Background(), workflowruntime.ReuseNodeOutputsRequest{
		InvocationID: target.ID, ExpectedGeneration: claimedTarget.Generation,
		Claim:  workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation},
		Origin: workflowruntime.OriginPinned, Outputs: *source.Outputs, Source: source.Node.ID, SourceOrigin: workflowruntime.OriginExecuted,
		PlanDigest: ref.Digest, Policy: forgedPolicy, IdempotencyKey: "pin-forged-policy", At: base.Add(5 * time.Minute),
	}); !errors.Is(forgedErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("forged pin policy = %v", forgedErr)
	}
	pinDispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(5 * time.Minute) }})
	result, err := pinDispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: "node", Kind: "pin-fixture", KindVersion: "v1"}})
	if err != nil || result.Node.Origin != workflowruntime.OriginPinned || executions != 1 || result.Outputs == nil || *result.Outputs != *source.Outputs {
		t.Fatalf("pinned dispatch = %#v, %v executions=%d", result, err, executions)
	}
}

func TestPinCoordinatorRejectsForgedValueRecordsBeforeAuthorizationOrBinding(t *testing.T) {
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("pin-integrity", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": memoValue(t, "result", "durable", values.RedactionPublic, values.RetentionProject)}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	store := inmemory.NewStore()
	base := time.Date(2026, time.August, 24, 17, 15, 0, 0, time.UTC)
	sourceClaim := memoClaimedNode(t, store, "pin-integrity-source", "", base)
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	source, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: sourceClaim, Node: graph.Node{ID: "node", Kind: "pin-integrity", KindVersion: "v1"}})
	if err != nil || source.Outputs == nil {
		t.Fatalf("source dispatch = %#v, %v", source, err)
	}
	validRecord, err := store.LoadValueRecord(context.Background(), *source.Outputs)
	if err != nil {
		t.Fatal(err)
	}
	planRef := testPlan()
	plan := graph.Graph{ID: planRef.ID, Version: planRef.Version, Digest: planRef.Digest, Nodes: []graph.Node{{ID: "node", Kind: "pin-integrity", KindVersion: "v1"}}}
	const rawSurrogate = "DO-NOT-LEAK-SURROGATE"
	tests := []struct {
		name   string
		mutate func(workflowruntime.ValueRecord) workflowruntime.ValueRecord
	}{
		{name: "mismatched reference", mutate: func(record workflowruntime.ValueRecord) workflowruntime.ValueRecord {
			record.Ref.ID = "forged-values"
			return record
		}},
		{name: "invalid owner", mutate: func(record workflowruntime.ValueRecord) workflowruntime.ValueRecord {
			record.Owner = workflowruntime.ValueOwner{}
			return record
		}},
		{name: "content digest mismatch", mutate: func(record workflowruntime.ValueRecord) workflowruntime.ValueRecord {
			record.Values = values.ValueSet{"result": memoValue(t, "result", rawSurrogate, values.RedactionPublic, values.RetentionProject)}
			return record
		}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := memoPendingNode(t, store, fmt.Sprintf("pin-integrity-target-%d", index), "", base.Add(time.Duration(index+1)*time.Minute))
			pins := &countingPinStore{PinStore: store}
			authorizations := 0
			authorizer := workflowruntime.ReuseAuthorizerFunc(func(context.Context, workflowruntime.ReuseCandidate) (workflowruntime.ReusePolicyDecision, error) {
				authorizations++
				return workflowruntime.ReusePolicyDecision{Allow: true, Code: "unexpected", Reason: "must not authorize forged records"}, nil
			})
			coordinator := workflowruntime.PinCoordinator{Store: store, Pins: pins, Values: fixedValueRecordStore{record: test.mutate(validRecord)}, Plans: staticRecoveryPlans{graph: plan}, Registry: registry, Authorizer: authorizer}
			_, bindErr := coordinator.Bind(context.Background(), workflowruntime.PinNodeRequest{Target: target.ID, Outputs: *source.Outputs, Authority: workflowruntime.ReuseAuthority{Principal: "developer"}, IdempotencyKey: fmt.Sprintf("pin-integrity-%d", index), At: base.Add(time.Hour)})
			if !errors.Is(bindErr, workflowruntime.ErrInvalidReuse) || strings.Contains(bindErr.Error(), rawSurrogate) {
				t.Fatalf("forged value record error = %v", bindErr)
			}
			if authorizations != 0 || pins.bindCalls != 0 {
				t.Fatalf("forged record reached side effects: authorizations=%d binds=%d", authorizations, pins.bindCalls)
			}
			if _, loadErr := store.LoadPin(context.Background(), target.ID); !errors.Is(loadErr, workflowruntime.ErrNotFound) {
				t.Fatalf("forged record created pin: %v", loadErr)
			}
		})
	}
}

func TestMemoizedMaterializeRequiresExecutorAndFreshHostApproval(t *testing.T) {
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("memo-materialize", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	kind.SpecValue.Effects = graph.EffectSet{graph.EffectMaterialize}
	kind.SpecValue.Memoization = stepkind.MemoizationApproved
	executions := 0
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executions++
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": memoValue(t, "result", "artifact", values.RedactionPublic, values.RetentionProject)}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	node := graph.Node{ID: "node", Kind: "memo-materialize", KindVersion: "v1", Memoization: &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.input"}, MaxAge: "1h"}}
	keyDigest, _ := values.DigestInline("hello")
	store := inmemory.NewStore()
	base := time.Date(2026, time.August, 24, 17, 30, 0, 0, time.UTC)
	authorizations := 0
	allow := workflowruntime.ReuseAuthorizerFunc(func(_ context.Context, candidate workflowruntime.ReuseCandidate) (workflowruntime.ReusePolicyDecision, error) {
		authorizations++
		if len(candidate.Effects) != 1 || candidate.Effects[0] != graph.EffectMaterialize {
			return workflowruntime.ReusePolicyDecision{}, errors.New("materialize effect absent")
		}
		return workflowruntime.ReusePolicyDecision{Allow: true, Code: "materialize_allowed", Reason: "trusted materialization is reusable"}, nil
	})
	first, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, ReuseAuthorizer: allow, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	firstResult, err := first.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedNode(t, store, "materialize-source", keyDigest, base), Node: node})
	if err != nil || firstResult.Node.Origin != workflowruntime.OriginExecuted || len(firstResult.Warnings) != 0 || authorizations != 1 {
		t.Fatalf("materialize publication = %#v, %v authorizations=%d", firstResult, err, authorizations)
	}

	denied, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(2 * time.Minute) }})
	deniedResult, err := denied.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedNode(t, store, "materialize-denied", keyDigest, base.Add(time.Minute)), Node: node})
	if err != nil || deniedResult.Node.Origin != workflowruntime.OriginExecuted || !hasDiagnostic(deniedResult.Diagnostics, workflowruntime.CodeMemoRejected) || len(deniedResult.Warnings) != 1 || executions != 2 {
		t.Fatalf("materialize denied reuse = %#v, %v executions=%d", deniedResult, err, executions)
	}

	approved, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, ReuseAuthorizer: allow, Now: func() time.Time { return base.Add(4 * time.Minute) }})
	approvedResult, err := approved.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedNode(t, store, "materialize-approved", keyDigest, base.Add(3*time.Minute)), Node: node})
	if err != nil || approvedResult.Node.Origin != workflowruntime.OriginMemoized || executions != 2 || authorizations != 2 {
		t.Fatalf("materialize approved reuse = %#v, %v executions=%d authorizations=%d", approvedResult, err, executions, authorizations)
	}
}

func TestMemoPublicationRetentionAndPersistenceFailureNeverReverseSuccess(t *testing.T) {
	t.Run("run retention", func(t *testing.T) {
		store := inmemory.NewStore()
		registry := stepkind.NewRegistry()
		kind := stepkindtest.NewNoopKind("memo-retention", "v1")
		kind.SpecValue.InputSchema = objectSchema("input", "string")
		kind.SpecValue.OutputSchema = objectSchema("result", "string")
		executions := 0
		kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
			executions++
			return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": memoValue(t, "result", "ephemeral", values.RedactionPublic, values.RetentionRun)}}, nil
		}
		if err := registry.Register(kind); err != nil {
			t.Fatal(err)
		}
		node := graph.Node{ID: "node", Kind: "memo-retention", KindVersion: "v1", Memoization: &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.input"}, MaxAge: "1h"}}
		keyDigest, _ := values.DigestInline("hello")
		base := time.Date(2026, time.August, 24, 18, 0, 0, 0, time.UTC)
		dispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
		first, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedNode(t, store, "retention-source", keyDigest, base), Node: node})
		if err != nil || first.Node.Origin != workflowruntime.OriginExecuted || len(first.Warnings) != 1 || first.Warnings[0].Stage != workflowruntime.DispatchPublishMemo {
			t.Fatalf("retention result = %#v, %v", first, err)
		}
		second, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedNode(t, store, "retention-target", keyDigest, base.Add(time.Minute)), Node: node})
		if err != nil || second.Node.Origin != workflowruntime.OriginExecuted || !hasDiagnostic(second.Diagnostics, workflowruntime.CodeMemoMiss) || executions != 2 {
			t.Fatalf("retention miss = %#v, %v executions=%d", second, err, executions)
		}
	})

	t.Run("append failure", func(t *testing.T) {
		baseStore := inmemory.NewStore()
		store := &failingMemoStore{Store: baseStore, failure: errors.New("memo journal unavailable")}
		registry := stepkind.NewRegistry()
		kind := stepkindtest.NewNoopKind("memo-failure", "v1")
		kind.SpecValue.InputSchema = objectSchema("input", "string")
		kind.SpecValue.OutputSchema = objectSchema("result", "string")
		kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
			return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": memoValue(t, "result", "durable", values.RedactionPublic, values.RetentionProject)}}, nil
		}
		if err := registry.Register(kind); err != nil {
			t.Fatal(err)
		}
		base := time.Date(2026, time.August, 24, 18, 30, 0, 0, time.UTC)
		keyDigest, _ := values.DigestInline("hello")
		node := graph.Node{ID: "node", Kind: "memo-failure", KindVersion: "v1", Memoization: &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.input"}, MaxAge: "1h"}}
		dispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
		result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedNode(t, baseStore, "memo-failure", keyDigest, base), Node: node})
		if err != nil || result.Node.Status != workflowruntime.NodeSucceeded || result.Node.Origin != workflowruntime.OriginExecuted || result.Outputs == nil || len(result.Warnings) != 1 || !errors.Is(result.Warnings[0].Cause, store.failure) {
			t.Fatalf("append failure result = %#v, %v", result, err)
		}
	})
}

func TestSchemaIncompatibleMemoEntryIsDiagnosedAndExecuted(t *testing.T) {
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("memo-schema", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	executions := 0
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executions++
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": memoValue(t, "result", "valid", values.RedactionPublic, values.RetentionProject)}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	baseStore := inmemory.NewStore()
	base := time.Date(2026, time.August, 24, 19, 0, 0, 0, time.UTC)
	keyDigest, _ := values.DigestInline("hello")
	node := graph.Node{ID: "node", Kind: "memo-schema", KindVersion: "v1", Memoization: &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.input"}, MaxAge: "1h"}}
	sourceDispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: baseStore, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	source, err := sourceDispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedNode(t, baseStore, "schema-source", keyDigest, base), Node: node})
	if err != nil || source.Outputs == nil {
		t.Fatalf("schema source = %#v, %v", source, err)
	}
	wrong := values.ValueSet{"result": memoValue(t, "result", 42, values.RedactionPublic, values.RetentionProject)}
	store := &corruptingLoadStore{Store: baseStore, ref: *source.Outputs, values: wrong}
	targetDispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(2 * time.Minute) }})
	result, err := targetDispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedNode(t, baseStore, "schema-target", keyDigest, base.Add(time.Minute)), Node: node})
	if err != nil || result.Node.Origin != workflowruntime.OriginExecuted || !hasDiagnostic(result.Diagnostics, workflowruntime.CodeMemoRejected) || executions != 2 {
		t.Fatalf("schema rejection = %#v, %v executions=%d", result, err, executions)
	}
}

func TestMemoAndPinRetentionContractsFailClosed(t *testing.T) {
	runOnly := values.ValueSet{"result": memoValue(t, "result", "run", values.RedactionPublic, values.RetentionRun)}
	if err := workflowruntime.ValidateMemoizableValueSet(runOnly); !errors.Is(err, workflowruntime.ErrReuseDenied) {
		t.Fatalf("run-retained memo value accepted: %v", err)
	}
	if err := workflowruntime.ValidatePinnableValueSet(runOnly, false); !errors.Is(err, workflowruntime.ErrReuseDenied) {
		t.Fatalf("cross-run pin accepted run-retained value: %v", err)
	}
	if err := workflowruntime.ValidatePinnableValueSet(runOnly, true); err != nil {
		t.Fatalf("same-run pin rejected run-retained value: %v", err)
	}
	project := values.ValueSet{"result": memoValue(t, "result", "project", values.RedactionPrivate, values.RetentionProject)}
	if err := workflowruntime.ValidateMemoizableValueSet(project); err != nil {
		t.Fatalf("project-retained private memo value rejected: %v", err)
	}
}

func TestReusePersistenceMetadataRejectsCredentialsAndUnboundedFields(t *testing.T) {
	validAuthority := workflowruntime.ReuseAuthority{Principal: "developer@example.test", Scope: "project", Attributes: map[string]string{"role": "developer"}}
	if err := validAuthority.Validate(); err != nil {
		t.Fatalf("valid authority: %v", err)
	}
	validDecision := workflowruntime.ReusePolicyDecision{Allow: true, Code: "reuse_allowed", Reason: "approved by project policy", Attributes: map[string]string{"rule": "safe-reuse"}}
	if err := validDecision.Validate(); err != nil {
		t.Fatalf("valid decision: %v", err)
	}
	tests := []struct {
		name     string
		validate func() error
	}{
		{name: "long principal", validate: func() error { return (workflowruntime.ReuseAuthority{Principal: strings.Repeat("p", 257)}).Validate() }},
		{name: "secret reference", validate: func() error { return (workflowruntime.ReuseAuthority{Principal: "secret://provider/key"}).Validate() }},
		{name: "credentialed uri", validate: func() error {
			return (workflowruntime.ReuseAuthority{Principal: "https://user:password@example.test"}).Validate()
		}},
		{name: "signed query", validate: func() error {
			return (workflowruntime.ReusePolicyDecision{Allow: true, Code: "allowed", Reason: "https://example.test/callback?signature=value"}).Validate()
		}},
		{name: "sensitive attribute key", validate: func() error {
			return (workflowruntime.ReuseAuthority{Principal: "developer", Attributes: map[string]string{"access-token": "redacted"}}).Validate()
		}},
		{name: "bearer material", validate: func() error {
			return (workflowruntime.ReusePolicyDecision{Allow: true, Code: "allowed", Reason: "Bearer credential"}).Validate()
		}},
		{name: "long reason", validate: func() error {
			return (workflowruntime.ReusePolicyDecision{Allow: true, Code: "allowed", Reason: strings.Repeat("r", 1025)}).Validate()
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.validate(); err == nil {
				t.Fatal("credential-shaped or unbounded metadata accepted")
			}
		})
	}
}

func TestMemoizationReusesAcrossFanOutInvocationIdentities(t *testing.T) {
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("memo-fanout", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	executions := 0
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executions++
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": memoValue(t, "result", "shared", values.RedactionPublic, values.RetentionProject)}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	store := inmemory.NewStore()
	base := time.Date(2026, time.August, 24, 19, 30, 0, 0, time.UTC)
	createRun(t, store, "memo-fanout", base)
	keyDigest, _ := values.DigestInline("same-item")
	node := graph.Node{ID: "node", Kind: "memo-fanout", KindVersion: "v1", Memoization: &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.input"}, MaxAge: "1h"}}
	dispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	sourceID := workflowruntime.NodeInvocationID{RunID: "memo-fanout", NodeID: "node", Iteration: "item-0001"}
	first, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedInvocation(t, store, sourceID, keyDigest, base), Node: node})
	if err != nil || first.Node.Origin != workflowruntime.OriginExecuted {
		t.Fatalf("fan-out source = %#v, %v", first, err)
	}
	targetID := workflowruntime.NodeInvocationID{RunID: "memo-fanout", NodeID: "node", Iteration: "item-0002"}
	second, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedInvocation(t, store, targetID, keyDigest, base.Add(time.Minute)), Node: node})
	if err != nil || second.Node.Origin != workflowruntime.OriginMemoized || executions != 1 || second.Outputs == nil || first.Outputs == nil || *second.Outputs != *first.Outputs {
		t.Fatalf("fan-out reuse = %#v, %v executions=%d", second, err, executions)
	}
}

func TestMemoizationKeySelectsEquivalenceDespiteIrrelevantInputChanges(t *testing.T) {
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("memo-selector", "v1")
	kind.SpecValue.InputSchema = graph.Schema{"type": "object", "required": []any{"key", "irrelevant"}, "properties": map[string]any{"key": map[string]any{"type": "string"}, "irrelevant": map[string]any{"type": "string"}}, "additionalProperties": false}
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	executions := 0
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executions++
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": memoValue(t, "result", "selected", values.RedactionPublic, values.RetentionProject)}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	store := inmemory.NewStore()
	base := time.Date(2026, time.August, 24, 20, 0, 0, 0, time.UTC)
	keyDigest, _ := values.DigestInline("stable-key")
	node := graph.Node{ID: "node", Kind: "memo-selector", KindVersion: "v1", Memoization: &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.key"}, MaxAge: "1h"}}
	firstInputs := values.ValueSet{"key": memoValue(t, "key", "stable-key", values.RedactionPublic, values.RetentionProject), "irrelevant": memoValue(t, "irrelevant", "first", values.RedactionPublic, values.RetentionProject)}
	secondInputs := values.ValueSet{"key": memoValue(t, "key", "stable-key", values.RedactionPublic, values.RetentionProject), "irrelevant": memoValue(t, "irrelevant", "changed", values.RedactionPublic, values.RetentionProject)}
	firstID := workflowruntime.NodeInvocationID{RunID: "selector-first", NodeID: "node"}
	createRun(t, store, firstID.RunID, base)
	dispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	first, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedInvocationValues(t, store, firstID, keyDigest, firstInputs, base), Node: node})
	if err != nil || first.Node.Origin != workflowruntime.OriginExecuted {
		t.Fatalf("selector source = %#v, %v", first, err)
	}
	secondID := workflowruntime.NodeInvocationID{RunID: "selector-second", NodeID: "node"}
	createRun(t, store, secondID.RunID, base.Add(time.Minute))
	second, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: memoClaimedInvocationValues(t, store, secondID, keyDigest, secondInputs, base.Add(time.Minute)), Node: node})
	if err != nil || second.Node.Origin != workflowruntime.OriginMemoized || executions != 1 || second.Outputs == nil || first.Outputs == nil || *second.Outputs != *first.Outputs {
		t.Fatalf("selector reuse = %#v, %v executions=%d", second, err, executions)
	}
}

func memoClaimedNode(t *testing.T, store *inmemory.Store, run string, memoDigest string, at time.Time) workflowruntime.ReadyClaim {
	t.Helper()
	node := memoPendingNode(t, store, run, memoDigest, at)
	return memoClaimExisting(t, store, node, at)
}

func memoClaimedInvocation(t *testing.T, store *inmemory.Store, id workflowruntime.NodeInvocationID, memoDigest string, at time.Time) workflowruntime.ReadyClaim {
	t.Helper()
	inputs := values.ValueSet{"input": memoValue(t, "input", "hello", values.RedactionPublic, values.RetentionProject)}
	return memoClaimedInvocationValues(t, store, id, memoDigest, inputs, at)
}

func memoClaimedInvocationValues(t *testing.T, store *inmemory.Store, id workflowruntime.NodeInvocationID, memoDigest string, inputs values.ValueSet, at time.Time) workflowruntime.ReadyClaim {
	t.Helper()
	node := memoPendingInvocationValues(t, store, id, memoDigest, inputs, at)
	return memoClaimExisting(t, store, node, at)
}

func memoClaimExisting(t *testing.T, store *inmemory.Store, node workflowruntime.NodeInvocationSnapshot, at time.Time) workflowruntime.ReadyClaim {
	t.Helper()
	ready, err := store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: at.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	identity := fmt.Sprintf("%s-%s-%s", node.ID.RunID, node.ID.NodeID, node.ID.Iteration)
	claim, ok, err := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(context.Background(), workflowruntime.ReadyClaimRequest{Owner: "worker", Token: "token-" + identity, IdempotencyKey: "claim-" + identity, Now: at.Add(2 * time.Second), LeaseUntil: at.Add(time.Hour)})
	if err != nil || !ok || claim.Candidate.InvocationID != ready.Snapshot.ID {
		t.Fatalf("ClaimNext = %#v/%v/%v", claim, ok, err)
	}
	return claim
}

func memoPendingNode(t *testing.T, store *inmemory.Store, run string, memoDigest string, at time.Time) workflowruntime.NodeInvocationSnapshot {
	t.Helper()
	runID := workflowruntime.RunID(run)
	createRun(t, store, runID, at)
	return memoPendingInvocation(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "node"}, memoDigest, at)
}

func memoPendingInvocation(t *testing.T, store *inmemory.Store, id workflowruntime.NodeInvocationID, memoDigest string, at time.Time) workflowruntime.NodeInvocationSnapshot {
	t.Helper()
	inputs := values.ValueSet{"input": memoValue(t, "input", "hello", values.RedactionPublic, values.RetentionProject)}
	return memoPendingInvocationValues(t, store, id, memoDigest, inputs, at)
}

func memoPendingInvocationValues(t *testing.T, store *inmemory.Store, id workflowruntime.NodeInvocationID, memoDigest string, inputs values.ValueSet, at time.Time) workflowruntime.NodeInvocationSnapshot {
	t.Helper()
	node, err := store.CreateNodeInvocation(context.Background(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: id, Status: workflowruntime.NodePending, CreatedAt: at, UpdatedAt: at}})
	if err != nil {
		t.Fatal(err)
	}
	identity := fmt.Sprintf("%s-%s-%s", id.RunID, id.NodeID, id.Iteration)
	bound, err := store.BindNodeInputs(context.Background(), workflowruntime.BindNodeInputsRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, IdempotencyKey: "bind-" + identity, Values: inputs, MemoKeyDigest: memoDigest, At: at})
	if err != nil {
		t.Fatal(err)
	}
	return bound.Node
}

type failingMemoStore struct {
	*inmemory.Store
	failure error
}

func (s *failingMemoStore) RecordMemoEntry(context.Context, workflowruntime.MemoEntry) (workflowruntime.MemoEntry, workflowruntime.IdempotencyOutcome, error) {
	return workflowruntime.MemoEntry{}, "", s.failure
}

type corruptingLoadStore struct {
	*inmemory.Store
	ref    values.ValueSetRef
	values values.ValueSet
}

type fixedValueRecordStore struct {
	record workflowruntime.ValueRecord
}

func (s fixedValueRecordStore) LoadValueRecord(context.Context, values.ValueSetRef) (workflowruntime.ValueRecord, error) {
	return s.record, nil
}

type countingPinStore struct {
	workflowruntime.PinStore
	bindCalls int
}

func (s *countingPinStore) BindPin(ctx context.Context, request workflowruntime.BindPinRequest) (workflowruntime.BindPinResult, error) {
	s.bindCalls++
	return s.PinStore.BindPin(ctx, request)
}

func (s *corruptingLoadStore) LoadValues(ctx context.Context, ref values.ValueSetRef) (values.ValueSet, error) {
	if ref == s.ref {
		return s.values, nil
	}
	return s.Store.LoadValues(ctx, ref)
}

func memoValue(t *testing.T, output string, payload any, redaction values.RedactionClass, retention values.RetentionClass) values.Value {
	t.Helper()
	value, err := values.NewInline(payload, values.Metadata{Producer: values.Producer{Kind: "fixture", Reference: "node", Output: output}, MediaType: "application/json", Redaction: redaction, Retention: retention})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func findEvent(events []workflowruntime.Event, kind string) *workflowruntime.Event {
	for _, event := range events {
		if event.Type == kind {
			cloned := event
			return &cloned
		}
	}
	return nil
}
func hasDiagnostic(findings []diagnostic.Diagnostic, code diagnostic.Code) bool {
	for _, finding := range findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}
