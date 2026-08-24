package gateadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	gateadapter "github.com/hollis-labs/hadron/workflow/adapters/gate"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	workflowgate "github.com/hollis-labs/hadron/workflow/gate"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

var gateTime = time.Date(2026, time.August, 24, 16, 0, 0, 0, time.UTC)

func TestRegisterHumanGateAndHonestOutputSchema(t *testing.T) {
	registry := stepkind.NewRegistry()
	executor, err := gateadapter.Register(registry, gateOptions(nil, nil))
	if err != nil || executor == nil {
		t.Fatalf("Register = %#v, %v", executor, err)
	}
	_, spec, err := stepkind.Resolve(registry, gateadapter.Name, gateadapter.Version)
	if err != nil || !spec.CanSuspend || spec.Observation.Mode != stepkind.ObservationNone {
		t.Fatalf("spec = %#v, %v", spec, err)
	}
	encoded, _ := json.Marshal(spec.OutputSchema)
	if !strings.Contains(string(encoded), `"timed_out":{"const":false}`) {
		t.Fatalf("output schema claims unreachable timeout success: %s", encoded)
	}
}

func TestHumanGateSuspendsWithSharedCheckpointAndPrivatePayload(t *testing.T) {
	var authorityRequest workflowgate.AuthorizationRequest
	var payloadRequest workflowgate.PayloadRequest
	var authorityLabel, payloadLabel string
	options := gateadapter.Options{
		Authority: workflowgate.AuthorityResolverFunc(func(_ context.Context, request workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
			authorityRequest = request
			authorityLabel = request.Checkpoint.Options[0].Label
			request.Checkpoint.Options[0].Label = "mutated"
			return workflowwait.ResponderAuthority{Kind: workflowgate.AuthorityGatePolicy, Reference: "production-policy"}, nil
		}),
		Payloads: workflowgate.PayloadStoreFunc(func(_ context.Context, request workflowgate.PayloadRequest) (values.ValueSetRef, error) {
			payloadRequest = request
			payloadLabel = request.Checkpoint.Options[0].Label
			request.Checkpoint.Options[0].Label = "mutated-again"
			return values.ValueSetRef{ID: "gate-payload-1", Digest: values.SHA256Digest([]byte("gate-payload"))}, nil
		}),
		Now: func() time.Time { return gateTime },
	}
	executor, err := gateadapter.New(options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(t.Context(), gatePrepared(gateConfig(false, true)))
	if err != nil || result.Outcome != stepkind.StepWaiting || result.Wait == nil {
		t.Fatalf("Execute = %#v, %v", result, err)
	}
	record := result.Wait.Record
	if record.Kind != workflowwait.KindGate || record.WakeSource != workflowwait.WakeGate || record.Payload == nil || record.Payload.ID != "gate-payload-1" ||
		record.Visibility != workflowwait.VisibilityPrivate || !record.Deadline.Equal(gateTime.Add(24*time.Hour)) || result.Wait.ResumeToken != "" {
		t.Fatalf("gate record = %#v", record)
	}
	if authorityRequest.Checkpoint.Subject.Kind != workflowgate.PolicyEnvironment || authorityRequest.Checkpoint.Subject.Reference != "production" ||
		authorityRequest.Attempt != 2 || payloadRequest.Attempt != 2 || authorityLabel != "Approve" || payloadLabel != "Approve" {
		t.Fatalf("host requests were not defensive: %#v / %#v", authorityRequest, payloadRequest)
	}
	if result.Wait.ID == "" || strings.Contains(result.Wait.ID, "production-policy") {
		t.Fatalf("wait identity leaked policy: %q", result.Wait.ID)
	}
}

func TestHumanGatePayloadStorageIsRetryStableForImmutableCheckpoint(t *testing.T) {
	var requests []workflowgate.PayloadRequest
	ref := values.ValueSetRef{ID: "gate-payload-retry", Digest: values.SHA256Digest([]byte("gate-payload-retry"))}
	payloads := workflowgate.PayloadStoreFunc(func(_ context.Context, request workflowgate.PayloadRequest) (values.ValueSetRef, error) {
		requests = append(requests, request)
		return ref, nil
	})
	executor, err := gateadapter.New(gateOptions(nil, payloads))
	if err != nil {
		t.Fatal(err)
	}
	first, err := executor.Execute(t.Context(), gatePrepared(gateConfig(false, true)))
	if err != nil {
		t.Fatal(err)
	}
	second, err := executor.Execute(t.Context(), gatePrepared(gateConfig(false, true)))
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 || !reflect.DeepEqual(requests[0], requests[1]) || first.Wait.ID != second.Wait.ID || first.Wait.Record.Payload == nil || second.Wait.Record.Payload == nil || *first.Wait.Record.Payload != ref || *second.Wait.Record.Payload != ref {
		t.Fatalf("gate payload retry identity = requests %#v, waits %#v / %#v", requests, first.Wait, second.Wait)
	}
}

func TestHumanGateContinuationDecisionSkipAndResumeMetadata(t *testing.T) {
	executor, _ := gateadapter.New(gateOptions(nil, nil))
	config := gateConfig(true, true)
	initial, err := executor.Execute(t.Context(), gatePrepared(config))
	if err != nil {
		t.Fatal(err)
	}
	resume := inline(t, map[string]any{"decision": "skip"}, values.RedactionPrivate, values.RetentionRun)
	invocation := gatePrepared(config)
	invocation.Invocation.Continuation = gateContinuation(t, initial.Wait, resume)
	completed, err := executor.Execute(t.Context(), invocation)
	if err != nil || completed.Outcome != stepkind.StepCompleted {
		t.Fatalf("continued Execute = %#v, %v", completed, err)
	}
	if completed.Outputs["decision"].Inline != "skip" || completed.Outputs["skipped"].Inline != true || completed.Outputs["timed_out"].Inline != false {
		t.Fatalf("typed gate outputs = %#v", completed.Outputs)
	}
	metadata := completed.Outputs["resume"].Inline.(map[string]any)
	responder := metadata["responder"].(map[string]any)
	if metadata["status"] != "resumed" || metadata["source"] != "gate" || responder["reference"] != "user-1" {
		t.Fatalf("resume metadata = %#v", metadata)
	}
	if !strings.Contains(completed.Outputs["decision"].Producer.Reference, "/attempt-2") {
		t.Fatalf("producer provenance = %#v", completed.Outputs["decision"].Producer)
	}
	forged := gatePrepared(config)
	forged.Invocation.Continuation = gateContinuation(t, initial.Wait, inline(t, map[string]any{"decision": "skip", "forged": true}, values.RedactionPrivate, values.RetentionRun))
	if _, err := executor.Execute(t.Context(), forged); err == nil || !errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("schema-invalid direct gate continuation = %v", err)
	}
}

func TestHumanGateOptionalAndProductPolicyFailClosed(t *testing.T) {
	executor, _ := gateadapter.New(gateOptions(nil, nil))
	for _, test := range []struct {
		name   string
		config graph.Config
		path   string
	}{
		{"nonblocking", gateConfig(true, false), "config.blocking"},
		{"optional_without_skip", gateConfig(false, true), "config"},
		{"approvers", withField(gateConfig(false, true), "approvers", []any{"alice"}), "config.approvers"},
		{"dynamic_environment", withField(gateConfig(false, true), "environment", "{{ inputs.environment }}"), "config.environment"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := test.config
			if test.name == "optional_without_skip" {
				config["optional"] = true
			}
			findings := executor.ValidateConfig(t.Context(), config)
			if len(findings) == 0 || !strings.Contains(diagnosticMessages(findings), test.path) {
				t.Fatalf("diagnostics = %#v", findings)
			}
			if _, err := executor.Execute(t.Context(), gatePrepared(config)); err == nil {
				t.Fatal("invalid gate executed")
			}
		})
	}

	valid := gateConfig(false, true)
	first := executor.ValidateConfig(t.Context(), withField(valid, "z_unknown", true))
	for range 30 {
		if repeated := executor.ValidateConfig(t.Context(), withField(valid, "z_unknown", true)); !reflect.DeepEqual(first, repeated) {
			t.Fatalf("nondeterministic diagnostics: %#v / %#v", first, repeated)
		}
	}
}

func TestHumanGateTypedNilCancellationClassificationAndConcurrency(t *testing.T) {
	var nilAuthority *gateAuthorityStub
	if _, err := gateadapter.New(gateadapter.Options{Authority: nilAuthority, Payloads: gateOptions(nil, nil).Payloads}); err == nil {
		t.Fatal("typed-nil authority accepted")
	}
	var nilPayload *gatePayloadStub
	if _, err := gateadapter.New(gateadapter.Options{Authority: gateOptions(nil, nil).Authority, Payloads: nilPayload}); err == nil {
		t.Fatal("typed-nil payload store accepted")
	}

	ctx, cancel := context.WithCancel(context.Background())
	executor, _ := gateadapter.New(gateadapter.Options{
		Authority: workflowgate.AuthorityResolverFunc(func(context.Context, workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
			cancel()
			return workflowwait.ResponderAuthority{Kind: "test"}, nil
		}),
		Payloads: gateOptions(nil, nil).Payloads, Now: func() time.Time { return gateTime },
	})
	if _, err := executor.Execute(ctx, gatePrepared(gateConfig(false, true))); !errors.Is(err, context.Canceled) {
		t.Fatalf("late authority cancellation = %v", err)
	}

	payloadCtx, payloadCancel := context.WithCancel(context.Background())
	executor, _ = gateadapter.New(gateadapter.Options{
		Authority: gateOptions(nil, nil).Authority,
		Payloads: workflowgate.PayloadStoreFunc(func(context.Context, workflowgate.PayloadRequest) (values.ValueSetRef, error) {
			payloadCancel()
			return values.ValueSetRef{ID: "orphan-with-host-cleanup", Digest: values.SHA256Digest([]byte("orphan"))}, nil
		}),
		Now: func() time.Time { return gateTime },
	})
	if _, err := executor.Execute(payloadCtx, gatePrepared(gateConfig(false, true))); !errors.Is(err, context.Canceled) {
		t.Fatalf("late payload cancellation = %v", err)
	}
	clockCtx, clockCancel := context.WithCancel(context.Background())
	executor, _ = gateadapter.New(gateadapter.Options{
		Authority: gateOptions(nil, nil).Authority, Payloads: gateOptions(nil, nil).Payloads,
		Now: func() time.Time { clockCancel(); return gateTime },
	})
	if _, err := executor.Execute(clockCtx, gatePrepared(gateConfig(false, true))); !errors.Is(err, context.Canceled) {
		t.Fatalf("late clock cancellation = %v", err)
	}

	standard, _ := gateadapter.New(gateOptions(nil, nil))
	initial, err := standard.Execute(t.Context(), gatePrepared(gateConfig(false, true)))
	if err != nil {
		t.Fatal(err)
	}
	for _, unsafe := range []values.Value{
		inline(t, map[string]any{"decision": "approve"}, values.RedactionPublic, values.RetentionRun),
		inline(t, map[string]any{"decision": "approve"}, values.RedactionPrivate, values.RetentionNone),
	} {
		invocation := gatePrepared(gateConfig(false, true))
		invocation.Invocation.Continuation = gateContinuation(t, initial.Wait, unsafe)
		if _, err := standard.Execute(t.Context(), invocation); err == nil {
			t.Fatalf("unsafe gate resume accepted: %v", err)
		}
	}

	const workers = 32
	results := make(chan string, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := standard.Execute(context.Background(), gatePrepared(gateConfig(false, true)))
			if err != nil {
				results <- "error:" + err.Error()
				return
			}
			results <- result.Wait.ID
		}()
	}
	group.Wait()
	close(results)
	var expected string
	for result := range results {
		if expected == "" {
			expected = result
		}
		if result != expected {
			t.Fatalf("concurrent gate IDs differ: %q / %q", expected, result)
		}
	}
}

func TestHumanGateAuthorityResultIsDefensivelyOwned(t *testing.T) {
	attributes := map[string]string{"role": "reviewer"}
	options := gateOptions(workflowgate.AuthorityResolverFunc(func(context.Context, workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
		return workflowwait.ResponderAuthority{Kind: workflowgate.AuthorityGatePolicy, Reference: "stable", Attributes: attributes}, nil
	}), nil)
	executor, err := gateadapter.New(options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(t.Context(), gatePrepared(gateConfig(false, true)))
	if err != nil {
		t.Fatal(err)
	}
	attributes["role"] = "mutated"
	attributes["new"] = "host"
	if result.Wait.Record.Authority.Attributes["role"] != "reviewer" || result.Wait.Record.Authority.Attributes["new"] != "" {
		t.Fatalf("host retained authority ownership: %#v", result.Wait.Record.Authority.Attributes)
	}
}

type gateAuthorityStub struct{}

func (*gateAuthorityStub) ResolveGateAuthority(context.Context, workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
	return workflowwait.ResponderAuthority{Kind: "test"}, nil
}

type gatePayloadStub struct{}

func (*gatePayloadStub) StoreGatePayload(context.Context, workflowgate.PayloadRequest) (values.ValueSetRef, error) {
	return values.ValueSetRef{ID: "payload", Digest: values.SHA256Digest([]byte("payload"))}, nil
}

func gateOptions(authority workflowgate.AuthorityResolver, payloads workflowgate.PayloadStore) gateadapter.Options {
	if authority == nil {
		authority = workflowgate.AuthorityResolverFunc(func(_ context.Context, _ workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
			return workflowwait.ResponderAuthority{Kind: workflowgate.AuthorityGatePolicy, Reference: "policy"}, nil
		})
	}
	if payloads == nil {
		payloads = workflowgate.PayloadStoreFunc(func(_ context.Context, _ workflowgate.PayloadRequest) (values.ValueSetRef, error) {
			return values.ValueSetRef{ID: "gate-payload", Digest: values.SHA256Digest([]byte("payload"))}, nil
		})
	}
	return gateadapter.Options{Authority: authority, Payloads: payloads, Now: func() time.Time { return gateTime }}
}

func gateConfig(optional, blocking bool) graph.Config {
	options := []any{
		map[string]any{"id": "approve", "label": "Approve"},
		map[string]any{"id": "reject", "label": "Reject"},
	}
	if optional {
		options = append(options, map[string]any{"id": "skip", "label": "Skip", "kind": "skip"})
	}
	return graph.Config{
		"prompt": "Release version?", "options": options, "environment": "production", "timeout": "24h",
		"optional": optional, "blocking": blocking,
		"escalations": []any{map[string]any{"after": "1h", "subject": map[string]any{"kind": "notification", "reference": "release-team"}}},
	}
}

func withField(config graph.Config, key string, value any) graph.Config {
	copyConfig := make(graph.Config, len(config)+1)
	for name, item := range config {
		copyConfig[name] = item
	}
	copyConfig[key] = value
	return copyConfig
}

func gatePrepared(config graph.Config) stepkind.PreparedInvocation {
	return stepkind.PreparedInvocation{Invocation: stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "approval", Iteration: "item-1", Attempt: 2},
		Config:   config, Inputs: values.ValueSet{},
	}}
}

func gateContinuation(t *testing.T, waitResult *stepkind.WaitResult, payload values.Value) *stepkind.WaitContinuation {
	t.Helper()
	set := values.ValueSet{"resume": payload}
	ref, err := values.NewValueSetRef("gate-resume-values", set)
	if err != nil {
		t.Fatal(err)
	}
	record := waitResult.Record
	record.Status = workflowwait.StatusResumed
	record.ResumeValues = &ref
	record.Resolution = &workflowwait.Resolution{
		Source: workflowwait.WakeGate, Responder: workflowwait.Responder{Kind: "user", Reference: "user-1"},
		PayloadDigest: ref.Digest, IdempotencyKey: "gate-decision-1", ResolvedAt: gateTime.Add(time.Minute),
	}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	return &stepkind.WaitContinuation{ID: waitResult.ID, Record: record, Values: set}
}

func inline(t *testing.T, value any, redaction values.RedactionClass, retention values.RetentionClass) values.Value {
	t.Helper()
	result, err := values.NewInline(value, values.Metadata{
		Producer: values.Producer{Kind: "resume", Reference: "resume-1"}, MediaType: "application/json", Redaction: redaction, Retention: retention,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func diagnosticMessages(findings []diagnostic.Diagnostic) string {
	messages := make([]string, 0, len(findings))
	for _, finding := range findings {
		messages = append(messages, finding.Message)
	}
	return strings.Join(messages, "\n")
}
