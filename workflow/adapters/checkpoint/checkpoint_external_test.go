package checkpoint_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	checkpointadapter "github.com/hollis-labs/hadron/workflow/adapters/checkpoint"
	gateadapter "github.com/hollis-labs/hadron/workflow/adapters/gate"
	workflowgate "github.com/hollis-labs/hadron/workflow/gate"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

var checkpointTime = time.Date(2026, time.August, 24, 20, 0, 0, 0, time.UTC)

func TestRegisterCheckpointMetadataAndConfiguredResumeSchema(t *testing.T) {
	registry := stepkind.NewRegistry()
	executor, err := checkpointadapter.Register(registry, checkpointOptions(nil, nil))
	if err != nil || executor == nil {
		t.Fatalf("Register = %#v, %v", executor, err)
	}
	_, spec, err := stepkind.Resolve(registry, checkpointadapter.KindName, checkpointadapter.KindVersion)
	if err != nil || !spec.CanSuspend || spec.EmbeddedModeSupported || spec.Observation.Mode != stepkind.ObservationNone ||
		spec.Idempotency != graph.IdempotencyIntrinsic || spec.RetrySafety != stepkind.RetrySafe ||
		!reflect.DeepEqual(spec.RequiredCapabilities, []string{checkpointadapter.CapabilityRespond, checkpointadapter.CapabilityWait}) {
		t.Fatalf("checkpoint spec = %#v, %v", spec, err)
	}
	encoded, _ := json.Marshal(spec.OutputSchema)
	if !strings.Contains(string(encoded), `"timed_out":{"const":false}`) || !strings.Contains(string(encoded), `"triggered":{"type":"boolean"}`) {
		t.Fatalf("output schema claims an unreachable successful timeout: %s", encoded)
	}

	result, err := executor.Execute(t.Context(), checkpointPrepared(checkpointConfig()))
	if err != nil || result.Wait == nil {
		t.Fatalf("Execute = %#v, %v", result, err)
	}
	expected, err := workflowwait.NewSchemaRef(checkpointDecisionSchema())
	if err != nil {
		t.Fatal(err)
	}
	if result.Wait.Record.ResumeSchema.Digest != expected.Digest || !reflect.DeepEqual(result.Wait.Record.ResumeSchema.Schema, expected.Schema) {
		t.Fatalf("configured decision schema was not surfaced exactly: %#v / %#v", result.Wait.Record.ResumeSchema, expected)
	}
}

func TestCheckpointOptionalNonBlockingTriggerAndProceedOutputs(t *testing.T) {
	var authorityCalls, payloadCalls int
	executor, err := checkpointadapter.New(checkpointadapter.Options{
		Authority: workflowgate.AuthorityResolverFunc(func(context.Context, workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
			authorityCalls++
			return workflowwait.ResponderAuthority{Kind: workflowgate.AuthorityGatePolicy, Reference: "release-policy"}, nil
		}),
		Payloads: workflowgate.PayloadStoreFunc(func(context.Context, workflowgate.PayloadRequest) (values.ValueSetRef, error) {
			payloadCalls++
			return values.ValueSetRef{ID: "checkpoint-payload", Digest: values.SHA256Digest([]byte("checkpoint-payload"))}, nil
		}),
		Now: func() time.Time { return checkpointTime },
	})
	if err != nil {
		t.Fatal(err)
	}
	config := checkpointWithTwo("optional", true, "blocking", false)
	config["trigger_input"], config["not_triggered"], config["default_decision"] = "gate-trigger", "proceed", "approve"
	falseInvocation := checkpointPrepared(config)
	falseInvocation.Invocation.Inputs["gate-trigger"] = checkpointResume(t, false, values.RedactionPrivate, values.RetentionRun)
	completed, err := executor.Execute(t.Context(), falseInvocation)
	if err != nil || completed.Outcome != stepkind.StepCompleted || completed.Outputs["decision"].Inline != "approve" || completed.Outputs["skipped"].Inline != true || completed.Outputs["triggered"].Inline != false || completed.Outputs["timed_out"].Inline != false {
		t.Fatalf("non-triggered proceed = %#v, %v", completed, err)
	}
	resume := completed.Outputs["resume"].Inline.(map[string]any)
	if resume["status"] != "not-triggered" || resume["source"] != "not-triggered" || resume["correlation"] != config["correlation"] || resume["wait_id"] != nil || resume["responder"] != nil || resume["resolved_at"] != nil || authorityCalls != 0 || payloadCalls != 0 {
		t.Fatalf("non-triggered resume/side effects = %#v authority=%d payload=%d", resume, authorityCalls, payloadCalls)
	}
	trueInvocation := checkpointPrepared(config)
	trueInvocation.Invocation.Inputs["gate-trigger"] = checkpointResume(t, true, values.RedactionPrivate, values.RetentionRun)
	waiting, err := executor.Execute(t.Context(), trueInvocation)
	if err != nil || waiting.Outcome != stepkind.StepWaiting || authorityCalls != 1 || payloadCalls != 1 {
		t.Fatalf("triggered wait = %#v calls=%d/%d err=%v", waiting, authorityCalls, payloadCalls, err)
	}
	continued := trueInvocation
	continued.Invocation.Continuation = checkpointContinuation(t, waiting.Wait, checkpointResume(t, map[string]any{"decision": "reject"}, values.RedactionPrivate, values.RetentionRun))
	resolved, err := executor.Execute(t.Context(), continued)
	if err != nil || resolved.Outputs["triggered"].Inline != true || resolved.Outputs["decision"].Inline != "reject" {
		t.Fatalf("triggered continuation = %#v, %v", resolved, err)
	}

	invalidDefault := cloneCheckpointConfig(config)
	invalidDefault["default_decision"] = "missing"
	if findings := executor.ValidateConfig(t.Context(), invalidDefault); len(findings) == 0 {
		t.Fatal("unknown default decision accepted")
	}
	skip := cloneCheckpointConfig(config)
	skip["not_triggered"] = "skip"
	delete(skip, "default_decision")
	invocation := checkpointPrepared(skip)
	invocation.Invocation.Inputs["gate-trigger"] = checkpointResume(t, false, values.RedactionPrivate, values.RetentionRun)
	if _, err := executor.Execute(t.Context(), invocation); err == nil {
		t.Fatal("non-triggered skip bypassed compiler readiness lowering")
	}
}

func TestSharedGateProfileIsCopiedAndNilSpecIsDeterministic(t *testing.T) {
	profile := gateadapter.Profile{
		Name: "custom_gate", Version: "v1", Label: "custom gate", RespondCapability: "custom.respond", WaitCapability: "wait.resume",
		InvalidCode: "custom_invalid", AuthorityFailedCode: "custom_authority", PayloadFailedCode: "custom_payload",
		ContinuationCode: "custom_continuation", DecisionSchema: gateadapter.DecisionSchemaConfigured,
	}
	executor, err := gateadapter.NewProfile(profile, checkpointOptions(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	profile.Name = "mutated"
	profile.RespondCapability = "mutated"
	if spec := executor.Spec(); spec.Name != "custom_gate" || !reflect.DeepEqual(spec.RequiredCapabilities, []string{"custom.respond", "wait.resume"}) {
		t.Fatalf("executor retained caller profile ownership: %#v", spec)
	}
	var nilExecutor *gateadapter.Executor
	if spec := nilExecutor.Spec(); spec.Name != gateadapter.Name || spec.Version != gateadapter.Version {
		t.Fatalf("nil executor spec = %#v", spec)
	}
	for name, mutate := range map[string]func(*gateadapter.Profile){
		"name":       func(value *gateadapter.Profile) { value.Name = "Bad Name" },
		"version":    func(value *gateadapter.Profile) { value.Version = "V 1" },
		"capability": func(value *gateadapter.Profile) { value.RespondCapability = "https://capability" },
		"error_code": func(value *gateadapter.Profile) { value.InvalidCode = "bad code" },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := profile
			invalid.Name = "custom_gate"
			invalid.RespondCapability = "custom.respond"
			mutate(&invalid)
			if _, err := gateadapter.NewProfile(invalid, checkpointOptions(nil, nil)); err == nil {
				t.Fatal("malformed profile accepted")
			}
		})
	}
}

func TestCheckpointSuspendsWithSharedGateContractAndImmutableEscalations(t *testing.T) {
	var authorityRequest workflowgate.AuthorizationRequest
	var payloadRequest workflowgate.PayloadRequest
	var authorityLabel, authorityEscalation, payloadLabel string
	authorityAttributes := map[string]string{"role": "release-reviewer"}
	executor, err := checkpointadapter.New(checkpointadapter.Options{
		Authority: workflowgate.AuthorityResolverFunc(func(_ context.Context, request workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
			authorityRequest = request
			authorityLabel = request.Checkpoint.Options[0].Label
			authorityEscalation = request.Checkpoint.Escalations[0].Subject.Reference
			request.Checkpoint.Options[0].Label = "host-mutated"
			request.Checkpoint.Escalations[0].Subject.Reference = "host-mutated"
			return workflowwait.ResponderAuthority{Kind: workflowgate.AuthorityGatePolicy, Reference: "release-policy", Attributes: authorityAttributes}, nil
		}),
		Payloads: workflowgate.PayloadStoreFunc(func(_ context.Context, request workflowgate.PayloadRequest) (values.ValueSetRef, error) {
			payloadRequest = request
			payloadLabel = request.Checkpoint.Options[0].Label
			request.Checkpoint.Options[0].Label = "payload-mutated"
			return values.ValueSetRef{ID: "checkpoint-payload-1", Digest: values.SHA256Digest([]byte("checkpoint-payload-1"))}, nil
		}),
		Now: func() time.Time { return checkpointTime },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(t.Context(), checkpointPrepared(checkpointConfig()))
	if err != nil || result.Outcome != stepkind.StepWaiting || result.Wait == nil {
		t.Fatalf("Execute = %#v, %v", result, err)
	}
	record := result.Wait.Record
	if record.Kind != workflowwait.KindGate || record.WakeSource != workflowwait.WakeGate || record.Payload == nil || record.Payload.ID != "checkpoint-payload-1" ||
		record.Visibility != workflowwait.VisibilityPrivate || !record.Deadline.Equal(checkpointTime.Add(24*time.Hour)) || result.Wait.ResumeToken != "" {
		t.Fatalf("checkpoint wait = %#v", result.Wait)
	}
	if authorityRequest.Checkpoint.Prompt != "Release version?" || authorityRequest.Checkpoint.Subject.Reference != "production" ||
		authorityLabel != "Approve" || authorityEscalation != "release-team" || payloadLabel != "Approve" || payloadRequest.Checkpoint.Escalations[0].After != "1h" ||
		authorityRequest.Attempt != 2 || payloadRequest.Attempt != 2 {
		t.Fatalf("shared checkpoint host requests = %#v / %#v", authorityRequest, payloadRequest)
	}
	authorityAttributes["role"] = "mutated-after-return"
	if record.Authority.Attributes["role"] != "release-reviewer" {
		t.Fatalf("authority attributes retained host ownership: %#v", record.Authority.Attributes)
	}
	// Escalations are persisted presentation/policy metadata only. The adapter
	// deliberately invokes no escalation collaborator.
	if len(payloadRequest.Checkpoint.Escalations) != 1 || result.Wait.ID == "" {
		t.Fatalf("escalation/wait identity = %#v / %q", payloadRequest.Checkpoint.Escalations, result.Wait.ID)
	}
}

func TestCheckpointRetryIdentityAndContinuationAreTyped(t *testing.T) {
	var requests []workflowgate.PayloadRequest
	payloadRef := values.ValueSetRef{ID: "checkpoint-payload-retry", Digest: values.SHA256Digest([]byte("checkpoint-payload-retry"))}
	executor, err := checkpointadapter.New(checkpointOptions(nil, workflowgate.PayloadStoreFunc(func(_ context.Context, request workflowgate.PayloadRequest) (values.ValueSetRef, error) {
		requests = append(requests, request)
		return payloadRef, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	first, err := executor.Execute(t.Context(), checkpointPrepared(checkpointConfig()))
	if err != nil {
		t.Fatal(err)
	}
	second, err := executor.Execute(t.Context(), checkpointPrepared(checkpointConfig()))
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 || !reflect.DeepEqual(requests[0], requests[1]) || first.Wait.ID != second.Wait.ID || *first.Wait.Record.Payload != payloadRef || *second.Wait.Record.Payload != payloadRef {
		t.Fatalf("retry identity = %#v / %#v / %#v", requests, first.Wait, second.Wait)
	}

	invocation := checkpointPrepared(checkpointConfig())
	invocation.Invocation.Continuation = checkpointContinuation(t, first.Wait, checkpointResume(t, map[string]any{"decision": "approve"}, values.RedactionPrivate, values.RetentionRun))
	completed, err := executor.Execute(t.Context(), invocation)
	if err != nil || completed.Outcome != stepkind.StepCompleted || completed.Outputs["decision"].Inline != "approve" || completed.Outputs["skipped"].Inline != false || completed.Outputs["timed_out"].Inline != false || completed.Outputs["triggered"].Inline != true {
		t.Fatalf("continued Execute = %#v, %v", completed, err)
	}
	resume := completed.Outputs["resume"].Inline.(map[string]any)
	if resume["status"] != "resumed" || resume["source"] != "gate" || resume["correlation"] != first.Wait.Record.Correlation ||
		!strings.Contains(completed.Outputs["decision"].Producer.Reference, "/attempt-2") {
		t.Fatalf("typed checkpoint outputs = %#v", completed.Outputs)
	}

	for name, payload := range map[string]values.Value{
		"additional_property": checkpointResume(t, map[string]any{"decision": "approve", "forged": true}, values.RedactionPrivate, values.RetentionRun),
		"unknown_decision":    checkpointResume(t, map[string]any{"decision": "later"}, values.RedactionPrivate, values.RetentionRun),
		"public":              checkpointResume(t, map[string]any{"decision": "approve"}, values.RedactionPublic, values.RetentionRun),
		"nonretained":         checkpointResume(t, map[string]any{"decision": "approve"}, values.RedactionPrivate, values.RetentionNone),
	} {
		t.Run(name, func(t *testing.T) {
			forged := checkpointPrepared(checkpointConfig())
			forged.Invocation.Continuation = checkpointContinuationUnchecked(t, first.Wait, payload)
			if _, executeErr := executor.Execute(t.Context(), forged); executeErr == nil {
				t.Fatal("unsafe checkpoint continuation succeeded")
			}
		})
	}
}

func TestCheckpointFailsClosedBeforeHostEffects(t *testing.T) {
	var authorityCalls, payloadCalls int
	executor, err := checkpointadapter.New(checkpointadapter.Options{
		Authority: workflowgate.AuthorityResolverFunc(func(context.Context, workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
			authorityCalls++
			return workflowwait.ResponderAuthority{Kind: workflowgate.AuthorityGatePolicy, Reference: "policy"}, nil
		}),
		Payloads: workflowgate.PayloadStoreFunc(func(context.Context, workflowgate.PayloadRequest) (values.ValueSetRef, error) {
			payloadCalls++
			return values.ValueSetRef{ID: "payload", Digest: values.SHA256Digest([]byte("payload"))}, nil
		}),
		Now: func() time.Time { return checkpointTime },
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]graph.Config{
		"unknown":              checkpointWith("approvers", []any{"alice"}),
		"dynamic_environment":  checkpointWith("environment", "{{ inputs.environment }}"),
		"bad_schema":           checkpointWith("decision_schema", map[string]any{"$ref": "https://example.invalid/schema"}),
		"nonblocking":          checkpointWithTwo("optional", true, "blocking", false),
		"bad_option_shape":     checkpointWith("options", []any{map[string]any{"id": "approve", "label": "Approve", "secret": true}}),
		"bad_escalation":       checkpointWith("escalations", []any{map[string]any{"after": "0s", "subject": map[string]any{"kind": "notification", "reference": "team"}}}),
		"too_many_options":     checkpointWith("options", repeatedOptions(129)),
		"too_many_escalations": checkpointWith("escalations", repeatedEscalations(33)),
		"config_byte_limit":    checkpointWith("prompt", strings.Repeat("x", 65<<10)),
	}
	for name, config := range cases {
		t.Run(name, func(t *testing.T) {
			beforeAuthority, beforePayload := authorityCalls, payloadCalls
			first := executor.ValidateConfig(t.Context(), config)
			if len(first) == 0 {
				t.Fatal("invalid config had no diagnostic")
			}
			for range 20 {
				if repeated := executor.ValidateConfig(t.Context(), config); !reflect.DeepEqual(first, repeated) {
					t.Fatalf("nondeterministic config diagnostics: %#v / %#v", first, repeated)
				}
			}
			if _, executeErr := executor.Execute(t.Context(), checkpointPrepared(config)); executeErr == nil {
				t.Fatal("invalid checkpoint executed")
			}
			if authorityCalls != beforeAuthority || payloadCalls != beforePayload {
				t.Fatalf("invalid checkpoint reached hosts: authority=%d payload=%d", authorityCalls, payloadCalls)
			}
		})
	}
}

func TestCheckpointRejectsUnsafeHostAuthorityAndForgedResponder(t *testing.T) {
	const secret = "secret://gate.example/reply?token=raw"
	largeAttributes := map[string]string{}
	for index := range 33 {
		largeAttributes["item-"+strings.Repeat("x", index+1)] = "safe"
	}
	for name, authority := range map[string]workflowwait.ResponderAuthority{
		"uri":               {Kind: workflowgate.AuthorityGatePolicy, Reference: "https://gate.example/reply"},
		"kind_credential":   {Kind: "bearer", Reference: "policy"},
		"query":             {Kind: workflowgate.AuthorityGatePolicy, Reference: "policy?mode=review"},
		"userinfo":          {Kind: workflowgate.AuthorityGatePolicy, Reference: "reviewer@policy"},
		"fragment":          {Kind: workflowgate.AuthorityGatePolicy, Reference: "policy#review"},
		"assignment":        {Kind: workflowgate.AuthorityGatePolicy, Reference: "policy=review"},
		"credential_marker": {Kind: workflowgate.AuthorityGatePolicy, Reference: "bearer-policy"},
		"reference_bytes":   {Kind: workflowgate.AuthorityGatePolicy, Reference: strings.Repeat("x", 4097)},
		"attribute_key":     {Kind: workflowgate.AuthorityGatePolicy, Reference: "policy", Attributes: map[string]string{"api_key": "safe"}},
		"attribute_value":   {Kind: workflowgate.AuthorityGatePolicy, Reference: "policy", Attributes: map[string]string{"role": "secret-value"}},
		"attribute_uri":     {Kind: workflowgate.AuthorityGatePolicy, Reference: "policy", Attributes: map[string]string{"role": "https://gate.example"}},
		"attribute_count":   {Kind: workflowgate.AuthorityGatePolicy, Reference: "policy", Attributes: largeAttributes},
		"attribute_bytes":   {Kind: workflowgate.AuthorityGatePolicy, Reference: "policy", Attributes: map[string]string{"role": strings.Repeat("x", 1025)}},
	} {
		t.Run(name, func(t *testing.T) {
			payloadCalls := 0
			executor, err := checkpointadapter.New(checkpointadapter.Options{
				Authority: workflowgate.AuthorityResolverFunc(func(context.Context, workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
					return authority, nil
				}),
				Payloads: workflowgate.PayloadStoreFunc(func(context.Context, workflowgate.PayloadRequest) (values.ValueSetRef, error) {
					payloadCalls++
					return values.ValueSetRef{ID: "unsafe-payload", Digest: values.SHA256Digest([]byte("unsafe"))}, nil
				}),
				Now: func() time.Time { return checkpointTime },
			})
			if err != nil {
				t.Fatal(err)
			}
			result, executeErr := executor.Execute(t.Context(), checkpointPrepared(checkpointConfig()))
			if executeErr == nil || strings.Contains(executeErr.Error(), authority.Reference) || payloadCalls != 0 || !reflect.ValueOf(result).IsZero() {
				t.Fatalf("unsafe host authority = %#v, %v, payload calls %d", result, executeErr, payloadCalls)
			}
		})
	}

	standard, _ := checkpointadapter.New(checkpointOptions(nil, nil))
	initial, err := standard.Execute(t.Context(), checkpointPrepared(checkpointConfig()))
	if err != nil {
		t.Fatal(err)
	}
	continuation := checkpointContinuation(t, initial.Wait, checkpointResume(t, map[string]any{"decision": "approve"}, values.RedactionPrivate, values.RetentionRun))
	continuation.Record.Resolution.Responder.Reference = secret
	invocation := checkpointPrepared(checkpointConfig())
	invocation.Invocation.Continuation = continuation
	if _, err := standard.Execute(t.Context(), invocation); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe forged responder = %v", err)
	}
	continuation = checkpointContinuation(t, initial.Wait, checkpointResume(t, map[string]any{"decision": "approve"}, values.RedactionPrivate, values.RetentionRun))
	continuation.Record.Resolution.Responder.Attributes = map[string]string{"authorization": "raw"}
	invocation.Invocation.Continuation = continuation
	if result, err := standard.Execute(t.Context(), invocation); err == nil || !reflect.ValueOf(result).IsZero() {
		t.Fatalf("unsafe forged responder attributes = %#v, %v", result, err)
	}
}

func TestCheckpointTypedNilCancellationErrorsAndConcurrency(t *testing.T) {
	var nilRegistry *checkpointRegistryStub
	if _, err := checkpointadapter.Register(nilRegistry, checkpointOptions(nil, nil)); err == nil {
		t.Fatal("typed-nil registry accepted")
	}
	var nilAuthority *checkpointAuthorityStub
	if _, err := checkpointadapter.New(checkpointadapter.Options{Authority: nilAuthority, Payloads: checkpointOptions(nil, nil).Payloads}); err == nil {
		t.Fatal("typed-nil authority accepted")
	}
	var nilPayload *checkpointPayloadStub
	if _, err := checkpointadapter.New(checkpointadapter.Options{Authority: checkpointOptions(nil, nil).Authority, Payloads: nilPayload}); err == nil {
		t.Fatal("typed-nil payload store accepted")
	}

	ctx, cancel := context.WithCancel(context.Background())
	executor, _ := checkpointadapter.New(checkpointadapter.Options{
		Authority: workflowgate.AuthorityResolverFunc(func(context.Context, workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
			cancel()
			return workflowwait.ResponderAuthority{Kind: workflowgate.AuthorityGatePolicy, Reference: "policy"}, nil
		}),
		Payloads: checkpointOptions(nil, nil).Payloads, Now: func() time.Time { return checkpointTime },
	})
	if _, err := executor.Execute(ctx, checkpointPrepared(checkpointConfig())); !errors.Is(err, context.Canceled) {
		t.Fatalf("late authority cancellation = %v", err)
	}

	payloadCtx, payloadCancel := context.WithCancel(context.Background())
	executor, _ = checkpointadapter.New(checkpointadapter.Options{
		Authority: checkpointOptions(nil, nil).Authority,
		Payloads: workflowgate.PayloadStoreFunc(func(context.Context, workflowgate.PayloadRequest) (values.ValueSetRef, error) {
			payloadCancel()
			return values.ValueSetRef{ID: "orphan-host-cleans-up", Digest: values.SHA256Digest([]byte("orphan"))}, nil
		}),
		Now: func() time.Time { return checkpointTime },
	})
	if _, err := executor.Execute(payloadCtx, checkpointPrepared(checkpointConfig())); !errors.Is(err, context.Canceled) {
		t.Fatalf("late payload cancellation = %v", err)
	}

	clockCtx, clockCancel := context.WithCancel(context.Background())
	executor, _ = checkpointadapter.New(checkpointadapter.Options{
		Authority: checkpointOptions(nil, nil).Authority, Payloads: checkpointOptions(nil, nil).Payloads,
		Now: func() time.Time { clockCancel(); return checkpointTime },
	})
	if _, err := executor.Execute(clockCtx, checkpointPrepared(checkpointConfig())); !errors.Is(err, context.Canceled) {
		t.Fatalf("late clock cancellation = %v", err)
	}

	failing, _ := checkpointadapter.New(checkpointadapter.Options{
		Authority: workflowgate.AuthorityResolverFunc(func(context.Context, workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
			return workflowwait.ResponderAuthority{}, errors.New("authority raw credential secret-token")
		}),
		Payloads: checkpointOptions(nil, nil).Payloads, Now: func() time.Time { return checkpointTime },
	})
	if _, err := failing.Execute(t.Context(), checkpointPrepared(checkpointConfig())); err == nil || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("checkpoint error leaked raw cause: %v", err)
	}

	standard, _ := checkpointadapter.New(checkpointOptions(nil, nil))
	const workers = 24
	results := make(chan string, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			result, executeErr := standard.Execute(context.Background(), checkpointPrepared(checkpointConfig()))
			if executeErr != nil {
				results <- "error:" + executeErr.Error()
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
			t.Fatalf("concurrent checkpoint IDs differ: %q / %q", expected, result)
		}
	}
}

type checkpointAuthorityStub struct{}

type checkpointRegistryStub struct{}

func (*checkpointRegistryStub) Register(stepkind.StepKind) error                { return nil }
func (*checkpointRegistryStub) Lookup(string, string) (stepkind.StepKind, bool) { return nil, false }
func (*checkpointRegistryStub) List() []stepkind.StepKindSpec                   { return nil }

func (*checkpointAuthorityStub) ResolveGateAuthority(context.Context, workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
	return workflowwait.ResponderAuthority{Kind: workflowgate.AuthorityGatePolicy, Reference: "policy"}, nil
}

type checkpointPayloadStub struct{}

func (*checkpointPayloadStub) StoreGatePayload(context.Context, workflowgate.PayloadRequest) (values.ValueSetRef, error) {
	return values.ValueSetRef{ID: "payload", Digest: values.SHA256Digest([]byte("payload"))}, nil
}

func checkpointOptions(authority workflowgate.AuthorityResolver, payload workflowgate.PayloadStore) checkpointadapter.Options {
	if authority == nil {
		authority = workflowgate.AuthorityResolverFunc(func(context.Context, workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
			return workflowwait.ResponderAuthority{Kind: workflowgate.AuthorityGatePolicy, Reference: "release-policy"}, nil
		})
	}
	if payload == nil {
		payload = workflowgate.PayloadStoreFunc(func(context.Context, workflowgate.PayloadRequest) (values.ValueSetRef, error) {
			return values.ValueSetRef{ID: "checkpoint-payload", Digest: values.SHA256Digest([]byte("checkpoint-payload"))}, nil
		})
	}
	return checkpointadapter.Options{Authority: authority, Payloads: payload, Now: func() time.Time { return checkpointTime }}
}

func checkpointConfig() graph.Config {
	return graph.Config{
		"prompt": "Release version?",
		"options": []any{
			map[string]any{"id": "approve", "label": "Approve"},
			map[string]any{"id": "reject", "label": "Reject"},
		},
		"decision_schema": checkpointDecisionSchema(),
		"environment":     "production",
		"correlation":     "release:2026-08-24",
		"timeout":         "24h",
		"escalations": []any{
			map[string]any{"after": "1h", "subject": map[string]any{"kind": "notification", "reference": "release-team"}},
		},
	}
}

func checkpointDecisionSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false, "required": []any{"decision"},
		"properties": map[string]any{"decision": map[string]any{"type": "string", "enum": []any{"approve", "reject"}}},
	}
}

func checkpointWith(key string, value any) graph.Config {
	result := cloneCheckpointConfig(checkpointConfig())
	result[key] = value
	return result
}

func checkpointWithTwo(first string, firstValue any, second string, secondValue any) graph.Config {
	result := checkpointWith(first, firstValue)
	result[second] = secondValue
	return result
}

func repeatedOptions(count int) []any {
	result := make([]any, 0, count)
	for index := range count {
		result = append(result, map[string]any{"id": "option-" + strings.Repeat("x", index+1), "label": "Option"})
	}
	return result
}

func repeatedEscalations(count int) []any {
	result := make([]any, 0, count)
	for index := range count {
		result = append(result, map[string]any{"after": (time.Duration(index+1) * time.Hour).String(), "subject": map[string]any{"kind": "notification", "reference": "team"}})
	}
	return result
}

func cloneCheckpointConfig(input graph.Config) graph.Config {
	encoded, _ := json.Marshal(input)
	var result graph.Config
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	_ = decoder.Decode(&result)
	return result
}

func checkpointPrepared(config graph.Config) stepkind.PreparedInvocation {
	return stepkind.PreparedInvocation{Invocation: stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-checkpoint", NodeID: "release-approval", Iteration: "item-1", Attempt: 2},
		Config:   config, Inputs: values.ValueSet{},
	}}
}

func checkpointResume(t *testing.T, input any, redaction values.RedactionClass, retention values.RetentionClass) values.Value {
	t.Helper()
	result, err := values.NewInline(input, values.Metadata{
		Producer: values.Producer{Kind: "resume", Reference: "checkpoint-resume"}, MediaType: "application/json",
		Redaction: redaction, Retention: retention,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func checkpointContinuation(t *testing.T, waitResult *stepkind.WaitResult, payload values.Value) *stepkind.WaitContinuation {
	t.Helper()
	continuation := checkpointContinuationUnchecked(t, waitResult, payload)
	if err := continuation.Validate(); err != nil {
		t.Fatal(err)
	}
	return continuation
}

func checkpointContinuationUnchecked(t *testing.T, waitResult *stepkind.WaitResult, payload values.Value) *stepkind.WaitContinuation {
	t.Helper()
	set := values.ValueSet{"resume": payload}
	ref, err := values.NewValueSetRef("checkpoint-resume-values", set)
	if err != nil {
		t.Fatal(err)
	}
	record := waitResult.Record
	record.Status = workflowwait.StatusResumed
	record.ResumeValues = &ref
	record.Resolution = &workflowwait.Resolution{
		Source: workflowwait.WakeGate, Responder: workflowwait.Responder{Kind: "user", Reference: "reviewer-1"},
		PayloadDigest: ref.Digest, IdempotencyKey: "checkpoint-decision-1", ResolvedAt: checkpointTime.Add(time.Minute),
	}
	return &stepkind.WaitContinuation{ID: waitResult.ID, Record: record, Values: set}
}
