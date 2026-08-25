package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/adapters/agent"
	waitadapter "github.com/hollis-labs/hadron/workflow/adapters/wait"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

type durableHost struct {
	mu           sync.Mutex
	launches     map[string]hostLaunch
	observations map[string]agent.SessionObservation
	cancel       func(context.Context) error
	heartbeat    func(context.Context) error
	launchHook   func(context.Context)
	afterApply   error
	created      int
}

type hostLaunch struct {
	digest string
	result agent.LaunchResult
}

func newDurableHost() *durableHost {
	return &durableHost{launches: map[string]hostLaunch{}, observations: map[string]agent.SessionObservation{}}
}

func (h *durableHost) LaunchSession(ctx context.Context, request agent.LaunchRequest) (agent.LaunchResult, error) {
	if h.launchHook != nil {
		h.launchHook(ctx)
	}
	digest, err := request.Digest()
	if err != nil {
		return agent.LaunchResult{}, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if prior, ok := h.launches[request.IdempotencyKey]; ok {
		if prior.digest != digest {
			return agent.LaunchResult{}, agent.ErrLaunchConflict
		}
		result := prior.result
		result.Outcome = agent.LaunchReplayed
		return result, nil
	}
	h.created++
	result := agent.LaunchResult{
		Outcome: agent.LaunchApplied,
		Ref: agent.SessionRef{ID: "session-" + request.LaunchID, Substrate: request.Substrate,
			Correlation: request.Correlation, RequestDigest: digest},
		Handle: agent.SessionHandle{SessionID: "session-" + request.LaunchID, SessionURI: "agent://session/" + request.LaunchID,
			Mailbox: "mailbox://agent/" + request.LaunchID, Substrate: request.Substrate, Correlation: request.Correlation},
	}
	h.launches[request.IdempotencyKey] = hostLaunch{digest: digest, result: result}
	if h.afterApply != nil {
		err := h.afterApply
		h.afterApply = nil
		return agent.LaunchResult{}, err
	}
	return result, nil
}

func (h *durableHost) ObserveSession(_ context.Context, ref agent.SessionRef) (agent.SessionObservation, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.observations[ref.ID], nil
}

func (h *durableHost) HeartbeatSession(ctx context.Context, _ agent.SessionRef) error {
	if h.heartbeat != nil {
		return h.heartbeat(ctx)
	}
	return nil
}

func (h *durableHost) CancelSession(ctx context.Context, _ agent.SessionRef) error {
	if h.cancel != nil {
		return h.cancel(ctx)
	}
	return nil
}

func TestAgentSessionSpecRegistrationAndConfigValidation(t *testing.T) {
	host := newDurableHost()
	registry := stepkind.NewRegistry()
	kind, err := agent.Register(registry, agent.Options{Host: host})
	if err != nil {
		t.Fatal(err)
	}
	resolved, spec, err := stepkind.Resolve(registry, agent.KindName, agent.KindVersion)
	if err != nil || resolved != kind {
		t.Fatalf("Resolve = %#v, %#v, %v", resolved, spec, err)
	}
	if spec.EmbeddedModeSupported || spec.CanSuspend || spec.Idempotency != graph.IdempotencyKeyed ||
		spec.RetrySafety != stepkind.RetryRequiresIdempotency || spec.Cancellation.Mode != stepkind.CancellationExplicit ||
		spec.Observation.Mode != stepkind.ObservationPoll || !spec.Observation.Heartbeat {
		t.Fatalf("Spec = %#v", spec)
	}
	for _, test := range []struct {
		name   string
		config graph.Config
		path   string
	}{
		{"missing", graph.Config{}, "config.substrate"},
		{"optional wrong type", validConfig(json.Number("1")), "config.prompt"},
		{"unknown", graph.Config{"substrate": "local", "launch_id": "a", "logical_agent_id": "b", "token": "secret"}, "config.token"},
	} {
		t.Run(test.name, func(t *testing.T) {
			findings := kind.ValidateConfig(t.Context(), test.config)
			if len(findings) != 1 || !strings.Contains(findings[0].Message, test.path) {
				t.Fatalf("findings = %#v", findings)
			}
		})
	}
	var nilHost *durableHost
	if _, err := agent.New(agent.Options{Host: nilHost}); !errors.Is(err, agent.ErrInvalidOptions) {
		t.Fatalf("New(typed nil) = %v", err)
	}
}

func TestAgentSessionLaunchReplayCorrelationAndRecovery(t *testing.T) {
	host := newDurableHost()
	kind, err := agent.New(agent.Options{Host: host})
	if err != nil {
		t.Fatal(err)
	}
	correlation := inline(t, "agent:parent-run:launch", values.RedactionPrivate, values.RetentionRun)
	invocation := stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "child-run", NodeID: "session", Attempt: 1},
		Config:   validConfig(nil), Inputs: values.ValueSet{agent.ParentCorrelationInput: correlation}, IdempotencyKey: "launch-key",
	}
	first, err := kind.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation})
	if err != nil || first.Outcome != stepkind.StepExternal || first.External == nil {
		t.Fatalf("Execute = %#v, %v", first, err)
	}
	if first.External.Metadata["correlation"] != "agent:parent-run:launch" ||
		strings.Contains(mustJSON(t, first.External), "prompt") || strings.Contains(mustJSON(t, first.External), "typed-secret") {
		t.Fatalf("durable external ref = %#v", first.External)
	}
	second, err := kind.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation})
	if err != nil || !reflect.DeepEqual(first, second) || host.created != 1 {
		t.Fatalf("replay = %#v, %v; created=%d", second, err, host.created)
	}

	// A new adapter process can recover solely from the durable ref when its
	// injected host is durable; no adapter-local launch state is required.
	restarted, _ := agent.New(agent.Options{Host: host})
	host.observations[first.External.ID] = agent.SessionObservation{State: agent.SessionPending, Progress: map[string]string{"heartbeat": "healthy"}}
	observation, err := restarted.Observe(t.Context(), *first.External)
	if err != nil || observation.State != stepkind.ObservationPending || observation.Progress["heartbeat"] != "healthy" {
		t.Fatalf("recovered Observe = %#v, %v", observation, err)
	}

	changed := invocation
	changed.Config = graph.Config{"substrate": "local", "launch_id": "different", "logical_agent_id": "worker", "prompt": "hello", "idempotency_key": "launch-key"}
	_, err = kind.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: changed})
	var executionError *stepkind.ExecutionError
	if !errors.Is(err, agent.ErrLaunchConflict) || !errors.As(err, &executionError) || executionError.Code != agent.CodeLaunchConflict {
		t.Fatalf("changed replay = %T %v", err, err)
	}
	invalidIdentity := invocation
	invalidIdentity.Identity.NodeID = "Bad Node"
	invalidIdentity.IdempotencyKey = "invalid-node"
	invalidIdentity.Config["idempotency_key"] = "invalid-node"
	if _, err := kind.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invalidIdentity}); err == nil {
		t.Fatal("non-normalized logical node identity accepted")
	}
	if err := (agent.LogicalIdentity{RunID: "run", NodeID: "Bad Node"}).Validate(); err == nil {
		t.Fatal("LogicalIdentity accepted non-normalized NodeID")
	}
}

func TestAgentSessionAmbiguousLaunchRetryConvergesWithoutDuplicate(t *testing.T) {
	host := newDurableHost()
	host.afterApply = &stepkind.ExecutionError{Code: "host_ambiguous", Message: "host response was lost", Classification: stepkind.Retryable}
	kind, _ := agent.New(agent.Options{Host: host})
	invocation := validInvocation(t)
	if _, err := kind.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation}); err == nil || stepkind.ClassifyError(err) != stepkind.Retryable {
		t.Fatalf("ambiguous launch = %T %v", err, err)
	}
	replayed, err := kind.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation})
	if err != nil || replayed.Outcome != stepkind.StepExternal || host.created != 1 {
		t.Fatalf("retry = %#v, %v; created=%d", replayed, err, host.created)
	}

	ctx, cancel := context.WithCancel(t.Context())
	host.launchHook = func(context.Context) { cancel() }
	changed := invocation
	changed.IdempotencyKey = "second-key"
	changed.Config = validConfig(nil)
	changed.Config["idempotency_key"] = "second-key"
	if _, err := kind.Execute(ctx, stepkind.PreparedInvocation{Invocation: changed}); !errors.Is(err, context.Canceled) {
		t.Fatalf("late launch cancellation = %v", err)
	}
}

func TestAgentSessionTerminalValuesFailuresAndLifecycleCancellation(t *testing.T) {
	host := newDurableHost()
	kind, _ := agent.New(agent.Options{Host: host})
	result, err := kind.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: validInvocation(t)})
	if err != nil {
		t.Fatal(err)
	}
	exact := inline(t, json.Number("9007199254740993.0001"), values.RedactionPublic, values.RetentionNone)
	host.observations[result.External.ID] = agent.SessionObservation{
		State: agent.SessionSucceeded,
		Handle: agent.SessionHandle{SessionID: result.External.ID, SessionURI: "agent://session/main", Mailbox: "mailbox://agent/main",
			Substrate: "local", Correlation: result.External.Metadata["correlation"]},
		Result: &exact,
	}
	observed, err := kind.Observe(t.Context(), *result.External)
	if err != nil || observed.State != stepkind.ObservationSucceeded || observed.Result == nil {
		t.Fatalf("Observe = %#v, %v", observed, err)
	}
	output := observed.Result.Outputs[agent.OutputResult]
	if number, ok := output.Inline.(json.Number); !ok || number.String() != "9007199254740993.0001" ||
		output.Redaction != values.RedactionPrivate || output.Retention != values.RetentionRun {
		t.Fatalf("result output = %#v", output)
	}
	if output.Producer.Reference == "" || !strings.Contains(observed.Result.Outputs[agent.OutputHandle].Producer.Reference, "attempt-1") {
		t.Fatalf("output provenance = %#v", observed.Result.Outputs)
	}

	safeCause := errors.New("process-local raw provider failure")
	host.observations[result.External.ID] = agent.SessionObservation{State: agent.SessionFailed, Failure: &agent.SessionFailure{
		Code: "agent_provider_failed", Message: "agent provider failed", Retryable: true, Cause: safeCause,
	}}
	failed, err := kind.Observe(t.Context(), *result.External)
	if err != nil || failed.Failure == nil || !errors.Is(failed.Failure, safeCause) || strings.Contains(failed.Failure.Error(), safeCause.Error()) {
		t.Fatalf("failed observation = %#v, %v", failed, err)
	}

	for name, operation := range map[string]func(context.Context) error{
		"heartbeat": func(ctx context.Context) error { return kind.Heartbeat(ctx, *result.External) },
		"cancel":    func(ctx context.Context) error { return kind.Cancel(ctx, *result.External) },
	} {
		t.Run(name+" canceled after collaborator", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			if name == "heartbeat" {
				host.heartbeat = func(context.Context) error { cancel(); return nil }
			} else {
				host.cancel = func(context.Context) error { cancel(); return nil }
			}
			if err := operation(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("operation = %v", err)
			}
		})
	}
	var nilKind *agent.Kind
	if _, err := nilKind.Observe(t.Context(), *result.External); err == nil {
		t.Fatal("typed-nil Observe succeeded")
	}
	if err := nilKind.Heartbeat(t.Context(), *result.External); err == nil {
		t.Fatal("typed-nil Heartbeat succeeded")
	}
	if err := nilKind.Cancel(t.Context(), *result.External); err == nil {
		t.Fatal("typed-nil Cancel succeeded")
	}

	for _, state := range []agent.SessionState{agent.SessionPending, agent.SessionFailed} {
		observation := agent.SessionObservation{State: state, Handle: agent.SessionHandle{
			SessionID: "different", Substrate: "local", Correlation: result.External.Metadata["correlation"],
		}}
		if state == agent.SessionFailed {
			observation.Failure = &agent.SessionFailure{Code: "failed", Message: "agent session failed"}
		}
		host.observations[result.External.ID] = observation
		if _, err := kind.Observe(t.Context(), *result.External); err == nil {
			t.Fatalf("%s mismatched handle accepted", state)
		}
	}
}

func TestComposeIsDeterministicParentScopedAndUsesOrdinaryCallWait(t *testing.T) {
	config := validConfig(nil)
	delete(config, "idempotency_key")
	request := agent.CompositionRequest{
		NodeID: "launch-agent", DisplayName: "Launch agent", Config: config,
		ParentClose: graph.ParentCloseRequestCancel, Wait: &agent.WaitPolicy{Timeout: graph.Duration("15m")},
		Retry: &graph.RetryPolicy{Attempts: 3, On: []string{"transient"}}, Timeout: &graph.TimeoutPolicy{
			Execution: graph.Duration("1h"), Heartbeat: graph.Duration("2m"),
		},
	}
	first, err := agent.Compose(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := agent.Compose(request)
	if err != nil || mustJSON(t, first) != mustJSON(t, second) {
		t.Fatalf("Compose deterministic = %v", err)
	}
	if first.Launch.ID != "launch-agent-launch" || first.Wait.ID != "launch-agent" || first.Launch.Call == nil || first.Launch.Call.Mode != graph.CallRun || first.Launch.Call.OnParentClose != graph.ParentCloseRequestCancel || first.Wait == nil {
		t.Fatalf("composition = %#v", first)
	}
	parentBinding := first.Launch.InputBindings[agent.ParentCorrelationInput]
	if parentBinding.Kind != graph.BindingInterpolation || parentBinding.Interpolation != "agent:{{ run.id }}:launch-agent" {
		t.Fatalf("parent-scoped binding = %#v", parentBinding)
	}
	waitBinding := first.Wait.InputBindings["run-id"]
	if waitBinding.Expression == nil || waitBinding.Expression.Text != `steps["launch-agent-launch"].outputs["run-id"]` ||
		first.Wait.Config["child_run"].(map[string]any)["input"] != "run-id" || len(first.Edges) != 1 || first.Edges[0].Kind != graph.EdgeData {
		t.Fatalf("wait handoff = %#v / %#v", first.Wait, first.Edges)
	}
	payloadSchema, ok := first.Wait.Config["payload_schema"].(map[string]any)
	childRunConfig := first.Wait.Config["child_run"].(map[string]any)
	if !ok || payloadSchema["type"] != "object" || childRunConfig["fail_on_unsuccessful"] != true || !reflect.DeepEqual(first.Wait.Outputs[0].Schema, graph.Schema(payloadSchema)) {
		t.Fatalf("typed child completion schema = %#v / %#v", first.Wait.Config["payload_schema"], first.Wait.Outputs[0].Schema)
	}
	if got := outputNames(first.Launch.Outputs); !reflect.DeepEqual(got, []string{"run-id", "status", "events-ref", "cancellation", "outputs-ref"}) {
		t.Fatalf("wait-mode launch outputs = %#v", got)
	}
	if got := outputNames(first.Wait.Outputs); !reflect.DeepEqual(got, []string{"payload", "resume", "timed_out"}) {
		t.Fatalf("wait-mode authored outputs = %#v", got)
	}
	if first.Definition.Graph.Inputs[0].Name != agent.ParentCorrelationInput ||
		first.Definition.Graph.Nodes[0].Kind != agent.KindName || first.Definition.Graph.Nodes[0].InputBindings[agent.ParentCorrelationInput].Expression.Text != `inputs["parent-correlation"]` {
		t.Fatalf("generated child = %#v", first.Definition.Graph)
	}
	if first.Definition.Graph.Nodes[0].Timeout == nil || first.Definition.Graph.Nodes[0].Timeout.Heartbeat != "2m" ||
		first.Definition.Graph.Nodes[0].Retry == nil || first.Definition.Graph.Nodes[0].Retry.Attempts != 3 {
		t.Fatalf("generated lifecycle policy = %#v / %#v", first.Definition.Graph.Nodes[0].Timeout, first.Definition.Graph.Nodes[0].Retry)
	}
	timeoutBase := time.Date(2026, 8, 24, 9, 0, 0, 0, time.UTC)
	timeoutEvaluation, timeoutErr := workflowruntime.EvaluateTimeouts(first.Definition.Graph.Nodes[0].Timeout, workflowruntime.TimeoutAnchor{
		ScheduledAt: timeoutBase, StartedAt: timeoutBase, ExternalAt: timeoutBase,
	}, timeoutBase.Add(3*time.Minute))
	if timeoutErr != nil || timeoutEvaluation.Due == nil || timeoutEvaluation.Due.Kind != workflowruntime.TimeoutHeartbeat {
		t.Fatalf("ordinary runtime heartbeat timeout = %#v, %v", timeoutEvaluation, timeoutErr)
	}
	mutated := first
	mutated.Definition.Graph.Nodes[0].Config["substrate"] = "mutated"
	third, _ := agent.Compose(request)
	if third.Definition.Graph.Nodes[0].Config["substrate"] != "local" {
		t.Fatal("Compose leaked mutable config")
	}

	engine := values.NewExpressionEngine()
	metadata := values.Metadata{Producer: values.Producer{Kind: "test", Reference: "test/evaluation"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
	correlation, err := engine.EvaluateBinding(parentBinding, values.ExpressionContext{Run: map[string]any{"id": "parent-run"}}, values.ExpressionOptions{}, metadata)
	if err != nil || correlation.Inline != "agent:parent-run:launch-agent" {
		t.Fatalf("parent binding evaluation = %#v, %v", correlation, err)
	}
	runID := inline(t, "child-run-123", values.RedactionPrivate, values.RetentionRun)
	boundRunID, err := engine.EvaluateBinding(waitBinding, values.ExpressionContext{Steps: map[string]values.StepContext{
		"launch-agent-launch": {Status: "succeeded", Outputs: values.ValueSet{"run-id": runID}},
	}}, values.ExpressionOptions{}, metadata)
	if err != nil || boundRunID.Inline != "child-run-123" {
		t.Fatalf("wait binding evaluation = %#v, %v", boundRunID, err)
	}

	registry := stepkind.NewRegistry()
	for _, registered := range []stepkind.StepKind{
		stepkindtest.NewNoopKind("call", "v1"),
		stepkindtest.NewNoopKind(agent.KindName, agent.KindVersion),
		stepkindtest.NewNoopKind(waitadapter.WaitForName, waitadapter.Version),
	} {
		if err := registry.Register(registered); err != nil {
			t.Fatal(err)
		}
	}
	parent := graph.Graph{ID: "parent", Version: "v1", Nodes: []graph.Node{first.Launch, *first.Wait}, Edges: first.Edges}
	findings := workflowcompile.ValidateGraph(t.Context(), parent, workflowcompile.ValidationOptions{
		StepKinds: registry,
		Definitions: workflowcompile.DefinitionResolverFunc(func(_ context.Context, ref graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
			if ref.Digest != first.Definition.Definition.Digest {
				return workflowcompile.ResolvedDefinition{}, errors.New("unexpected definition")
			}
			return first.Definition, nil
		}),
	})
	if len(findings) != 0 {
		t.Fatalf("ValidateGraph(composition) = %#v", findings)
	}
	withoutDisplay := request
	withoutDisplay.DisplayName = ""
	fallback, fallbackErr := agent.Compose(withoutDisplay)
	if fallbackErr != nil || fallback.Wait.DisplayName != "launch-agent result" {
		t.Fatalf("wait display-name fallback = %#v, %v", fallback.Wait, fallbackErr)
	}
	for _, reserved := range []string{"correlation", "idempotency_key"} {
		invalid := request
		invalid.Config = graph.Config{"substrate": "local", "launch_id": "main", "logical_agent_id": "worker", reserved: "author-owned"}
		if _, err := agent.Compose(invalid); err == nil || !strings.Contains(err.Error(), "reserves config."+reserved) {
			t.Fatalf("reserved %s = %v", reserved, err)
		}
	}

	for _, policy := range []graph.ParentClosePolicy{graph.ParentCloseCancel, graph.ParentCloseAbandon, graph.ParentCloseRequestCancel} {
		request.ParentClose, request.Wait = policy, nil
		composition, composeErr := agent.Compose(request)
		if composeErr != nil || composition.Wait != nil || composition.Launch.ID != "launch-agent" || composition.Launch.Call.OnParentClose != policy ||
			!reflect.DeepEqual(outputNames(composition.Launch.Outputs), []string{"run-id", "status", "events-ref", "cancellation", "outputs-ref"}) {
			t.Fatalf("policy %q = %#v, %v", policy, composition, composeErr)
		}
	}
}

func TestComposedWaitDispatchesThroughTypedCallOutputAndDurableChildRunWait(t *testing.T) {
	composition := compiledDispatchComposition(t)

	registry := stepkind.NewRegistry()
	callKind := stepkindtest.NewNoopKind("call", "v1")
	callKind.SpecValue.ConfigSchema = graph.Schema{"type": "object", "additionalProperties": false}
	callKind.SpecValue.InputSchema = graph.Schema{"type": "object", "required": []any{agent.ParentCorrelationInput}, "properties": map[string]any{
		agent.ParentCorrelationInput: map[string]any{"type": "string"},
	}, "additionalProperties": false}
	callKind.SpecValue.OutputSchema = graph.Schema{"type": "object", "required": []any{"run-id", "status", "events-ref", "cancellation", "outputs-ref"}, "properties": map[string]any{
		"run-id": map[string]any{"type": "string"}, "status": map[string]any{"type": "string"}, "events-ref": map[string]any{"type": "string"},
		"cancellation": map[string]any{"type": "object"}, "outputs-ref": map[string]any{"type": []any{"object", "null"}},
	}, "additionalProperties": false}
	callKind.ExecuteFunc = func(_ context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		if prepared.Invocation.Inputs[agent.ParentCorrelationInput].Inline != "agent:parent-run:launch-agent" ||
			prepared.Invocation.Call == nil || prepared.Invocation.Call.Spec.OnParentClose != graph.ParentCloseCancel {
			t.Fatalf("call invocation = %#v", prepared.Invocation)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{
			"run-id": inline(t, "child-run-123", values.RedactionPrivate, values.RetentionRun), "status": inline(t, "started", values.RedactionPrivate, values.RetentionRun),
			"events-ref": inline(t, "events://child-run-123", values.RedactionPrivate, values.RetentionRun), "cancellation": inline(t, map[string]any{"policy": "cancel"}, values.RedactionPrivate, values.RetentionRun),
			"outputs-ref": inline(t, nil, values.RedactionPrivate, values.RetentionRun),
		}}, nil
	}
	if registerErr := registry.Register(callKind); registerErr != nil {
		t.Fatal(registerErr)
	}
	var authorized waitadapter.AuthorityRequest
	waitKind, err := waitadapter.NewWaitFor(waitadapter.Options{
		Authority: waitadapter.AuthorityResolverFunc(func(_ context.Context, request waitadapter.AuthorityRequest) (workflowwait.ResponderAuthority, error) {
			authorized = request
			return workflowwait.ResponderAuthority{Kind: "child_run", Reference: request.Source.Reference}, nil
		}),
		Now: func() time.Time { return time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if registerErr := registry.Register(waitKind); registerErr != nil {
		t.Fatal(registerErr)
	}

	store := inmemory.NewStore()
	now := time.Date(2026, 8, 24, 11, 59, 0, 0, time.UTC)
	if _, _, createErr := store.CreateRun(t.Context(), workflowruntime.CreateRunRequest{
		ID: "parent-run", Plan: testPlanRef(), Status: workflowruntime.RunPending,
		StartIdempotencyKey: "start-parent", CreatedAt: now,
	}); createErr != nil {
		t.Fatal(createErr)
	}
	engine := values.NewExpressionEngine()
	metadata := values.Metadata{Producer: values.Producer{Kind: "binding", Reference: "parent-run/launch-agent"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
	parentCorrelation, err := engine.EvaluateBinding(composition.Launch.InputBindings[agent.ParentCorrelationInput], values.ExpressionContext{Run: map[string]any{"id": "parent-run"}}, values.ExpressionOptions{}, metadata)
	if err != nil {
		t.Fatal(err)
	}
	callClaim := createReadyClaim(t, store, "parent-run", composition.Launch.ID, values.ValueSet{agent.ParentCorrelationInput: parentCorrelation}, now, "call")
	dispatchNow := now.Add(2 * time.Second)
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return dispatchNow }})
	if err != nil {
		t.Fatal(err)
	}
	callResult, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{
		Claim: callClaim, Node: composition.Launch, IdempotencyKey: "call-idempotency",
		CallLineage: []graph.DefinitionRef{{Kind: "workflow", ID: "parent", Version: "v1", Digest: values.SHA256Digest([]byte("parent"))}},
	})
	if err != nil || callResult.Node.Status != workflowruntime.NodeSucceeded || callResult.Outputs == nil {
		t.Fatalf("Dispatch(call) = %#v, %v", callResult, err)
	}
	callOutputs, err := store.LoadValues(t.Context(), *callResult.Outputs)
	if err != nil {
		t.Fatal(err)
	}
	waitInput, err := engine.EvaluateBinding(composition.Wait.InputBindings["run-id"], values.ExpressionContext{Steps: map[string]values.StepContext{
		composition.Launch.ID: {Status: "succeeded", Outputs: callOutputs},
	}}, values.ExpressionOptions{}, metadata)
	if err != nil || waitInput.Inline != "child-run-123" {
		t.Fatalf("wait binding = %#v, %v", waitInput, err)
	}
	waitClaim := createReadyClaim(t, store, "parent-run", composition.Wait.ID, values.ValueSet{"run-id": waitInput}, now.Add(3*time.Second), "wait")
	dispatchNow = now.Add(5 * time.Second)
	waitResult, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: waitClaim, Node: *composition.Wait, IdempotencyKey: "wait-idempotency"})
	if err != nil || waitResult.Node.Status != workflowruntime.NodeWaiting || waitResult.Wait == nil || authorized.Source.Reference != "child-run-123" || authorized.Correlation != "child-run-123" {
		t.Fatalf("Dispatch(wait) = %#v, authority=%#v, %v", waitResult, authorized, err)
	}
	childOutputs := map[string]any{
		"handle": map[string]any{"session_id": "session-main", "session_uri": nil, "mailbox": nil, "substrate": "local", "correlation": "agent:parent-run:launch-agent"},
		"status": "succeeded", "result": "complete",
	}
	terminalPayload := inline(t, map[string]any{
		"status": "succeeded", "outputs": childOutputs,
	}, values.RedactionPrivate, values.RetentionRun)
	resumed, err := (workflowruntime.WaitCoordinator{Store: store}).Resume(t.Context(), workflowruntime.ResumeCommand{
		WaitID: waitResult.Wait.Ref.ID, Correlation: "child-run-123", WakeSource: workflowwait.WakeChildRun,
		Responder: workflowwait.Responder{Kind: "child_run", Reference: "child-run-123"}, Payload: terminalPayload,
		IdempotencyKey: "child-completed", ReceivedAt: now.Add(6 * time.Second),
	})
	if err != nil || resumed.Node.Status != workflowruntime.NodeReady {
		t.Fatalf("Resume(child) = %#v, %v", resumed, err)
	}
	queue := workflowruntime.NewReadyQueueCoordinator(store, nil)
	continuedClaim, ok, err := queue.ClaimNext(t.Context(), workflowruntime.ReadyClaimRequest{
		RunID: "parent-run", Owner: "worker-continued", Token: "continued-token", IdempotencyKey: "claim-continued",
		Now: now.Add(7 * time.Second), LeaseUntil: now.Add(time.Hour),
	})
	if err != nil || !ok {
		t.Fatalf("ClaimNext(continued) = %#v, %v, %v", continuedClaim, ok, err)
	}
	dispatchNow = now.Add(8 * time.Second)
	completed, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: continuedClaim, Node: *composition.Wait, IdempotencyKey: "wait-idempotency"})
	if err != nil || completed.Node.Status != workflowruntime.NodeSucceeded || completed.Outputs == nil {
		t.Fatalf("Dispatch(continued) = %#v, %v", completed, err)
	}
	outputs, err := store.LoadValues(t.Context(), *completed.Outputs)
	if err != nil || outputs["timed_out"].Inline != false {
		t.Fatalf("continued outputs = %#v, %v", outputs, err)
	}
	completion := outputs["payload"].Inline.(map[string]any)
	if completion["status"] != "succeeded" || completion["result"] != "complete" {
		t.Fatalf("typed completion = %#v", completion)
	}
}

func compiledDispatchComposition(t *testing.T) agent.Composition {
	t.Helper()
	const source = `workflow: {id: agent-dispatch, version: v1}
steps:
  - id: launch-agent
    timeout: {wait: 15m}
    agent_launch:
      substrate: local
      logical_agent_id: worker
      launch_id: main
      parent_close: cancel
      wait: true
`
	plan := compileAgentSource(t, "agent-dispatch.workflow.yaml", source)
	launch := nodeByID(t, plan.Graph, "launch-agent-launch")
	wait := nodeByID(t, plan.Graph, "launch-agent")
	return agent.Composition{Definition: plan.BundledDefinitions[0], Launch: launch, Wait: &wait, Edges: append([]graph.Edge(nil), plan.Graph.Edges...)}
}

func TestComposedChildTerminalFailuresDispatchAsOrdinaryNodeFailure(t *testing.T) {
	for _, status := range []string{"failed", "canceled", "timed_out"} {
		t.Run(status, func(t *testing.T) {
			config := validConfig(nil)
			delete(config, "idempotency_key")
			composition, err := agent.Compose(agent.CompositionRequest{
				NodeID: "agent-result", Config: config, Wait: &agent.WaitPolicy{Timeout: graph.Duration("15m")},
			})
			if err != nil {
				t.Fatal(err)
			}
			registry := stepkind.NewRegistry()
			waitKind, err := waitadapter.NewWaitFor(waitadapter.Options{
				Authority: waitadapter.AuthorityResolverFunc(func(_ context.Context, request waitadapter.AuthorityRequest) (workflowwait.ResponderAuthority, error) {
					return workflowwait.ResponderAuthority{Kind: "child_run", Reference: request.Source.Reference}, nil
				}),
				Now: func() time.Time { return time.Date(2026, 8, 24, 13, 0, 0, 0, time.UTC) },
			})
			if err != nil {
				t.Fatal(err)
			}
			if registerErr := registry.Register(waitKind); registerErr != nil {
				t.Fatal(registerErr)
			}
			store := inmemory.NewStore()
			runID := workflowruntime.RunID("parent-" + status)
			now := time.Date(2026, 8, 24, 12, 59, 0, 0, time.UTC)
			if _, _, createErr := store.CreateRun(t.Context(), workflowruntime.CreateRunRequest{
				ID: runID, Plan: testPlanRef(), Status: workflowruntime.RunPending, StartIdempotencyKey: "start-" + status, CreatedAt: now,
			}); createErr != nil {
				t.Fatal(createErr)
			}
			claim := createReadyClaim(t, store, runID, composition.Wait.ID, values.ValueSet{"run-id": inline(t, "child-"+status, values.RedactionPrivate, values.RetentionRun)}, now, "initial-"+status)
			dispatchNow := now.Add(3 * time.Second)
			dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return dispatchNow }})
			if err != nil {
				t.Fatal(err)
			}
			waiting, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: *composition.Wait, IdempotencyKey: "wait-" + status})
			if err != nil || waiting.Node.Status != workflowruntime.NodeWaiting || waiting.Wait == nil {
				t.Fatalf("initial Dispatch = %#v, %v", waiting, err)
			}
			terminal := inline(t, map[string]any{
				"status": status, "failure": map[string]any{"code": "child_" + status, "message": "child run ended"},
			}, values.RedactionPrivate, values.RetentionRun)
			resumed, err := (workflowruntime.WaitCoordinator{Store: store}).Resume(t.Context(), workflowruntime.ResumeCommand{
				WaitID: waiting.Wait.Ref.ID, Correlation: "child-" + status, WakeSource: workflowwait.WakeChildRun,
				Responder: workflowwait.Responder{Kind: "child_run", Reference: "child-" + status}, Payload: terminal,
				IdempotencyKey: "terminal-" + status, ReceivedAt: now.Add(4 * time.Second),
			})
			if err != nil || resumed.Node.Status != workflowruntime.NodeReady {
				t.Fatalf("Resume = %#v, %v", resumed, err)
			}
			queue := workflowruntime.NewReadyQueueCoordinator(store, nil)
			continued, ok, err := queue.ClaimNext(t.Context(), workflowruntime.ReadyClaimRequest{
				RunID: runID, Owner: "continued", Token: "continued-" + status, IdempotencyKey: "claim-terminal-" + status,
				Now: now.Add(5 * time.Second), LeaseUntil: now.Add(time.Hour),
			})
			if err != nil || !ok {
				t.Fatalf("ClaimNext = %#v, %v, %v", continued, ok, err)
			}
			dispatchNow = now.Add(6 * time.Second)
			failed, dispatchErr := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: continued, Node: *composition.Wait, IdempotencyKey: "wait-" + status})
			if dispatchErr == nil || failed.Node.Status != workflowruntime.NodeFailed || failed.Attempt.Status != workflowruntime.NodeFailed ||
				failed.Attempt.Failure == nil || failed.Attempt.Failure.Code != waitadapter.CodeChildRunFailed {
				t.Fatalf("terminal Dispatch = %#v, %v", failed, dispatchErr)
			}
		})
	}
}

func validConfig(prompt any) graph.Config {
	config := graph.Config{"substrate": "local", "launch_id": "main", "logical_agent_id": "worker", "idempotency_key": "launch-key"}
	if prompt != nil {
		config["prompt"] = prompt
	} else {
		config["prompt"] = "hello"
	}
	return config
}

func validInvocation(t *testing.T) stepkind.Invocation {
	t.Helper()
	return stepkind.Invocation{Identity: stepkind.InvocationIdentity{RunID: "child-run", NodeID: "session", Attempt: 1}, Config: validConfig(nil), Inputs: values.ValueSet{}, IdempotencyKey: "launch-key"}
}

func inline(t *testing.T, payload any, redaction values.RedactionClass, retention values.RetentionClass) values.Value {
	t.Helper()
	value, err := values.NewInline(payload, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "test/input"}, MediaType: "application/json", Redaction: redaction, Retention: retention})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mustJSON(t *testing.T, input any) string {
	t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func outputNames(outputs []graph.OutputSpec) []string {
	names := make([]string, len(outputs))
	for index := range outputs {
		names[index] = outputs[index].Name
	}
	return names
}

func testPlanRef() workflowruntime.PlanRef {
	return workflowruntime.PlanRef{ID: "plan", Version: "v1", Digest: values.SHA256Digest([]byte("agent-plan")), SchemaVersion: "v1"}
}

func createReadyClaim(t *testing.T, store *inmemory.Store, runID workflowruntime.RunID, nodeID string, inputs values.ValueSet, now time.Time, suffix string) workflowruntime.ReadyClaim {
	t.Helper()
	inputRef, err := store.SaveValues(t.Context(), workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "node-inputs", RunID: runID}, Values: inputs})
	if err != nil {
		t.Fatal(err)
	}
	id := workflowruntime.NodeInvocationID{RunID: runID, NodeID: nodeID}
	created, err := store.CreateNodeInvocation(t.Context(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID: id, Status: workflowruntime.NodePending, Inputs: &inputRef, CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, transitionErr := store.TransitionNode(t.Context(), workflowruntime.NodeTransitionRequest{InvocationID: id, ExpectedGeneration: created.Generation, To: workflowruntime.NodeReady, At: now.Add(time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	queue := workflowruntime.NewReadyQueueCoordinator(store, nil)
	claim, ok, err := queue.ClaimNext(t.Context(), workflowruntime.ReadyClaimRequest{
		RunID: runID, Owner: "worker-" + suffix, Token: "token-" + suffix, IdempotencyKey: "claim-" + suffix,
		Now: now.Add(2 * time.Second), LeaseUntil: now.Add(time.Hour),
	})
	if err != nil || !ok || claim.Candidate.InvocationID != id {
		t.Fatalf("ClaimNext(%s) = %#v, %v, %v", nodeID, claim, ok, err)
	}
	return claim
}
