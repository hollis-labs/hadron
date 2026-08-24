package call_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
	"github.com/hollis-labs/hadron/workflow/adapters/call/calltest"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestRegisterAndSpecExposeConservativeCallContract(t *testing.T) {
	executor := newTestExecutor(t, resolverFunc(func(context.Context, graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
		return resolvedDefinition("child", nil, nil), nil
	}), calltest.NewJournal(), inlineFunc(func(context.Context, calladapter.InlineRequest) (calladapter.InlineResult, error) {
		return calladapter.InlineResult{Outputs: values.ValueSet{}}, nil
	}), runFunc(validChildRun))
	registry := stepkind.NewRegistry()
	registered, err := calladapter.Register(registry, testOptions(executor))
	if err != nil || registered == nil {
		t.Fatalf("Register() = %v, %v", registered, err)
	}
	kind, ok := registry.Lookup(calladapter.KindName, calladapter.KindVersion)
	if !ok || kind != registered {
		t.Fatalf("Lookup() = %v, %v", kind, ok)
	}
	spec := registered.Spec()
	if spec.Name != "call" || spec.Version != "v1" || spec.Idempotency != graph.IdempotencyKeyed ||
		spec.RetrySafety != stepkind.RetryRequiresIdempotency || spec.Cancellation.Mode != stepkind.CancellationContext ||
		!spec.Lifecycle.Prepare || spec.EmbeddedModeSupported || len(spec.Effects) != 5 {
		t.Fatalf("Spec() = %#v", spec)
	}
	if findings := registered.ValidateConfig(t.Context(), nil); len(findings) != 1 {
		t.Fatalf("ValidateConfig(nil) = %#v", findings)
	}
	if findings := registered.ValidateConfig(t.Context(), graph.Config{"definition": "duplicate"}); len(findings) != 1 {
		t.Fatalf("ValidateConfig(fields) = %#v", findings)
	}
}

func TestNewRejectsTypedNilCollaborators(t *testing.T) {
	valid := calladapter.Options{
		Resolver: executorResolver(resolvedDefinition("child", nil, nil)), State: calltest.NewJournal(),
		Context: defaultContextProvider(), Inline: inlineFunc(emptyInline), Runs: runFunc(validChildRun),
	}
	var nilResolver resolverFunc
	var nilState *calltest.Journal
	var nilContext calladapter.ContextProviderFunc
	var nilInline inlineFunc
	var nilRuns runFunc
	for _, options := range []calladapter.Options{
		{Resolver: nilResolver, State: valid.State, Context: valid.Context, Inline: valid.Inline, Runs: valid.Runs},
		{Resolver: valid.Resolver, State: nilState, Context: valid.Context, Inline: valid.Inline, Runs: valid.Runs},
		{Resolver: valid.Resolver, State: valid.State, Context: nilContext, Inline: valid.Inline, Runs: valid.Runs},
		{Resolver: valid.Resolver, State: valid.State, Context: valid.Context, Inline: nilInline, Runs: valid.Runs},
		{Resolver: valid.Resolver, State: valid.State, Context: valid.Context, Inline: valid.Inline, Runs: nilRuns},
	} {
		if _, err := calladapter.New(options); !errors.Is(err, calladapter.ErrInvalidOptions) {
			t.Fatalf("New(%#v) error = %v", options, err)
		}
	}
}

func TestBindInputsAppliesDefinitionThenImportThenNodeLocalPrecedence(t *testing.T) {
	definitionDefault := graph.Binding{Kind: graph.BindingLiteral, Literal: "definition"}
	resolved := resolvedDefinition("child", []graph.InputSpec{
		{Name: "name", Required: true, Schema: graph.Schema{"type": "string"}, Default: &definitionDefault},
		{Name: "count", Required: true, Schema: graph.Schema{"type": "number"}},
	}, nil)
	resolved.InputBindings = map[string]graph.Binding{
		"name":  {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.import_name"}},
		"count": {Kind: graph.BindingLiteral, Literal: 1},
	}
	localName := inlineValue(t, "node-local", "local", values.RedactionPublic, values.RetentionProject)
	localCount := inlineValue(t, 3, "local", values.RedactionPrivate, values.RetentionRun)
	contextName := inlineValue(t, "import-default", "parent", values.RedactionPrivate, values.RetentionRun)
	result := calladapter.BindInputs(calladapter.BindInputsRequest{
		Invocation: testIdentity(1), Resolved: resolved,
		LocalInputs: values.ValueSet{"name": localName, "count": localCount},
		Context:     values.ExpressionContext{Inputs: values.ValueSet{"import_name": contextName}},
	})
	if len(result.Diagnostics) != 0 {
		t.Fatalf("BindInputs() diagnostics = %#v", result.Diagnostics)
	}
	if !reflect.DeepEqual(result.Inputs["name"], localName) || !reflect.DeepEqual(result.Inputs["count"], localCount) {
		t.Fatalf("node-local values did not win with envelopes intact: %#v", result.Inputs)
	}

	withoutLocal := calladapter.BindInputs(calladapter.BindInputsRequest{
		Invocation: testIdentity(1), Resolved: resolved,
		LocalInputs: values.ValueSet{},
		Context:     values.ExpressionContext{Inputs: values.ValueSet{"import_name": contextName}},
	})
	if len(withoutLocal.Diagnostics) != 0 || withoutLocal.Inputs["name"].Inline != "import-default" || withoutLocal.Inputs["count"].Inline == nil {
		t.Fatalf("import defaults did not override definition defaults: %#v %#v", withoutLocal.Inputs, withoutLocal.Diagnostics)
	}
	if withoutLocal.Inputs["name"].Producer != contextName.Producer || withoutLocal.Inputs["name"].Redaction != contextName.Redaction {
		t.Fatalf("exact import passthrough lost typed metadata: %#v", withoutLocal.Inputs["name"])
	}
}

func TestPreparePassesEveryDefinitionReferenceShapeLosslessly(t *testing.T) {
	digest := testDigest("requested")
	tests := []struct {
		name string
		ref  graph.DefinitionRef
	}{
		{"path", graph.DefinitionRef{Kind: "workflow", Locator: "nested/child.workflow.yaml"}},
		{"registry", graph.DefinitionRef{Authority: "registry", Kind: "workflow", ID: "child"}},
		{"version", graph.DefinitionRef{Authority: "registry", Kind: "workflow", ID: "child", Version: "v2"}},
		{"digest", graph.DefinitionRef{Authority: "registry", Kind: "workflow", ID: "child", Digest: digest}},
		{"package", graph.DefinitionRef{Authority: "package", Kind: "package", ID: "child", Locator: "pkg://tools/child", Version: "v3", Digest: digest}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var captured graph.DefinitionRef
			executor := mustExecutor(t, calladapter.Options{
				Resolver: resolverFunc(func(_ context.Context, ref graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
					captured = ref
					return resolvedDefinition("resolved", nil, nil), nil
				}),
				State: calltest.NewJournal(), Context: defaultContextProvider(),
				Inline: inlineFunc(emptyInline), Runs: runFunc(validChildRun),
			})
			invocation := testInvocation(graph.CallInline, test.ref, 1, values.ValueSet{})
			if _, err := executor.Prepare(t.Context(), invocation); err != nil {
				t.Fatalf("Prepare() error = %v", err)
			}
			if !reflect.DeepEqual(captured, test.ref) {
				t.Fatalf("resolver ref = %#v, want %#v", captured, test.ref)
			}
		})
	}
}

func TestPrepareRejectsResolverGraphDefinitionAndProvenanceDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*workflowcompile.ResolvedDefinition)
	}{
		{"graph digest", func(resolved *workflowcompile.ResolvedDefinition) {
			resolved.Graph.Digest = testDigest("different-graph")
			resolved.Graph.Provenance.Digest = resolved.Graph.Digest
		}},
		{"graph provenance digest", func(resolved *workflowcompile.ResolvedDefinition) {
			resolved.Graph.Provenance.Digest = testDigest("different-provenance")
		}},
		{"graph provenance identity", func(resolved *workflowcompile.ResolvedDefinition) {
			resolved.Graph.Provenance.Authority = "different-authority"
		}},
		{"graph identity", func(resolved *workflowcompile.ResolvedDefinition) {
			resolved.Graph.ID = "different-child"
		}},
		{"invalid provenance metadata UTF-8", func(resolved *workflowcompile.ResolvedDefinition) {
			resolved.Graph.Provenance.Metadata = graph.Metadata{string([]byte{0xff}): "invalid"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved := resolvedDefinition("child", nil, nil)
			test.mutate(&resolved)
			journal := calltest.NewJournal()
			executor := mustExecutor(t, calladapter.Options{
				Resolver: executorResolver(resolved), State: journal, Context: defaultContextProvider(),
				Inline: inlineFunc(emptyInline), Runs: runFunc(validChildRun),
			})
			_, err := executor.Prepare(t.Context(), testInvocation(graph.CallInline, graph.DefinitionRef{ID: "child"}, 1, values.ValueSet{}))
			var execution *stepkind.ExecutionError
			if !errors.As(err, &execution) || execution.Code != calladapter.CodeResolutionInvalid || len(journal.Events()) != 0 {
				t.Fatalf("Prepare() = %T %v, events=%d", err, err, len(journal.Events()))
			}
		})
	}
}

func TestPrepareRequiresAuthoritativeLineageAndEnforcesCycleDepth(t *testing.T) {
	child := resolvedDefinition("child", nil, nil)
	tests := []struct {
		name     string
		lineage  []graph.DefinitionRef
		maxDepth int
		wantCode string
	}{
		{"missing", nil, 4, calladapter.CodeCycle},
		{"cycle", []graph.DefinitionRef{child.Definition}, 4, calladapter.CodeCycle},
		{"ancestor cycle", []graph.DefinitionRef{rootDefinition(), resolvedDefinition("middle", nil, nil).Definition, rootDefinition()}, 4, calladapter.CodeCycle},
		{"depth", []graph.DefinitionRef{rootDefinition(), resolvedDefinition("middle", nil, nil).Definition}, 1, calladapter.CodeDepthExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := mustExecutor(t, calladapter.Options{
				Resolver: resolverFunc(func(context.Context, graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
					return child, nil
				}),
				State: calltest.NewJournal(), Context: defaultContextProvider(),
				Inline: inlineFunc(emptyInline), Runs: runFunc(validChildRun), MaxDepth: test.maxDepth,
			})
			invocation := testInvocation(graph.CallInline, graph.DefinitionRef{ID: "child"}, 1, values.ValueSet{})
			invocation.Call.Lineage = test.lineage
			_, err := executor.Prepare(t.Context(), invocation)
			var execution *stepkind.ExecutionError
			if !errors.As(err, &execution) || execution.Code != test.wantCode {
				t.Fatalf("Prepare() error = %T %v", err, err)
			}
		})
	}
}

func TestResolutionJournalPinsRetryAndConflictsOnResolverDrift(t *testing.T) {
	journal := calltest.NewJournal()
	current := resolvedDefinition("child", nil, nil)
	executor := mustExecutor(t, calladapter.Options{
		Resolver: resolverFunc(func(context.Context, graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
			return current, nil
		}),
		State: journal, Context: defaultContextProvider(), Inline: inlineFunc(emptyInline), Runs: runFunc(validChildRun),
	})
	first := testInvocation(graph.CallInline, graph.DefinitionRef{Authority: "registry", ID: "child", Version: "stable"}, 1, values.ValueSet{})
	if _, err := executor.Prepare(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	snapshot, err := journal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := calltest.RestoreJournal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	replayedExecutor := mustExecutor(t, calladapter.Options{
		Resolver: executorResolver(current), State: restarted, Context: defaultContextProvider(),
		Inline: inlineFunc(emptyInline), Runs: runFunc(validChildRun),
	})
	retry := first
	retry.Identity.Attempt = 2
	if _, prepareErr := replayedExecutor.Prepare(t.Context(), retry); prepareErr != nil {
		t.Fatalf("same-resolution retry did not replay: %v", prepareErr)
	}
	if got := len(restarted.Events()); got != 1 {
		t.Fatalf("replay appended events = %d, want 1", got)
	}

	drifted := resolvedDefinition("child", nil, nil)
	drifted.Definition.Digest = testDigest("child-drifted")
	drifted.Definition.Provenance.Digest = drifted.Definition.Digest
	drifted.Graph.Digest = drifted.Definition.Digest
	drifted.Graph.Provenance.Digest = drifted.Definition.Digest
	driftExecutor := mustExecutor(t, calladapter.Options{
		Resolver: executorResolver(drifted), State: restarted, Context: defaultContextProvider(),
		Inline: inlineFunc(emptyInline), Runs: runFunc(validChildRun),
	})
	_, err = driftExecutor.Prepare(t.Context(), retry)
	if !errors.Is(err, calladapter.ErrResolutionConflict) {
		t.Fatalf("resolver drift error = %v", err)
	}
	if got := len(restarted.Events()); got != 1 {
		t.Fatalf("conflict mutated event journal = %d", got)
	}
}

func TestResolutionJournalConcurrentExactReplayHasOneEvent(t *testing.T) {
	journal := calltest.NewJournal()
	record := validResolutionRecord()
	var applied atomic.Int64
	var wg sync.WaitGroup
	errorsFound := make(chan error, 64)
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			persisted, outcome, err := journal.RecordCallResolution(context.Background(), calladapter.RecordResolutionRequest{Record: record})
			if err != nil || !reflect.DeepEqual(persisted, record) {
				errorsFound <- errors.Join(err, errors.New("record mismatch"))
				return
			}
			if outcome == calladapter.ResolutionApplied {
				applied.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
	if applied.Load() != 1 || len(journal.Events()) != 1 {
		t.Fatalf("applied=%d events=%d", applied.Load(), len(journal.Events()))
	}
}

func TestRunModeAmbiguousSuccessRetryCreatesOneStableChildOperation(t *testing.T) {
	journal := calltest.NewJournal()
	launcher := &ambiguousLauncher{}
	executor := mustExecutor(t, calladapter.Options{
		Resolver: executorResolver(resolvedDefinition("child", nil, nil)), State: journal,
		Context: defaultContextProvider(), Inline: inlineFunc(emptyInline), Runs: launcher,
	})
	first := testInvocation(graph.CallRun, graph.DefinitionRef{Authority: "registry", ID: "child", Version: "stable"}, 1, values.ValueSet{})
	first.IdempotencyKey = "stable-node-key"
	prepared, err := executor.Prepare(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	if _, executeErr := executor.Execute(t.Context(), prepared); executeErr == nil || stepkind.ClassifyError(executeErr) != stepkind.Retryable {
		t.Fatalf("ambiguous first Execute() error = %v", executeErr)
	}

	retry := first
	retry.Identity.Attempt = 2
	prepared, err = executor.Prepare(t.Context(), retry)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(t.Context(), prepared)
	if err != nil {
		t.Fatalf("retry Execute() error = %v", err)
	}
	if launcher.created != 1 || launcher.calls != 2 || len(launcher.keys) != 2 || launcher.keys[0] != launcher.keys[1] ||
		launcher.ids[0] != launcher.ids[1] || result.Outputs[calladapter.OutputRunID].Inline != string(launcher.ids[0]) {
		t.Fatalf("launcher = created:%d calls:%d keys:%#v ids:%#v result:%#v", launcher.created, launcher.calls, launcher.keys, launcher.ids, result)
	}
}

func TestInlineModeEnforcesDeclaredTypedOutputs(t *testing.T) {
	declarations := []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "string"}}}
	valid := inlineValue(t, "ok", "child", values.RedactionPrivate, values.RetentionRun)
	wrongType := inlineValue(t, 7, "child", values.RedactionPrivate, values.RetentionRun)
	for _, test := range []struct {
		name    string
		outputs values.ValueSet
	}{
		{"missing", values.ValueSet{}},
		{"extra", values.ValueSet{"result": valid, "extra": valid}},
		{"schema", values.ValueSet{"result": wrongType}},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := mustExecutor(t, calladapter.Options{
				Resolver: executorResolver(resolvedDefinition("child", nil, declarations)), State: calltest.NewJournal(),
				Context: defaultContextProvider(), Inline: inlineFunc(func(context.Context, calladapter.InlineRequest) (calladapter.InlineResult, error) {
					return calladapter.InlineResult{Outputs: test.outputs}, nil
				}), Runs: runFunc(validChildRun),
			})
			prepared, err := executor.Prepare(t.Context(), testInvocation(graph.CallInline, graph.DefinitionRef{ID: "child"}, 1, values.ValueSet{}))
			if err != nil {
				t.Fatal(err)
			}
			_, err = executor.Execute(t.Context(), prepared)
			var execution *stepkind.ExecutionError
			if !errors.As(err, &execution) || execution.Code != calladapter.CodeResultInvalid {
				t.Fatalf("Execute() = %T %v", err, err)
			}
		})
	}
}

func TestInlineCollaboratorReceivesDefensiveStableRetryRequest(t *testing.T) {
	resolved := resolvedDefinition("child", []graph.InputSpec{{Name: "payload", Required: true, Schema: graph.Schema{"type": "object"}}}, []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "string"}}})
	payload := inlineValue(t, map[string]any{"message": "original"}, "parent", values.RedactionPrivate, values.RetentionRun)
	output := inlineValue(t, "done", "child", values.RedactionPrivate, values.RetentionRun)
	calls := 0
	executor := mustExecutor(t, calladapter.Options{
		Resolver: executorResolver(resolved), State: calltest.NewJournal(), Context: defaultContextProvider(),
		Inline: inlineFunc(func(_ context.Context, request calladapter.InlineRequest) (calladapter.InlineResult, error) {
			calls++
			if request.Definition.Graph.ID != "child" || request.Lineage[0].Digest != rootDefinition().Digest ||
				request.Inputs["payload"].Inline.(map[string]any)["message"] != "original" {
				return calladapter.InlineResult{}, errors.New("inline request did not remain immutable across replay")
			}
			request.Definition.Graph.ID = "mutated"
			request.Lineage[0].Digest = testDigest("mutated")
			request.Inputs["payload"].Inline.(map[string]any)["message"] = "mutated"
			return calladapter.InlineResult{Outputs: values.ValueSet{"result": output}}, nil
		}), Runs: runFunc(validChildRun),
	})
	invocation := testInvocation(graph.CallInline, graph.DefinitionRef{ID: "child"}, 1, values.ValueSet{"payload": payload})
	invocation.IdempotencyKey = "stable-inline-key"
	prepared, err := executor.Prepare(t.Context(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, executeErr := executor.Execute(t.Context(), prepared)
		if executeErr != nil || result.Outputs["result"].Inline != "done" {
			t.Fatalf("Execute() = %#v, %v", result, executeErr)
		}
	}
	if calls != 2 {
		t.Fatalf("inline calls = %d", calls)
	}
}

func TestRunModeProducesCompleteTypedHandleForEveryParentClosePolicy(t *testing.T) {
	for _, policy := range []graph.ParentClosePolicy{
		graph.ParentCloseCancel, graph.ParentCloseAbandon, graph.ParentCloseRequestCancel,
	} {
		t.Run(string(policy), func(t *testing.T) {
			var captured calladapter.ChildRunRequest
			launcher := runFunc(func(ctx context.Context, request calladapter.ChildRunRequest) (calladapter.ChildRunResult, error) {
				captured = request
				result, err := validChildRun(ctx, request)
				if err != nil {
					return calladapter.ChildRunResult{}, err
				}
				result.Run.Status = workflowruntime.RunSucceeded
				result.Run.Outputs = &values.ValueSetRef{ID: "child-outputs", Digest: testDigest("child-outputs")}
				return result, nil
			})
			executor := mustExecutor(t, calladapter.Options{
				Resolver: executorResolver(resolvedDefinition("child", nil, nil)), State: calltest.NewJournal(),
				Context: defaultContextProvider(), Inline: inlineFunc(emptyInline), Runs: launcher,
			})
			invocation := testInvocation(graph.CallRun, graph.DefinitionRef{ID: "child"}, 1, values.ValueSet{})
			invocation.Call.Spec.OnParentClose = policy
			prepared, err := executor.Prepare(t.Context(), invocation)
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.Execute(t.Context(), prepared)
			if err != nil {
				t.Fatal(err)
			}
			if captured.ParentClose != policy || captured.ChildRunID == "" || captured.IdempotencyKey == "" || result.Outcome != stepkind.StepCompleted {
				t.Fatalf("request=%#v result=%#v", captured, result)
			}
			for _, name := range []string{
				calladapter.OutputRunID, calladapter.OutputStatus, calladapter.OutputEventsRef,
				calladapter.OutputCancellation, calladapter.OutputOutputsRef,
			} {
				if value, ok := result.Outputs[name]; !ok || value.Validate() != nil {
					t.Fatalf("typed output %q = %#v", name, value)
				}
			}
			if result.Outputs[calladapter.OutputRunID].Inline != string(captured.ChildRunID) ||
				result.Outputs[calladapter.OutputStatus].Inline != string(workflowruntime.RunSucceeded) {
				t.Fatalf("run identity/status outputs = %#v", result.Outputs)
			}
			cancellation, ok := result.Outputs[calladapter.OutputCancellation].Inline.(map[string]any)
			if !ok || cancellation["policy"] != string(policy) || cancellation["run_id"] != string(captured.ChildRunID) {
				t.Fatalf("cancellation output = %#v", result.Outputs[calladapter.OutputCancellation])
			}
			outputs, ok := result.Outputs[calladapter.OutputOutputsRef].Inline.(map[string]any)
			if !ok || outputs["id"] != "child-outputs" || outputs["digest"] != testDigest("child-outputs") {
				t.Fatalf("output reference = %#v", result.Outputs[calladapter.OutputOutputsRef])
			}
		})
	}
}

type resolverFunc func(context.Context, graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error)

func (f resolverFunc) ResolveDefinition(ctx context.Context, ref graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
	return f(ctx, ref)
}

type inlineFunc func(context.Context, calladapter.InlineRequest) (calladapter.InlineResult, error)

func (f inlineFunc) ExecuteInline(ctx context.Context, request calladapter.InlineRequest) (calladapter.InlineResult, error) {
	return f(ctx, request)
}

type runFunc func(context.Context, calladapter.ChildRunRequest) (calladapter.ChildRunResult, error)

func (f runFunc) StartChildRun(ctx context.Context, request calladapter.ChildRunRequest) (calladapter.ChildRunResult, error) {
	return f(ctx, request)
}

func emptyInline(context.Context, calladapter.InlineRequest) (calladapter.InlineResult, error) {
	return calladapter.InlineResult{Outputs: values.ValueSet{}}, nil
}

func executorResolver(resolved workflowcompile.ResolvedDefinition) resolverFunc {
	return func(context.Context, graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
		return resolved, nil
	}
}

func defaultContextProvider() calladapter.ContextProvider {
	return calladapter.ContextProviderFunc(func(context.Context, stepkind.Invocation) (values.ExpressionContext, values.ExpressionOptions, error) {
		return values.ExpressionContext{Inputs: values.ValueSet{}}, values.ExpressionOptions{}, nil
	})
}

func testOptions(executor *calladapter.Executor) calladapter.Options {
	// Register validates and snapshots the already-tested collaborator shape;
	// use a separate minimal option set because Executor fields are private.
	return calladapter.Options{
		Resolver: executorResolver(resolvedDefinition("child", nil, nil)), State: calltest.NewJournal(),
		Context: defaultContextProvider(), Inline: inlineFunc(emptyInline), Runs: runFunc(validChildRun),
	}
}

func newTestExecutor(t *testing.T, resolver workflowcompile.DefinitionResolver, state calladapter.ResolutionStore, inline calladapter.InlineExecutor, runs calladapter.ChildRunExecutor) *calladapter.Executor {
	t.Helper()
	return mustExecutor(t, calladapter.Options{Resolver: resolver, State: state, Context: defaultContextProvider(), Inline: inline, Runs: runs})
}

func mustExecutor(t *testing.T, options calladapter.Options) *calladapter.Executor {
	t.Helper()
	executor, err := calladapter.New(options)
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func testInvocation(mode graph.CallMode, ref graph.DefinitionRef, attempt int, inputs values.ValueSet) stepkind.Invocation {
	return stepkind.Invocation{
		Identity: testIdentity(attempt), Config: graph.Config{}, Inputs: inputs,
		Call: &stepkind.CallInvocation{
			Spec:    graph.CallSpec{Definition: ref, Mode: mode, OnParentClose: graph.ParentCloseCancel},
			Lineage: []graph.DefinitionRef{rootDefinition()},
		},
	}
}

func testIdentity(attempt int) stepkind.InvocationIdentity {
	return stepkind.InvocationIdentity{RunID: "parent-run", NodeID: "call-child", Iteration: "item-1", Attempt: attempt}
}

func rootDefinition() graph.DefinitionRef {
	digest := testDigest("root")
	return graph.DefinitionRef{
		Kind: "workflow", ID: "root", Version: "v1", Digest: digest,
		Provenance: &graph.Provenance{Authority: "fixture", Locator: "root.workflow.yaml", Digest: digest},
	}
}

func resolvedDefinition(id string, inputs []graph.InputSpec, outputs []graph.OutputSpec) workflowcompile.ResolvedDefinition {
	digest := testDigest(id)
	provenance := graph.Provenance{Authority: "fixture", Origin: "test", Locator: id + ".workflow.yaml", Revision: "v1", Digest: digest}
	return workflowcompile.ResolvedDefinition{
		Definition: graph.DefinitionRef{Authority: "fixture", Kind: "workflow", ID: id, Locator: provenance.Locator, Version: "v1", Digest: digest, Provenance: &provenance},
		Graph:      graph.Graph{ID: id, Version: "v1", Digest: digest, Provenance: provenance, Inputs: inputs, Outputs: outputs, Nodes: []graph.Node{}},
	}
}

func validChildRun(_ context.Context, request calladapter.ChildRunRequest) (calladapter.ChildRunResult, error) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	invocation := workflowruntime.NodeInvocationID{RunID: workflowruntime.RunID(request.Parent.RunID), NodeID: request.Parent.NodeID, Iteration: request.Parent.Iteration}
	return calladapter.ChildRunResult{
		Run: workflowruntime.RunSnapshot{
			ID: request.ChildRunID, Plan: request.Plan, Status: workflowruntime.RunRunning,
			Generation: 1, CreatedAt: now, UpdatedAt: now,
		},
		Link: workflowruntime.ChildRunLink{
			ParentRunID: invocation.RunID, Invocation: invocation, ChildRunID: request.ChildRunID,
			Policy: request.ParentClose, CreatedAt: now,
		},
		EventsRef: "events:" + string(request.ChildRunID),
		Cancellation: calladapter.CancellationHandle{
			RunID: request.ChildRunID, Policy: request.ParentClose, Ref: "cancel:" + string(request.ChildRunID),
		},
	}, nil
}

type ambiguousLauncher struct {
	calls          int
	created        int
	keys           []string
	ids            []workflowruntime.RunID
	stored         map[string]calladapter.ChildRunResult
	storedRequests map[string]calladapter.ChildRunRequest
}

func (l *ambiguousLauncher) StartChildRun(ctx context.Context, request calladapter.ChildRunRequest) (calladapter.ChildRunResult, error) {
	l.calls++
	l.keys = append(l.keys, request.IdempotencyKey)
	l.ids = append(l.ids, request.ChildRunID)
	if l.stored == nil {
		l.stored = make(map[string]calladapter.ChildRunResult)
		l.storedRequests = make(map[string]calladapter.ChildRunRequest)
	}
	result, exists := l.stored[request.IdempotencyKey]
	if !exists {
		l.created++
		result, _ = validChildRun(ctx, request)
		l.stored[request.IdempotencyKey] = result
		l.storedRequests[request.IdempotencyKey] = request
		return calladapter.ChildRunResult{}, &stepkind.ExecutionError{
			Code: "ambiguous", Message: "child start outcome is unknown", Classification: stepkind.Retryable,
		}
	}
	if !reflect.DeepEqual(l.storedRequests[request.IdempotencyKey], request) {
		return calladapter.ChildRunResult{}, workflowruntime.ErrIdempotencyConflict
	}
	return result, nil
}

func inlineValue(t *testing.T, payload any, producer string, redaction values.RedactionClass, retention values.RetentionClass) values.Value {
	t.Helper()
	value, err := values.NewInline(payload, values.Metadata{
		Producer: values.Producer{Kind: producer, Reference: "fixture"}, MediaType: "application/json",
		Redaction: redaction, Retention: retention,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func testDigest(value string) string { return values.SHA256Digest([]byte(value)) }

func validResolutionRecord() calladapter.ResolutionRecord {
	resolved := resolvedDefinition("child", nil, nil).Definition
	return calladapter.ResolutionRecord{
		Key: "call-resolution:key", Invocation: calladapter.CallSiteIdentity{RunID: "parent-run", NodeID: "call-child"},
		Requested: graph.DefinitionRef{Authority: "registry", Kind: "workflow", ID: "child", Version: "stable"},
		Resolved:  resolved, InputDigest: testDigest("inputs"), Lineage: []graph.DefinitionRef{rootDefinition(), resolved},
	}
}

var _ workflowcompile.DefinitionResolver = resolverFunc(nil)
var _ calladapter.InlineExecutor = inlineFunc(nil)
var _ calladapter.ChildRunExecutor = runFunc(nil)
