package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
	waitadapter "github.com/hollis-labs/hadron/workflow/adapters/wait"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	generatedAuthority = "workflow-sugar"
	generatedOrigin    = "agent-launch-sugar"
	generatedVersion   = "v1"
	waitInputName      = "run-id"
)

// WaitPolicy enables agent_launch wait sugar. Timeout is the canonical
// child-run wait deadline; it does not turn timeout into a successful result.
type WaitPolicy struct {
	Timeout graph.Duration `json:"timeout"`
}

// CompositionRequest describes one graph-native agent_launch declaration.
// Config is the agent_session config embedded in the generated child. Timeout
// and Retry apply to that external operation, while ParentClose applies to the
// ordinary call child-run link.
type CompositionRequest struct {
	NodeID      string                  `json:"node_id"`
	DisplayName string                  `json:"display_name,omitempty"`
	Config      graph.Config            `json:"config"`
	ParentClose graph.ParentClosePolicy `json:"parent_close"`
	Timeout     *graph.TimeoutPolicy    `json:"timeout,omitempty"`
	Retry       *graph.RetryPolicy      `json:"retry,omitempty"`
	Wait        *WaitPolicy             `json:"wait,omitempty"`
	// InputBindings are ordinary parent call bindings. Compose declares a
	// matching generated-child input and forwards each value to agent_session.
	InputBindings map[string]graph.Binding `json:"input_bindings,omitempty"`
	Source        *graph.SourceRef         `json:"source,omitempty"`
}

// Composition is the deterministic expansion of agent_launch. Definition is
// resolved child-workflow material owned by the adapter package. Launch and
// optional Wait are parent graph nodes; Edges contains the wait's data edge.
//
// Without Wait, Launch retains the authored node identity and exposes the
// ordinary call@v1 run-id, status, events-ref, cancellation, and outputs-ref
// handle fields. With Wait, Wait retains the authored identity and exposes the
// ordinary wait_for@v1 payload, resume, and timed_out fields; payload is the
// schema-bound object containing the agent handle, status, and result. The
// source expander must preserve these distinct downstream contracts.
type Composition struct {
	Definition workflowcompile.ResolvedDefinition `json:"definition"`
	Launch     graph.Node                         `json:"launch"`
	Wait       *graph.Node                        `json:"wait,omitempty"`
	Edges      []graph.Edge                       `json:"edges,omitempty"`
}

// Compose expands agent_launch into an ordinary child workflow call and an
// optional child-run wait. The wait reads the call's typed run-id output
// through a normal InputBinding; wait_for resolves that already-bound value at
// execution time.
func Compose(request CompositionRequest) (Composition, error) {
	if err := graph.ValidateID(request.NodeID); err != nil {
		return Composition{}, fmt.Errorf("agent launch node id: %w", err)
	}
	for _, reserved := range []string{"correlation", "idempotency_key"} {
		if _, authored := request.Config[reserved]; authored {
			return Composition{}, fmt.Errorf("agent launch sugar reserves config.%s for runtime-owned identity", reserved)
		}
	}
	if _, err := parseConfig(request.Config); err != nil {
		return Composition{}, err
	}
	if request.ParentClose == "" {
		request.ParentClose = graph.ParentCloseCancel
	}
	if !request.ParentClose.Valid() {
		return Composition{}, fmt.Errorf("unsupported agent parent-close policy %q", request.ParentClose)
	}
	if request.Wait != nil {
		if err := positiveDuration("wait.timeout", request.Wait.Timeout); err != nil {
			return Composition{}, err
		}
	}
	if err := validateTimeoutPolicy(request.Timeout); err != nil {
		return Composition{}, err
	}
	if _, reserved := request.InputBindings[ParentCorrelationInput]; reserved {
		return Composition{}, fmt.Errorf("agent launch input %q is reserved for runtime-owned correlation", ParentCorrelationInput)
	}
	inputNames := make([]string, 0, len(request.InputBindings))
	for name := range request.InputBindings {
		if err := graph.ValidateID(name); err != nil {
			return Composition{}, fmt.Errorf("agent launch input %q: %w", name, err)
		}
		inputNames = append(inputNames, name)
	}
	sort.Strings(inputNames)

	childID := generatedID(request.NodeID, "agent-session")
	childConfig, err := cloneJSON[graph.Config](request.Config)
	if err != nil {
		return Composition{}, fmt.Errorf("copy agent session config: %w", err)
	}
	outputDeclarations, err := agentOutputDeclarations()
	if err != nil {
		return Composition{}, err
	}
	childNode := graph.Node{
		ID: "session", Kind: KindName, KindVersion: KindVersion,
		Config: childConfig, Outputs: outputDeclarations,
		Effects: append(graph.EffectSet(nil), conservativeEffects...),
		Retry:   cloneRetry(request.Retry), Timeout: cloneTimeout(request.Timeout),
		Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed, Scope: "agent-session"},
	}
	childInputs := []graph.InputSpec{{Name: ParentCorrelationInput, Schema: graph.Schema{"type": "string"}, Required: true}}
	childBindings := map[string]graph.Binding{
		ParentCorrelationInput: {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs[" + strconv.Quote(ParentCorrelationInput) + "]"}},
	}
	launchBindings := map[string]graph.Binding{
		ParentCorrelationInput: {
			Kind:          graph.BindingInterpolation,
			Interpolation: "agent:{{ run.id }}:" + request.NodeID,
			Source:        cloneSource(request.Source),
		},
	}
	for _, name := range inputNames {
		binding, copyErr := cloneJSON[graph.Binding](request.InputBindings[name])
		if copyErr != nil {
			return Composition{}, fmt.Errorf("copy agent launch input %q: %w", name, copyErr)
		}
		childInputs = append(childInputs, graph.InputSpec{Name: name, Schema: graph.Schema{}, Required: true})
		childBindings[name] = graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs[" + strconv.Quote(name) + "]"}}
		launchBindings[name] = binding
	}
	child := graph.Graph{
		ID: childID, Version: generatedVersion,
		Inputs:  childInputs,
		Nodes:   []graph.Node{childNode},
		Outputs: agentWorkflowOutputs(outputDeclarations),
	}
	child.Nodes[0].InputBindings = childBindings
	digest, err := workflowcompile.GraphDigest(child)
	if err != nil {
		return Composition{}, fmt.Errorf("encode generated agent child graph: %w", err)
	}
	locator := "agent-launch:" + strings.TrimPrefix(digest, "sha256:")
	provenance := graph.Provenance{
		Authority: generatedAuthority, Origin: generatedOrigin, Locator: locator,
		Revision: generatedVersion, Digest: digest,
	}
	child.Digest, child.Provenance = digest, provenance
	clonedProvenance, err := cloneJSON[graph.Provenance](provenance)
	if err != nil {
		return Composition{}, fmt.Errorf("copy generated agent provenance: %w", err)
	}
	definitionRef := graph.DefinitionRef{
		Authority: generatedAuthority, Kind: "workflow", ID: child.ID, Locator: locator,
		Version: child.Version, Digest: digest, Provenance: &clonedProvenance,
	}
	definition := workflowcompile.ResolvedDefinition{Definition: definitionRef, Graph: child}

	launchID := request.NodeID
	if request.Wait != nil {
		launchID = generatedID(request.NodeID, "launch")
	}
	launch := graph.Node{
		ID: launchID, DisplayName: request.DisplayName,
		Kind: calladapter.KindName, KindVersion: calladapter.KindVersion, Config: graph.Config{},
		Call:          &graph.CallSpec{Definition: definitionRef, Mode: graph.CallRun, OnParentClose: request.ParentClose},
		InputBindings: launchBindings,
		Outputs:       callHandleDeclarations(),
		Effects:       append(graph.EffectSet(nil), conservativeEffects...),
		Idempotency:   &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed, Scope: "agent-launch"},
		Source:        cloneSource(request.Source),
	}
	composition := Composition{Definition: definition, Launch: launch}
	if request.Wait == nil {
		return cloneComposition(composition)
	}

	waitID := request.NodeID
	completionSchema, err := cloneJSON[graph.Schema](outputSchema())
	if err != nil {
		return Composition{}, fmt.Errorf("copy agent completion schema: %w", err)
	}
	waitDisplayName := request.DisplayName
	if waitDisplayName == "" {
		waitDisplayName = request.NodeID
	}
	waitDisplayName += " result"
	waitNode := graph.Node{
		ID: waitID, DisplayName: waitDisplayName,
		Kind: waitadapter.WaitForName, KindVersion: waitadapter.Version,
		Needs: []graph.Need{{Node: launchID, Kind: graph.EdgeData, Source: cloneSource(request.Source)}},
		Config: graph.Config{
			"child_run":      map[string]any{"input": waitInputName, "fail_on_unsuccessful": true},
			"timeout":        string(request.Wait.Timeout),
			"payload_schema": completionSchema,
		},
		InputBindings: map[string]graph.Binding{
			waitInputName: {
				Kind:       graph.BindingExpression,
				Expression: &graph.Expression{Text: "steps[" + strconv.Quote(launchID) + "].outputs[" + strconv.Quote(calladapter.OutputRunID) + "]", Source: cloneSource(request.Source)},
				Source:     cloneSource(request.Source),
			},
		},
		Outputs: waitOutputDeclarations(completionSchema),
		Effects: graph.EffectSet{graph.EffectRead},
		Source:  cloneSource(request.Source),
	}
	edge := graph.Edge{From: launchID, To: waitID, Kind: graph.EdgeData, Source: cloneSource(request.Source), Metadata: graph.Metadata{"generated_by": generatedOrigin}}
	composition.Wait, composition.Edges = &waitNode, []graph.Edge{edge}
	return cloneComposition(composition)
}

func agentOutputDeclarations() ([]graph.OutputSpec, error) {
	properties := outputSchema()["properties"].(map[string]any)
	handle, err := cloneJSON[graph.Schema](properties[OutputHandle].(map[string]any))
	if err != nil {
		return nil, fmt.Errorf("copy agent handle schema: %w", err)
	}
	status, err := cloneJSON[graph.Schema](properties[OutputStatus].(map[string]any))
	if err != nil {
		return nil, fmt.Errorf("copy agent status schema: %w", err)
	}
	return []graph.OutputSpec{
		{Name: OutputHandle, Schema: handle},
		{Name: OutputStatus, Schema: status},
		{Name: OutputResult, Schema: graph.Schema{}},
	}, nil
}

func agentWorkflowOutputs(declarations []graph.OutputSpec) []graph.OutputSpec {
	outputs := make([]graph.OutputSpec, len(declarations))
	copy(outputs, declarations)
	for index := range outputs {
		name := outputs[index].Name
		outputs[index].Value = &graph.Binding{
			Kind:       graph.BindingExpression,
			Expression: &graph.Expression{Text: "steps.session.outputs." + name},
		}
	}
	return outputs
}

func callHandleDeclarations() []graph.OutputSpec {
	return []graph.OutputSpec{
		{Name: calladapter.OutputRunID, Schema: graph.Schema{"type": "string"}},
		{Name: calladapter.OutputStatus, Schema: graph.Schema{"type": "string"}},
		{Name: calladapter.OutputEventsRef, Schema: graph.Schema{"type": "string"}},
		{Name: calladapter.OutputCancellation, Schema: graph.Schema{"type": "object"}},
		{Name: calladapter.OutputOutputsRef, Schema: graph.Schema{"type": []any{"object", "null"}}},
	}
}

func waitOutputDeclarations(payload graph.Schema) []graph.OutputSpec {
	return []graph.OutputSpec{
		{Name: "payload", Schema: payload},
		{Name: "resume", Schema: graph.Schema{"type": "object"}},
		{Name: "timed_out", Schema: graph.Schema{"const": false}},
	}
}

func generatedID(base, suffix string) string {
	candidate := graph.NormalizeID(base + "-" + suffix)
	if len(candidate) <= graph.MaxIDLength {
		return candidate
	}
	digest := strings.TrimPrefix(values.SHA256Digest([]byte(candidate)), "sha256:")[:12]
	maximum := graph.MaxIDLength - len(digest) - 1
	return strings.Trim(candidate[:maximum], "-") + "-" + digest
}

func positiveDuration(name string, raw graph.Duration) error {
	if raw == "" {
		return fmt.Errorf("%s is required", name)
	}
	parsed, err := time.ParseDuration(string(raw))
	if err != nil || parsed <= 0 {
		return fmt.Errorf("%s must be a positive Go duration", name)
	}
	return nil
}

func validateTimeoutPolicy(policy *graph.TimeoutPolicy) error {
	if policy == nil {
		return nil
	}
	for _, field := range []struct {
		name string
		raw  graph.Duration
	}{
		{"timeout.queue", policy.Queue}, {"timeout.execution", policy.Execution}, {"timeout.wait", policy.Wait},
		{"timeout.heartbeat", policy.Heartbeat}, {"timeout.schedule_to_close", policy.ScheduleToClose},
	} {
		if field.raw == "" {
			continue
		}
		if err := positiveDuration(field.name, field.raw); err != nil {
			return err
		}
	}
	return nil
}

func cloneComposition(input Composition) (Composition, error) {
	return cloneJSON[Composition](input)
}

func cloneRetry(input *graph.RetryPolicy) *graph.RetryPolicy {
	if input == nil {
		return nil
	}
	cloned := *input
	cloned.On = append([]string(nil), input.On...)
	return &cloned
}

func cloneTimeout(input *graph.TimeoutPolicy) *graph.TimeoutPolicy {
	if input == nil {
		return nil
	}
	cloned := *input
	return &cloned
}

func cloneSource(input *graph.SourceRef) *graph.SourceRef {
	if input == nil {
		return nil
	}
	cloned := *input
	cloned.Path = append([]string(nil), input.Path...)
	return &cloned
}

func cloneJSON[T any](input any) (T, error) {
	var cloned T
	encoded, err := json.Marshal(input)
	if err != nil {
		return cloned, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&cloned); err != nil {
		return cloned, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return cloned, fmt.Errorf("clone contains trailing JSON")
		}
		return cloned, err
	}
	return cloned, nil
}
