package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	// CodeInvalidRunBinding identifies an incomplete plan, run identity, or
	// persistence-independent binding request.
	CodeInvalidRunBinding diagnostic.Code = "HADR-VALUE-010"
	// CodeUnknownWorkflowInput identifies caller data absent from the plan's
	// declared workflow inputs.
	CodeUnknownWorkflowInput diagnostic.Code = "HADR-VALUE-011"
	// CodeMissingWorkflowInput identifies an unsatisfied required declaration.
	CodeMissingWorkflowInput diagnostic.Code = "HADR-VALUE-012"
	// CodeInvalidWorkflowDefault identifies a default that cannot be bound as a
	// literal typed value.
	CodeInvalidWorkflowDefault diagnostic.Code = "HADR-VALUE-013"
	// CodeInvalidWorkflowInput identifies caller/default data that is not a
	// losslessly normalized native JSON value.
	CodeInvalidWorkflowInput diagnostic.Code = "HADR-VALUE-014"
	// CodeWorkflowInputSchema identifies an invalid input schema or a value that
	// violates it.
	CodeWorkflowInputSchema diagnostic.Code = "HADR-VALUE-015"
	// CodeGraphNotComplete identifies output binding attempted before every
	// declared graph node has a canonical terminal observation.
	CodeGraphNotComplete diagnostic.Code = "HADR-VALUE-016"
	// CodeWorkflowOutputBinding identifies an absent, malformed, or failed
	// workflow output binding.
	CodeWorkflowOutputBinding diagnostic.Code = "HADR-VALUE-017"
	// CodeWorkflowOutputSchema identifies an invalid output schema or a bound
	// output that violates it.
	CodeWorkflowOutputSchema diagnostic.Code = "HADR-VALUE-018"
)

var ErrOutputConflict = errors.New("workflow run output finalization conflict")

var errUnsafeCallerNumber = errors.New("caller number cannot be bound losslessly")

const bindingMediaType = "application/json"

// BoundRun is an immutable release artifact between an ExecutionPlan and a
// persisted RunSnapshot. InputsRef names one validated, complete value set;
// its existence alone does not mean the run has started. Plan retains the
// exact plan ID, version, digest, and serialized schema version used at bind
// time. Provenance is copied from that immutable plan.
type BoundRun struct {
	ID         RunID              `json:"id"`
	Plan       PlanRef            `json:"plan"`
	InputsRef  values.ValueSetRef `json:"inputs_ref"`
	CreatedAt  time.Time          `json:"created_at"`
	Provenance graph.Provenance   `json:"provenance"`
}

// Validate defensively checks the extraction-safe BoundRun envelope. It does
// not resolve, load, or start the run.
func (r BoundRun) Validate() error {
	if err := validateOpaqueID("bound run id", string(r.ID)); err != nil {
		return err
	}
	if err := r.Plan.Validate(); err != nil {
		return err
	}
	if err := r.InputsRef.Validate(); err != nil {
		return fmt.Errorf("bound run inputs: %w", err)
	}
	if r.CreatedAt.IsZero() {
		return fmt.Errorf("bound run created_at is required")
	}
	if _, err := cloneProvenance(r.Provenance); err != nil {
		return fmt.Errorf("bound run provenance must be JSON-compatible: %w", err)
	}
	return nil
}

// BindRunRequest supplies immutable plan identity and caller data. Inputs are
// losslessly normalized native JSON values; strings are never parsed into
// numbers, booleans, null, arrays, or objects.
type BindRunRequest struct {
	ID        RunID
	Plan      *compile.ExecutionPlan
	Inputs    map[string]any
	CreatedAt time.Time
}

// BindRunResult contains a BoundRun only when all diagnostics are absent and
// the complete input set was saved successfully.
type BindRunResult struct {
	Run         *BoundRun
	Diagnostics []diagnostic.Diagnostic
}

// BindRun validates all caller inputs and defaults before performing exactly
// one SaveValues call. SaveValues has no idempotency contract: rebinding is a
// new release operation and may create another unreferenced immutable value
// set. A start retry must reuse the returned BoundRun and InputsRef.
func BindRun(ctx context.Context, store StateStore, request BindRunRequest) (BindRunResult, error) {
	if nilStateStore(store) {
		return BindRunResult{}, fmt.Errorf("state store is required")
	}
	planRef, findings := bindingPlanRef(request)
	if err := validateOpaqueID("bound run id", string(request.ID)); err != nil {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, nil,
			fmt.Sprintf("run identity is invalid: %v", err),
			"Supply a non-empty host-owned run identity.",
		))
	}
	if request.CreatedAt.IsZero() {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, planGraphSource(request.Plan),
			"bound run creation time is required",
			"Supply the immutable creation time for this binding operation.",
		))
	}
	if len(findings) != 0 {
		sortBindingDiagnostics(findings)
		return BindRunResult{Diagnostics: findings}, nil
	}

	boundInputs, inputFindings := bindWorkflowInputs(request.ID, request.Plan.Graph.Inputs, request.Inputs)
	if len(inputFindings) != 0 {
		sortBindingDiagnostics(inputFindings)
		return BindRunResult{Diagnostics: inputFindings}, nil
	}
	provenance, err := cloneProvenance(request.Plan.Provenance)
	if err != nil {
		return BindRunResult{}, fmt.Errorf("clone bound run provenance: %w", err)
	}
	inputRef, err := store.SaveValues(ctx, SaveValuesRequest{
		Owner:  ValueOwner{Kind: "run-inputs", RunID: request.ID},
		Values: boundInputs,
	})
	if err != nil {
		return BindRunResult{}, fmt.Errorf("save bound run inputs: %w", err)
	}
	bound := BoundRun{
		ID: request.ID, Plan: planRef, InputsRef: inputRef,
		CreatedAt: request.CreatedAt, Provenance: provenance,
	}
	if err := bound.Validate(); err != nil {
		return BindRunResult{}, fmt.Errorf("construct bound run: %w", err)
	}
	return BindRunResult{Run: &bound}, nil
}

// StartBoundRun delegates exact start replay and conflict decisions to
// StateStore.CreateRun. Callers must reuse the same BoundRun for retries.
func StartBoundRun(ctx context.Context, store StateStore, run BoundRun, startIdempotencyKey string) (RunSnapshot, IdempotencyOutcome, error) {
	if nilStateStore(store) {
		return RunSnapshot{}, "", fmt.Errorf("state store is required")
	}
	if err := run.Validate(); err != nil {
		return RunSnapshot{}, "", fmt.Errorf("invalid bound run: %w", err)
	}
	if strings.TrimSpace(startIdempotencyKey) == "" {
		return RunSnapshot{}, "", fmt.Errorf("start idempotency key is required")
	}
	if _, err := store.LoadValues(ctx, run.InputsRef); err != nil {
		return RunSnapshot{}, "", fmt.Errorf("load bound run inputs before start: %w", err)
	}
	inputRef := run.InputsRef
	return store.CreateRun(ctx, CreateRunRequest{
		ID: run.ID, Plan: run.Plan, Status: RunPending, Inputs: &inputRef,
		StartIdempotencyKey: startIdempotencyKey, CreatedAt: run.CreatedAt,
	})
}

// OutputFinalizationOutcome distinguishes a newly published output set from a
// semantically identical replay against an already succeeded run.
type OutputFinalizationOutcome string

const (
	OutputFinalizationApplied  OutputFinalizationOutcome = "applied"
	OutputFinalizationReplayed OutputFinalizationOutcome = "replayed"
)

// FinalizeRunRequest binds declared workflow outputs after graph completion.
// Context may contain more steps than the plan, but output evaluation sees
// only declared plan nodes. BaseOptions retains host policy such as AllowEnv.
type FinalizeRunRequest struct {
	BoundRun    BoundRun
	Run         RunSnapshot
	Plan        *compile.ExecutionPlan
	Context     values.ExpressionContext
	BaseOptions values.ExpressionOptions
	At          time.Time
}

// FinalizeRunResult is present only after outputs have been published or an
// already-published semantically identical set has been confirmed.
type FinalizeRunResult struct {
	Run         RunSnapshot
	Outputs     values.ValueSet
	OutputsRef  values.ValueSetRef
	Outcome     OutputFinalizationOutcome
	Diagnostics []diagnostic.Diagnostic
}

// FinalizeRunOutputs evaluates and validates the entire declared output set,
// saves it once, and publishes only the complete reference with the succeeded
// run transition. A failed transition can leave the complete saved set
// unreferenced, but never publishes partial outputs. Routine replay loads and
// compares existing outputs before any SaveValues call.
func FinalizeRunOutputs(ctx context.Context, store StateStore, request FinalizeRunRequest) (FinalizeRunResult, error) {
	if nilStateStore(store) {
		return FinalizeRunResult{}, fmt.Errorf("state store is required")
	}
	findings := validateFinalizationRequest(request)
	if len(findings) != 0 {
		sortBindingDiagnostics(findings)
		return FinalizeRunResult{Diagnostics: findings}, nil
	}
	inputs, err := store.LoadValues(ctx, request.BoundRun.InputsRef)
	if err != nil {
		return FinalizeRunResult{}, fmt.Errorf("load bound run inputs: %w", err)
	}
	contextCopy := request.Context
	contextCopy.Inputs = inputs
	options := request.BaseOptions
	options.VisibleSteps = declaredNodeIDs(request.Plan.Graph)

	outputs, outputFindings := bindWorkflowOutputs(
		request.BoundRun.ID, request.Plan.Graph.Outputs, contextCopy, options,
	)
	if len(outputFindings) != 0 {
		sortBindingDiagnostics(outputFindings)
		return FinalizeRunResult{Diagnostics: outputFindings}, nil
	}

	if request.Run.Status == RunSucceeded {
		return replayFinalizedOutputs(ctx, store, request.Run, outputs)
	}
	outputRef, err := store.SaveValues(ctx, SaveValuesRequest{
		Owner: ValueOwner{Kind: "run-outputs", RunID: request.BoundRun.ID}, Values: outputs,
	})
	if err != nil {
		return FinalizeRunResult{}, fmt.Errorf("save bound run outputs: %w", err)
	}
	transition, err := store.TransitionRun(ctx, RunTransitionRequest{
		RunID: request.Run.ID, ExpectedGeneration: request.Run.Generation,
		To: RunSucceeded, Outputs: &outputRef, At: request.At,
	})
	if err != nil {
		return FinalizeRunResult{}, fmt.Errorf("publish bound run outputs: %w", err)
	}
	return FinalizeRunResult{
		Run: transition.Snapshot, Outputs: outputs, OutputsRef: outputRef,
		Outcome: OutputFinalizationApplied,
	}, nil
}

func bindingPlanRef(request BindRunRequest) (PlanRef, []diagnostic.Diagnostic) {
	if request.Plan == nil {
		return PlanRef{}, []diagnostic.Diagnostic{bindingDiagnostic(
			CodeInvalidRunBinding, nil, "execution plan is required for run binding",
			"Compile and validate an immutable execution plan before binding inputs.",
		)}
	}
	plan := request.Plan
	ref := PlanRef{
		ID: plan.ID, Version: plan.Graph.Version, Digest: plan.Digest,
		SchemaVersion: plan.SchemaVersion,
	}
	var findings []diagnostic.Diagnostic
	if plan.ID != plan.Graph.ID {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, planGraphSource(plan),
			"execution plan and graph identities do not match",
			"Recompile the workflow into a coherent immutable execution plan.",
		))
	}
	if err := ref.Validate(); err != nil {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, planGraphSource(plan),
			fmt.Sprintf("execution plan identity is invalid: %v", err),
			"Recompile the workflow into a complete immutable execution plan.",
		))
	}
	if _, err := cloneProvenance(plan.Provenance); err != nil {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, planGraphSource(plan),
			fmt.Sprintf("execution plan provenance is invalid: %v", err),
			"Keep plan provenance JSON-compatible and immutable.",
		))
	}
	return ref, findings
}

func bindWorkflowInputs(runID RunID, specs []graph.InputSpec, caller map[string]any) (values.ValueSet, []diagnostic.Diagnostic) {
	declared := make(map[string]graph.InputSpec, len(specs))
	var findings []diagnostic.Diagnostic
	for _, spec := range specs {
		if err := graph.ValidateID(spec.Name); err != nil {
			findings = append(findings, bindingDiagnostic(
				CodeInvalidRunBinding, spec.Source,
				fmt.Sprintf("workflow input identity %q is invalid", spec.Name),
				"Recompile a graph with normalized workflow input names.",
			))
			continue
		}
		if _, duplicate := declared[spec.Name]; duplicate {
			findings = append(findings, bindingDiagnostic(
				CodeInvalidRunBinding, spec.Source,
				fmt.Sprintf("workflow input %q is declared more than once", spec.Name),
				"Recompile a graph with unique normalized input names.",
			))
			continue
		}
		declared[spec.Name] = spec
	}
	callerNames := make([]string, 0, len(caller))
	for name := range caller {
		callerNames = append(callerNames, name)
	}
	sort.Strings(callerNames)
	for _, name := range callerNames {
		if _, ok := declared[name]; ok {
			continue
		}
		findings = append(findings, bindingDiagnostic(
			CodeUnknownWorkflowInput, nil,
			fmt.Sprintf("caller input %q is not declared by the workflow", name),
			"Remove the unknown input or declare it in the workflow input contract.",
		))
	}

	bound := make(values.ValueSet, len(specs))
	for _, spec := range specs {
		if err := values.ValidateSchema(spec.Schema); err != nil {
			findings = append(findings, bindingDiagnostic(
				CodeWorkflowInputSchema, spec.Source,
				fmt.Sprintf("workflow input %q has an invalid schema", spec.Name),
				"Fix the declared workflow input schema before binding caller data.",
			))
			continue
		}
		raw, supplied := caller[spec.Name]
		allowEnvelope := supplied
		producerKind := "workflow_input"
		source := spec.Source
		if !supplied && spec.Default != nil {
			if spec.Default.Kind != graph.BindingLiteral || spec.Default.Expression != nil || spec.Default.Interpolation != "" {
				findings = append(findings, bindingDiagnostic(
					CodeInvalidWorkflowDefault, bindingSource(spec.Default, spec.Source),
					fmt.Sprintf("workflow input %q default must be a literal binding", spec.Name),
					"Replace the default expression or interpolation with a literal value.",
				))
				continue
			}
			raw, supplied = spec.Default.Literal, true
			allowEnvelope = false
			producerKind = "workflow_default"
			source = bindingSource(spec.Default, spec.Source)
		}
		if !supplied {
			if spec.Required {
				findings = append(findings, bindingDiagnostic(
					CodeMissingWorkflowInput, spec.Source,
					fmt.Sprintf("required workflow input %q is missing", spec.Name),
					"Supply the required caller input or declare a literal default.",
				))
			}
			continue
		}
		value, err := bindCallerValue(raw, bindingMetadata(producerKind, string(runID), spec.Name), allowEnvelope)
		if errors.Is(err, errUnsafeCallerNumber) {
			findings = append(findings, bindingDiagnostic(
				CodeInvalidWorkflowInput, source,
				fmt.Sprintf("workflow input %q contains a numeric value that cannot be bound losslessly", spec.Name),
				"Decode exact JSON numbers with json.Decoder.UseNumber or supply an integer type instead of an out-of-range floating-point integer.",
			))
			continue
		}
		if err != nil {
			findings = append(findings, bindingDiagnostic(
				CodeInvalidWorkflowInput, source,
				fmt.Sprintf("workflow input %q is not a lossless native JSON value", spec.Name),
				"Supply null, boolean, string, finite number, array, or string-keyed object data without string-encoded typed values.",
			))
			continue
		}
		if err := values.ValidateValueSchema(spec.Schema, value); err != nil {
			message := fmt.Sprintf("workflow input %q does not satisfy its schema", spec.Name)
			remediation := "Supply a value that satisfies the declared workflow input schema."
			if errors.Is(err, values.ErrInvalidSchema) {
				message = fmt.Sprintf("workflow input %q has an invalid schema", spec.Name)
				remediation = "Fix the declared workflow input schema before binding caller data."
			}
			findings = append(findings, bindingDiagnostic(
				CodeWorkflowInputSchema, source,
				message, remediation,
			))
			continue
		}
		bound[spec.Name] = value
	}
	return bound, findings
}

func bindCallerValue(raw any, metadata values.Metadata, allowEnvelope bool) (values.Value, error) {
	if allowEnvelope {
		switch typed := raw.(type) {
		case values.Value:
			return cloneBoundValue(typed)
		case values.ArtifactRef:
			return values.NewArtifact(typed)
		}
	}
	if err := validateLosslessCallerNumbers(reflect.ValueOf(raw), 0); err != nil {
		return values.Value{}, fmt.Errorf("%w: %w", errUnsafeCallerNumber, err)
	}
	return values.NewInline(raw, metadata)
}

func cloneBoundValue(value values.Value) (values.Value, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return values.Value{}, err
	}
	var cloned values.Value
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		return values.Value{}, err
	}
	return cloned, nil
}

func validateLosslessCallerNumbers(value reflect.Value, depth int) error {
	if !value.IsValid() {
		return nil
	}
	if depth > 100 {
		return fmt.Errorf("caller input nesting exceeds binding limit")
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return validateLosslessCallerNumbers(value.Elem(), depth+1)
	}
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		number := value.Float()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return nil // NewInline reports the JSON compatibility diagnostic.
		}
		maximumExactInteger := float64(1<<53 - 1)
		if value.Kind() == reflect.Float32 {
			maximumExactInteger = float64(1<<24 - 1)
		}
		if math.Trunc(number) == number && math.Abs(number) > maximumExactInteger {
			return fmt.Errorf("floating-point integer exceeds its exact consecutive integer range")
		}
	case reflect.Array, reflect.Slice:
		for index := 0; index < value.Len(); index++ {
			if err := validateLosslessCallerNumbers(value.Index(index), depth+1); err != nil {
				return err
			}
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			if err := validateLosslessCallerNumbers(iterator.Value(), depth+1); err != nil {
				return err
			}
		}
	default:
		return nil
	}
	return nil
}

func validateFinalizationRequest(request FinalizeRunRequest) []diagnostic.Diagnostic {
	var findings []diagnostic.Diagnostic
	if request.Plan == nil {
		return []diagnostic.Diagnostic{bindingDiagnostic(
			CodeInvalidRunBinding, nil, "execution plan is required for output finalization",
			"Load the immutable plan used to bind and start this run.",
		)}
	}
	if err := request.BoundRun.Validate(); err != nil {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, planGraphSource(request.Plan),
			fmt.Sprintf("bound run is invalid: %v", err),
			"Use the exact validated BoundRun returned by BindRun.",
		))
	}
	if err := request.Run.Validate(); err != nil {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, planGraphSource(request.Plan),
			fmt.Sprintf("run snapshot is invalid: %v", err),
			"Reload the current persisted run snapshot before finalizing outputs.",
		))
	}
	planRef, planFindings := bindingPlanRef(BindRunRequest{Plan: request.Plan})
	findings = append(findings, planFindings...)
	if request.BoundRun.Plan != planRef || request.Run.Plan != planRef || request.BoundRun.ID != request.Run.ID {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, planGraphSource(request.Plan),
			"plan, bound-run, and persisted-run identities do not match",
			"Finalize with the exact immutable plan and BoundRun used to create this run.",
		))
	}
	if equal, err := equalProvenance(request.BoundRun.Provenance, request.Plan.Provenance); err != nil || !equal {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, planGraphSource(request.Plan),
			"bound-run provenance does not match the immutable execution plan",
			"Finalize with the exact BoundRun returned for this execution plan.",
		))
	}
	if request.Run.Inputs == nil || *request.Run.Inputs != request.BoundRun.InputsRef {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, planGraphSource(request.Plan),
			"persisted run inputs do not match the bound run",
			"Reload the run and use its original BoundRun input reference.",
		))
	}
	if request.At.IsZero() {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, planGraphSource(request.Plan),
			"output finalization time is required",
			"Supply the deterministic completion time for the run transition.",
		))
	}
	if request.Run.Status != RunRunning && request.Run.Status != RunWaiting && request.Run.Status != RunSucceeded {
		findings = append(findings, bindingDiagnostic(
			CodeGraphNotComplete, planGraphSource(request.Plan),
			fmt.Sprintf("run status %q cannot publish completed outputs", request.Run.Status),
			"Finalize outputs only after an active run's graph has completed.",
		))
	}
	if request.Run.Status == RunSucceeded && request.Run.Outputs == nil {
		findings = append(findings, bindingDiagnostic(
			CodeInvalidRunBinding, planGraphSource(request.Plan),
			"succeeded run has no published output reference",
			"Repair the inconsistent persisted run before replaying finalization.",
		))
	}
	findings = append(findings, graphCompletionDiagnostics(request.Plan.Graph, request.Context)...)
	return findings
}

func graphCompletionDiagnostics(value graph.Graph, expressionContext values.ExpressionContext) []diagnostic.Diagnostic {
	nodes := append([]graph.Node(nil), value.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	var findings []diagnostic.Diagnostic
	for _, node := range nodes {
		observed, ok := expressionContext.Steps[node.ID]
		if !ok {
			findings = append(findings, bindingDiagnostic(
				CodeGraphNotComplete, node.Source,
				fmt.Sprintf("graph node %q has no terminal runtime observation", node.ID),
				"Wait until every declared graph node has a canonical terminal status.",
			))
			continue
		}
		if !terminalExpressionStatus(observed.Status) {
			findings = append(findings, bindingDiagnostic(
				CodeGraphNotComplete, node.Source,
				fmt.Sprintf("graph node %q has non-terminal or unsupported status %q", node.ID, observed.Status),
				"Wait until every declared graph node has a canonical terminal status.",
			))
		}
	}
	return findings
}

func terminalExpressionStatus(status string) bool {
	switch NodeStatus(status) {
	case NodeSucceeded, NodeFailed, NodeSkipped, NodeCanceled, NodeTimedOut, NodeCrashed:
		return true
	default:
		return false
	}
}

func bindWorkflowOutputs(
	runID RunID,
	specs []graph.OutputSpec,
	expressionContext values.ExpressionContext,
	options values.ExpressionOptions,
) (values.ValueSet, []diagnostic.Diagnostic) {
	engine := values.NewExpressionEngine()
	bound := make(values.ValueSet, len(specs))
	declared := make(map[string]struct{}, len(specs))
	var findings []diagnostic.Diagnostic
	for _, spec := range specs {
		if err := graph.ValidateID(spec.Name); err != nil {
			findings = append(findings, bindingDiagnostic(
				CodeInvalidRunBinding, spec.Source,
				fmt.Sprintf("workflow output identity %q is invalid", spec.Name),
				"Recompile a graph with normalized workflow output names.",
			))
			continue
		}
		if _, duplicate := declared[spec.Name]; duplicate {
			findings = append(findings, bindingDiagnostic(
				CodeInvalidRunBinding, spec.Source,
				fmt.Sprintf("workflow output %q is declared more than once", spec.Name),
				"Recompile a graph with unique normalized output names.",
			))
			continue
		}
		declared[spec.Name] = struct{}{}
		if spec.Value == nil {
			findings = append(findings, bindingDiagnostic(
				CodeWorkflowOutputBinding, spec.Source,
				fmt.Sprintf("workflow output %q has no value binding", spec.Name),
				"Declare a literal, expression, or interpolation value for the workflow output.",
			))
			continue
		}
		value, err := engine.EvaluateBinding(
			*spec.Value, expressionContext, options,
			bindingMetadata("workflow_output", string(runID), spec.Name),
		)
		if err != nil {
			findings = append(findings, outputBindingDiagnostic(spec, err))
			continue
		}
		if err := values.ValidateValueSchema(spec.Schema, value); err != nil {
			message := fmt.Sprintf("workflow output %q does not satisfy its schema", spec.Name)
			remediation := "Update the output binding or declared schema so the complete typed value satisfies the contract."
			if errors.Is(err, values.ErrInvalidSchema) {
				message = fmt.Sprintf("workflow output %q has an invalid schema", spec.Name)
				remediation = "Fix the declared workflow output schema before finalizing the run."
			}
			findings = append(findings, bindingDiagnostic(
				CodeWorkflowOutputSchema, bindingSource(spec.Value, spec.Source),
				message, remediation,
			))
			continue
		}
		bound[spec.Name] = value
	}
	return bound, findings
}

func outputBindingDiagnostic(spec graph.OutputSpec, err error) diagnostic.Diagnostic {
	var expressionErr *values.ExpressionError
	if errors.As(err, &expressionErr) {
		finding := expressionErr.Diagnostic
		if finding.Source == nil {
			finding.Source = cloneSourceRef(bindingSource(spec.Value, spec.Source))
		}
		if finding.Remediation == nil {
			finding.Remediation = &diagnostic.Remediation{Message: "Update the workflow output expression to use visible, completed typed values."}
		}
		return finding
	}
	return bindingDiagnostic(
		CodeWorkflowOutputBinding, bindingSource(spec.Value, spec.Source),
		fmt.Sprintf("workflow output %q could not be bound: %v", spec.Name, err),
		"Update the workflow output binding to produce a valid typed value.",
	)
}

func replayFinalizedOutputs(ctx context.Context, store StateStore, run RunSnapshot, candidate values.ValueSet) (FinalizeRunResult, error) {
	if run.Outputs == nil {
		return FinalizeRunResult{}, fmt.Errorf("%w: succeeded run has no outputs", ErrOutputConflict)
	}
	digest, err := values.DigestValueSet(candidate)
	if err != nil {
		return FinalizeRunResult{}, fmt.Errorf("digest candidate run outputs: %w", err)
	}
	if digest != run.Outputs.Digest {
		return FinalizeRunResult{}, fmt.Errorf("%w: candidate output digest differs from published output digest", ErrOutputConflict)
	}
	persisted, err := store.LoadValues(ctx, *run.Outputs)
	if err != nil {
		return FinalizeRunResult{}, fmt.Errorf("load published run outputs: %w", err)
	}
	equal, err := equalValueSets(candidate, persisted)
	if err != nil {
		return FinalizeRunResult{}, fmt.Errorf("compare published run outputs: %w", err)
	}
	if !equal {
		return FinalizeRunResult{}, fmt.Errorf("%w: candidate outputs differ from published outputs", ErrOutputConflict)
	}
	return FinalizeRunResult{
		Run: run, Outputs: candidate, OutputsRef: *run.Outputs,
		Outcome: OutputFinalizationReplayed,
	}, nil
}

func bindingMetadata(kind, reference, output string) values.Metadata {
	return values.Metadata{
		Producer:  values.Producer{Kind: kind, Reference: reference, Output: output},
		MediaType: bindingMediaType, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}
}

func bindingDiagnostic(code diagnostic.Code, source *graph.SourceRef, message, remediation string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError, Code: code, Message: message,
		Source:      cloneSourceRef(source),
		Remediation: &diagnostic.Remediation{Message: remediation},
	}
}

func sortBindingDiagnostics(findings []diagnostic.Diagnostic) {
	sort.SliceStable(findings, func(i, j int) bool {
		return bindingDiagnosticKey(findings[i]) < bindingDiagnosticKey(findings[j])
	})
}

func bindingDiagnosticKey(finding diagnostic.Diagnostic) string {
	location := ""
	if finding.Source != nil {
		location = fmt.Sprintf("%s\x00%09d\x00%09d\x00%s", finding.Source.Locator, finding.Source.StartLine, finding.Source.StartColumn, strings.Join(finding.Source.Path, "\x00"))
	}
	return location + "\x00" + string(finding.Code) + "\x00" + finding.Message
}

func declaredNodeIDs(value graph.Graph) []string {
	ids := make([]string, 0, len(value.Nodes))
	for _, node := range value.Nodes {
		ids = append(ids, node.ID)
	}
	sort.Strings(ids)
	return ids
}

func bindingSource(binding *graph.Binding, fallback *graph.SourceRef) *graph.SourceRef {
	if binding == nil {
		return fallback
	}
	if binding.Kind == graph.BindingExpression && binding.Expression != nil && binding.Expression.Source != nil {
		return binding.Expression.Source
	}
	if binding.Source != nil {
		return binding.Source
	}
	return fallback
}

func planGraphSource(plan *compile.ExecutionPlan) *graph.SourceRef {
	if plan == nil {
		return nil
	}
	if plan.Graph.Source != nil {
		return plan.Graph.Source
	}
	return plan.SourceMap.Graph
}

func cloneSourceRef(source *graph.SourceRef) *graph.SourceRef {
	if source == nil {
		return nil
	}
	cloned := *source
	cloned.Path = append([]string(nil), source.Path...)
	return &cloned
}

func cloneProvenance(provenance graph.Provenance) (graph.Provenance, error) {
	encoded, err := json.Marshal(provenance)
	if err != nil {
		return graph.Provenance{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var cloned graph.Provenance
	if err := decoder.Decode(&cloned); err != nil {
		return graph.Provenance{}, err
	}
	return cloned, nil
}

func equalValueSets(left, right values.ValueSet) (bool, error) {
	leftJSON, err := json.Marshal(left)
	if err != nil {
		return false, err
	}
	rightJSON, err := json.Marshal(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftJSON, rightJSON), nil
}

func equalProvenance(left, right graph.Provenance) (bool, error) {
	leftJSON, err := json.Marshal(left)
	if err != nil {
		return false, err
	}
	rightJSON, err := json.Marshal(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftJSON, rightJSON), nil
}

func nilStateStore(store StateStore) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
