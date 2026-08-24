package call

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

type preparedCall struct {
	resolved         workflowcompile.ResolvedDefinition
	resolution       ResolutionRecord
	inputs           values.ValueSet
	localInputDigest string
}

// New constructs call@v1. Every collaborator is required because the single
// immutable registered kind truthfully supports both inline and child-run
// modes. MaxDepth defaults to compile.DefaultMaxCallDepth.
func New(options Options) (*Executor, error) {
	if nilInterface(options.Resolver) {
		return nil, invalidOptions("definition resolver is required")
	}
	if nilInterface(options.State) {
		return nil, invalidOptions("resolution store is required")
	}
	if nilInterface(options.Context) {
		return nil, invalidOptions("binding context provider is required")
	}
	if nilInterface(options.Inline) {
		return nil, invalidOptions("inline executor is required")
	}
	if nilInterface(options.Runs) {
		return nil, invalidOptions("child-run executor is required")
	}
	maxDepth := options.MaxDepth
	if maxDepth == 0 {
		maxDepth = DefaultMaxDepth
	}
	if maxDepth < 1 {
		return nil, invalidOptions("max depth must be positive")
	}
	return &Executor{
		resolver: options.Resolver, state: options.State, context: options.Context,
		inline: options.Inline, runs: options.Runs, maxDepth: maxDepth,
	}, nil
}

// Register registers call@v1 in registry.
func Register(registry stepkind.Registry, options Options) (*Executor, error) {
	if nilInterface(registry) {
		return nil, invalidOptions("step-kind registry is required")
	}
	executor, err := New(options)
	if err != nil {
		return nil, err
	}
	if err := registry.Register(executor); err != nil {
		return nil, fmt.Errorf("register %s@%s: %w", KindName, KindVersion, err)
	}
	return executor, nil
}

// Spec returns conservative metadata. A child definition may contain any
// portable effect, so policy must inspect the resolved graph before granting
// narrower permissions. Durable call replay requires an invocation key.
func (e *Executor) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: KindName, Version: KindVersion,
		ConfigSchema: graph.Schema{
			"type": "object", "additionalProperties": false,
		},
		InputSchema:  graph.Schema{"type": "object"},
		OutputSchema: graph.Schema{"type": "object"},
		Effects: graph.EffectSet{
			graph.EffectRead, graph.EffectCompute, graph.EffectMaterialize,
			graph.EffectMutate, graph.EffectDestructive,
		},
		RequiredCapabilities:  []string{"workflow.call"},
		Idempotency:           graph.IdempotencyKeyed,
		RetrySafety:           stepkind.RetryRequiresIdempotency,
		Cancellation:          stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:           stepkind.ObservationSpec{Mode: stepkind.ObservationNone},
		Lifecycle:             stepkind.LifecycleSpec{Prepare: true},
		CanSuspend:            false,
		EmbeddedModeSupported: false,
	}
}

// ValidateConfig rejects adapter config fields. Call semantics are represented
// only by graph.Node.Call, preventing a second divergent call envelope.
func (e *Executor) ValidateConfig(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
	if config == nil {
		return []diagnostic.Diagnostic{configDiagnostic("call config must be an object")}
	}
	if len(config) != 0 {
		return []diagnostic.Diagnostic{configDiagnostic("call config must be empty; use the graph call field")}
	}
	return nil
}

// Prepare resolves, policy-checks, and durably records the immutable child
// definition before any child execution. Exact retries return the same record.
func (e *Executor) Prepare(ctx context.Context, invocation stepkind.Invocation) (stepkind.PreparedInvocation, error) {
	if ctx == nil {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "call invocation is invalid", stepkind.RetryPermanent, errors.New("context is required"))
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.PreparedInvocation{}, contextErr
	}
	if e == nil || nilInterface(e.resolver) || nilInterface(e.state) || nilInterface(e.context) || nilInterface(e.inline) || nilInterface(e.runs) || e.maxDepth < 1 {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "call executor is not initialized", stepkind.RetryPermanent, ErrInvalidOptions)
	}
	if err := invocation.Validate(); err != nil {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "call invocation is invalid", stepkind.RetryPermanent, err)
	}
	if invocation.Call == nil {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "call declaration is missing", stepkind.RetryPermanent, ErrInvalidCall)
	}
	if len(e.ValidateConfig(ctx, invocation.Config)) != 0 {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "call config is invalid", stepkind.RetryPermanent, ErrInvalidCall)
	}
	if err := validateCallSpec(invocation.Call.Spec); err != nil {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "call declaration is invalid", stepkind.RetryPermanent, err)
	}

	requested, err := cloneDefinitionRef(invocation.Call.Spec.Definition)
	if err != nil {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "call definition reference is invalid", stepkind.RetryPermanent, err)
	}
	resolved, err := e.resolver.ResolveDefinition(ctx, requested)
	if err != nil {
		return stepkind.PreparedInvocation{}, contextOrCall(ctx, CodeResolutionFailed, "call definition resolution failed", stepkind.Retryable, err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.PreparedInvocation{}, contextErr
	}
	resolved, err = normalizeResolvedDefinition(resolved)
	if err != nil {
		return stepkind.PreparedInvocation{}, callError(CodeResolutionInvalid, "resolved call definition is invalid", stepkind.RetryPermanent, err)
	}
	lineage, err := validateAndExtendLineage(invocation.Call.Lineage, resolved.Definition, e.maxDepth)
	if err != nil {
		code := CodeCycle
		if errors.Is(err, errDepthExceeded) {
			code = CodeDepthExceeded
		}
		return stepkind.PreparedInvocation{}, callError(code, "call lineage policy rejected the child definition", stepkind.RetryPermanent, err)
	}
	providerInvocation, err := cloneInvocation(invocation)
	if err != nil {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "call invocation could not be isolated", stepkind.RetryPermanent, err)
	}
	expressionContext, expressionOptions, err := e.context.ExpressionContext(ctx, providerInvocation)
	if err != nil {
		return stepkind.PreparedInvocation{}, contextOrCall(ctx, CodeInvalidInvocation, "call binding context is unavailable", stepkind.Retryable, err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.PreparedInvocation{}, contextErr
	}
	bound := BindInputs(BindInputsRequest{
		Invocation: invocation.Identity, Resolved: resolved,
		LocalInputs: invocation.Inputs, Context: expressionContext, Options: expressionOptions,
	})
	if len(bound.Diagnostics) != 0 {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "effective call inputs are invalid", stepkind.RetryPermanent, errors.New(bound.Diagnostics[0].Message))
	}
	localInputDigest, err := values.DigestValueSet(invocation.Inputs)
	if err != nil {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "node-local call inputs are invalid", stepkind.RetryPermanent, err)
	}
	inputDigest, err := values.DigestValueSet(bound.Inputs)
	if err != nil {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "effective call inputs are invalid", stepkind.RetryPermanent, err)
	}
	key, err := resolutionKey(invocation.Identity, requested)
	if err != nil {
		return stepkind.PreparedInvocation{}, callError(CodeResolutionInvalid, "call resolution identity is invalid", stepkind.RetryPermanent, err)
	}
	record := ResolutionRecord{
		Key: key, Invocation: callSite(invocation.Identity), Requested: requested,
		Resolved: resolved.Definition, InputDigest: inputDigest, Lineage: lineage,
	}
	persisted, outcome, err := e.state.RecordCallResolution(context.WithoutCancel(ctx), RecordResolutionRequest{Record: cloneResolutionRecord(record)})
	if err != nil {
		classification := stepkind.Retryable
		if errors.Is(err, ErrResolutionConflict) {
			classification = stepkind.RetryPermanent
		}
		return stepkind.PreparedInvocation{}, callError(CodeRecordFailed, "call resolution could not be recorded", classification, err)
	}
	if !outcome.valid() || !equalResolutionRecord(record, persisted) {
		return stepkind.PreparedInvocation{}, callError(CodeRecordFailed, "call resolution store returned an invalid durable result", stepkind.RetryPermanent, ErrResolutionConflict)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.PreparedInvocation{}, contextErr
	}
	clonedInvocation, err := cloneInvocation(invocation)
	if err != nil {
		return stepkind.PreparedInvocation{}, callError(CodeInvalidInvocation, "call invocation could not be isolated", stepkind.RetryPermanent, err)
	}
	return stepkind.PreparedInvocation{
		Invocation: clonedInvocation,
		State: &preparedCall{
			resolved: resolved, resolution: cloneResolutionRecord(record),
			inputs: cloneValueSet(bound.Inputs), localInputDigest: localInputDigest,
		},
	}, nil
}

// Execute drives inline child graphs or atomically creates/replays a child run.
func (e *Executor) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, callError(CodeInvalidInvocation, "call invocation is invalid", stepkind.RetryPermanent, errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if e == nil || nilInterface(e.inline) || nilInterface(e.runs) {
		return stepkind.StepResult{}, callError(CodeInvalidInvocation, "call executor is not initialized", stepkind.RetryPermanent, ErrInvalidOptions)
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, callError(CodeInvalidInvocation, "call invocation is invalid", stepkind.RetryPermanent, err)
	}
	state, ok := prepared.State.(*preparedCall)
	if !ok || state == nil {
		return stepkind.StepResult{}, callError(CodeInvalidInvocation, "prepared call state is missing", stepkind.RetryPermanent, ErrInvalidCall)
	}
	if prepared.Invocation.Call == nil || state.resolution.Invocation != callSite(prepared.Invocation.Identity) {
		return stepkind.StepResult{}, callError(CodeInvalidInvocation, "prepared call state does not match invocation", stepkind.RetryPermanent, ErrInvalidCall)
	}
	localDigest, err := values.DigestValueSet(prepared.Invocation.Inputs)
	if err != nil || localDigest != state.localInputDigest {
		return stepkind.StepResult{}, callError(CodeInvalidInvocation, "prepared node-local call inputs changed", stepkind.RetryPermanent, errors.Join(err, ErrResolutionConflict))
	}
	effectiveDigest, err := values.DigestValueSet(state.inputs)
	if err != nil || effectiveDigest != state.resolution.InputDigest {
		return stepkind.StepResult{}, callError(CodeInvalidInvocation, "prepared effective call inputs differ from durable resolution", stepkind.RetryPermanent, errors.Join(err, ErrResolutionConflict))
	}
	operationKey := state.resolution.Key
	if prepared.Invocation.IdempotencyKey != "" {
		operationKey += ":" + stableHash(prepared.Invocation.IdempotencyKey)
	}
	resolvedCopy, err := cloneResolvedDefinition(state.resolved)
	if err != nil {
		return stepkind.StepResult{}, callError(CodeInvalidInvocation, "prepared resolved definition could not be isolated", stepkind.RetryPermanent, err)
	}

	switch prepared.Invocation.Call.Spec.Mode {
	case graph.CallInline:
		result, executeErr := e.inline.ExecuteInline(ctx, InlineRequest{
			Parent: callSite(prepared.Invocation.Identity), Definition: resolvedCopy,
			Inputs: cloneValueSet(state.inputs), Lineage: cloneDefinitionRefs(state.resolution.Lineage),
			IdempotencyKey: operationKey,
		})
		if executeErr != nil {
			return stepkind.StepResult{}, contextOrCall(ctx, CodeInlineFailed, "inline child workflow failed", stepkind.ClassifyError(executeErr), executeErr)
		}
		if err := ctx.Err(); err != nil {
			return stepkind.StepResult{}, err
		}
		if err := validateDeclaredOutputs(state.resolved.Graph.Outputs, result.Outputs); err != nil {
			return stepkind.StepResult{}, callError(CodeResultInvalid, "inline child outputs are invalid", stepkind.RetryPermanent, err)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: cloneValueSet(result.Outputs)}, nil

	case graph.CallRun:
		plan, planErr := planRef(state.resolved)
		if planErr != nil {
			return stepkind.StepResult{}, callError(CodeResolutionInvalid, "resolved child plan identity is invalid", stepkind.RetryPermanent, planErr)
		}
		childID := childRunID(operationKey)
		policy := prepared.Invocation.Call.Spec.OnParentClose
		if policy == "" {
			policy = graph.ParentCloseCancel
		}
		result, executeErr := e.runs.StartChildRun(ctx, ChildRunRequest{
			Parent: callSite(prepared.Invocation.Identity), ChildRunID: childID,
			Definition: resolvedCopy, Plan: plan,
			Inputs: cloneValueSet(state.inputs), Lineage: cloneDefinitionRefs(state.resolution.Lineage),
			ParentClose: policy, IdempotencyKey: operationKey,
		})
		if executeErr != nil {
			return stepkind.StepResult{}, contextOrCall(ctx, CodeChildRunFailed, "child workflow run could not be started", stepkind.ClassifyError(executeErr), executeErr)
		}
		if err := ctx.Err(); err != nil {
			return stepkind.StepResult{}, err
		}
		if err := validateChildRunResult(prepared.Invocation.Identity, childID, plan, policy, result); err != nil {
			return stepkind.StepResult{}, callError(CodeResultInvalid, "child workflow run result is invalid", stepkind.RetryPermanent, err)
		}
		outputs, err := childHandleValues(prepared.Invocation.Identity, result)
		if err != nil {
			return stepkind.StepResult{}, callError(CodeResultInvalid, "child workflow handle is invalid", stepkind.RetryPermanent, err)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
	default:
		return stepkind.StepResult{}, callError(CodeInvalidInvocation, "call mode is invalid", stepkind.RetryPermanent, ErrInvalidCall)
	}
}

func configDiagnostic(message string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError, Code: diagnostic.Code("HADR-CALL-001"), Message: message,
		Remediation: &diagnostic.Remediation{Message: "Keep call config empty and declare definition, mode, and parent-close policy under call."},
	}
}

func callError(code, message string, classification stepkind.RetryClassification, cause error) error {
	if !classification.Valid() || classification == stepkind.RetryUnspecified {
		classification = stepkind.RetryPermanent
	}
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: classification, Cause: cause}
}

func contextOrCall(ctx context.Context, code, message string, classification stepkind.RetryClassification, cause error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if classification == stepkind.RetryUnspecified {
		classification = stepkind.RetryPermanent
	}
	return callError(code, message, classification, cause)
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var errDepthExceeded = errors.New("maximum call depth exceeded")

func validateAndExtendLineage(lineage []graph.DefinitionRef, child graph.DefinitionRef, maxDepth int) ([]graph.DefinitionRef, error) {
	if len(lineage) == 0 {
		return nil, fmt.Errorf("call lineage must include the parent definition")
	}
	if len(lineage) > maxDepth {
		return nil, fmt.Errorf("%w: next depth %d exceeds maximum %d", errDepthExceeded, len(lineage), maxDepth)
	}
	result := make([]graph.DefinitionRef, 0, len(lineage)+1)
	seen := make(map[string]int, len(lineage))
	for index, ancestor := range lineage {
		if err := validateRequestedDefinition(ancestor); err != nil {
			return nil, fmt.Errorf("call lineage[%d]: %w", index, err)
		}
		cloned, err := cloneDefinitionRef(ancestor)
		if err != nil {
			return nil, fmt.Errorf("call lineage[%d]: %w", index, err)
		}
		if err := values.ValidateDigest(cloned.Digest); err != nil {
			return nil, fmt.Errorf("call lineage[%d] requires immutable digest: %w", index, err)
		}
		if prior, duplicate := seen[cloned.Digest]; duplicate {
			return nil, fmt.Errorf("call lineage[%d] repeats ancestor lineage[%d] digest %q", index, prior, cloned.Digest)
		}
		seen[cloned.Digest] = index
		if cloned.Digest == child.Digest {
			return nil, fmt.Errorf("resolved definition %q repeats active lineage[%d]", child.Digest, index)
		}
		result = append(result, cloned)
	}
	result = append(result, child)
	return result, nil
}

func validateCallSpec(spec graph.CallSpec) error {
	if !spec.Mode.Valid() {
		return fmt.Errorf("unsupported call mode %q", spec.Mode)
	}
	if spec.OnParentClose != "" && !spec.OnParentClose.Valid() {
		return fmt.Errorf("unsupported parent-close policy %q", spec.OnParentClose)
	}
	return validateRequestedDefinition(spec.Definition)
}

func validateRequestedDefinition(ref graph.DefinitionRef) error {
	for _, field := range []struct{ name, value string }{
		{"authority", ref.Authority}, {"kind", ref.Kind}, {"id", ref.ID},
		{"locator", ref.Locator}, {"version", ref.Version}, {"digest", ref.Digest},
	} {
		if err := validateStableString("definition "+field.name, field.value, false); err != nil {
			return err
		}
	}
	if ref.ID == "" && ref.Locator == "" {
		return fmt.Errorf("definition requires id or locator")
	}
	if ref.ID != "" {
		if err := graph.ValidateID(ref.ID); err != nil {
			return fmt.Errorf("definition id: %w", err)
		}
	}
	if ref.Digest != "" {
		if err := values.ValidateDigest(ref.Digest); err != nil {
			return fmt.Errorf("definition digest: %w", err)
		}
	}
	if ref.Provenance != nil {
		if err := validateProvenance("definition provenance", *ref.Provenance, false); err != nil {
			return err
		}
		if ref.Digest != "" && ref.Provenance.Digest != "" && ref.Digest != ref.Provenance.Digest {
			return fmt.Errorf("definition digest differs from provenance digest")
		}
		if ref.Authority != "" && ref.Provenance.Authority != "" && ref.Authority != ref.Provenance.Authority {
			return fmt.Errorf("definition authority differs from provenance authority")
		}
		if ref.Locator != "" && ref.Provenance.Locator != "" && ref.Locator != ref.Provenance.Locator {
			return fmt.Errorf("definition locator differs from provenance locator")
		}
	}
	return nil
}

func normalizeResolvedDefinition(input workflowcompile.ResolvedDefinition) (workflowcompile.ResolvedDefinition, error) {
	if err := input.Graph.ValidateEnums(); err != nil {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved graph enums: %w", err)
	}
	if err := graph.ValidateID(input.Graph.ID); err != nil {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved graph id: %w", err)
	}
	if err := validateStableString("resolved graph version", input.Graph.Version, true); err != nil {
		return workflowcompile.ResolvedDefinition{}, err
	}
	if err := values.ValidateDigest(input.Graph.Digest); err != nil {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved graph digest: %w", err)
	}
	if err := validateProvenance("resolved graph provenance", input.Graph.Provenance, true); err != nil {
		return workflowcompile.ResolvedDefinition{}, err
	}
	if input.Graph.Provenance.Digest != input.Graph.Digest {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved graph provenance digest differs from graph digest")
	}
	definitionForValidation := input.Definition
	if definitionForValidation.ID == "" && definitionForValidation.Locator == "" {
		definitionForValidation.ID = input.Graph.ID
	}
	if err := validateRequestedDefinition(definitionForValidation); err != nil {
		return workflowcompile.ResolvedDefinition{}, err
	}
	cloned, err := cloneResolvedDefinition(input)
	if err != nil {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved definition must be JSON-compatible: %w", err)
	}
	ref := &cloned.Definition
	if ref.Kind == "" {
		ref.Kind = "workflow"
	}
	if ref.ID == "" {
		ref.ID = cloned.Graph.ID
	} else if ref.ID != cloned.Graph.ID {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved definition id differs from graph id")
	}
	if ref.Version == "" {
		ref.Version = cloned.Graph.Version
	} else if ref.Version != cloned.Graph.Version {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved definition version differs from graph version")
	}
	if ref.Digest == "" {
		ref.Digest = cloned.Graph.Digest
	} else if ref.Digest != cloned.Graph.Digest {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved definition digest differs from graph digest")
	}
	if ref.Provenance == nil {
		provenance := cloned.Graph.Provenance
		ref.Provenance = &provenance
	} else if !equalProvenance(*ref.Provenance, cloned.Graph.Provenance) {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved definition provenance differs from graph provenance")
	}
	if ref.Authority == "" && ref.Provenance != nil {
		ref.Authority = ref.Provenance.Authority
	}
	if ref.Locator == "" && ref.Provenance != nil {
		ref.Locator = ref.Provenance.Locator
	}
	if err := validateRequestedDefinition(*ref); err != nil {
		return workflowcompile.ResolvedDefinition{}, err
	}
	if ref.Digest == "" {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved definition digest is required")
	}
	if err := values.ValidateDigest(ref.Digest); err != nil {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved definition digest: %w", err)
	}
	if ref.Provenance == nil {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved definition provenance is required")
	}
	if ref.Provenance.Digest == "" {
		ref.Provenance.Digest = ref.Digest
	}
	if ref.Provenance.Digest != ref.Digest {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("resolved provenance digest differs from definition digest")
	}
	return cloned, nil
}

func validateProvenance(name string, provenance graph.Provenance, requireDigest bool) error {
	for _, field := range []struct{ name, value string }{
		{"authority", provenance.Authority}, {"origin", provenance.Origin}, {"locator", provenance.Locator},
		{"revision", provenance.Revision}, {"digest", provenance.Digest},
	} {
		if err := validateStableString(name+" "+field.name, field.value, field.name == "digest" && requireDigest); err != nil {
			return err
		}
	}
	if provenance.Digest != "" {
		if err := values.ValidateDigest(provenance.Digest); err != nil {
			return fmt.Errorf("%s digest: %w", name, err)
		}
	}
	for index, parent := range provenance.Parents {
		for _, field := range []struct{ name, value string }{
			{"authority", parent.Authority}, {"locator", parent.Locator}, {"digest", parent.Digest},
		} {
			if err := validateStableString(fmt.Sprintf("%s parent[%d] %s", name, index, field.name), field.value, false); err != nil {
				return err
			}
		}
		if parent.Digest != "" {
			if err := values.ValidateDigest(parent.Digest); err != nil {
				return fmt.Errorf("%s parent[%d] digest: %w", name, index, err)
			}
		}
	}
	if provenance.Metadata != nil {
		if _, err := values.DigestInline(map[string]any(provenance.Metadata)); err != nil {
			return fmt.Errorf("%s metadata: %w", name, err)
		}
	}
	return nil
}

func equalProvenance(left, right graph.Provenance) bool {
	leftJSON, leftErr := canonicalJSON(left)
	rightJSON, rightErr := canonicalJSON(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func planRef(resolved workflowcompile.ResolvedDefinition) (workflowruntime.PlanRef, error) {
	ref := workflowruntime.PlanRef{
		ID: resolved.Definition.ID, Version: resolved.Definition.Version,
		Digest: resolved.Definition.Digest, SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion,
	}
	if err := ref.Validate(); err != nil {
		return workflowruntime.PlanRef{}, err
	}
	return ref, nil
}

func resolutionKey(identity stepkind.InvocationIdentity, requested graph.DefinitionRef) (string, error) {
	encoded, err := canonicalJSON(struct {
		Identity  CallSiteIdentity    `json:"identity"`
		Requested graph.DefinitionRef `json:"requested"`
	}{callSite(identity), requested})
	if err != nil {
		return "", err
	}
	return "call-resolution:" + hexHash(encoded), nil
}

func callSite(identity stepkind.InvocationIdentity) CallSiteIdentity {
	return CallSiteIdentity{RunID: identity.RunID, NodeID: identity.NodeID, Iteration: identity.Iteration}
}

func childRunID(resolutionKey string) workflowruntime.RunID {
	return workflowruntime.RunID("call-" + stableHash(resolutionKey))
}

func stableHash(value string) string { return hexHash([]byte(value)) }

func hexHash(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func canonicalJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	if err := requireJSONEnd(decoder); err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
}

func requireJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("multiple JSON values")
}

func cloneInvocation(invocation stepkind.Invocation) (stepkind.Invocation, error) {
	var cloned stepkind.Invocation
	if err := cloneJSON(invocation, &cloned); err != nil {
		return stepkind.Invocation{}, err
	}
	return cloned, nil
}

func cloneResolvedDefinition(input workflowcompile.ResolvedDefinition) (workflowcompile.ResolvedDefinition, error) {
	var cloned workflowcompile.ResolvedDefinition
	if err := cloneJSON(input, &cloned); err != nil {
		return workflowcompile.ResolvedDefinition{}, err
	}
	return cloned, nil
}

func cloneDefinitionRef(input graph.DefinitionRef) (graph.DefinitionRef, error) {
	var cloned graph.DefinitionRef
	if err := cloneJSON(input, &cloned); err != nil {
		return graph.DefinitionRef{}, err
	}
	return cloned, nil
}

func cloneDefinitionRefs(input []graph.DefinitionRef) []graph.DefinitionRef {
	var cloned []graph.DefinitionRef
	_ = cloneJSON(input, &cloned)
	return cloned
}

func cloneResolutionRecord(input ResolutionRecord) ResolutionRecord {
	var cloned ResolutionRecord
	_ = cloneJSON(input, &cloned)
	return cloned
}

func cloneValueSet(input values.ValueSet) values.ValueSet {
	var cloned values.ValueSet
	_ = cloneJSON(input, &cloned)
	return cloned
}

func cloneJSON(input, output any) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	return requireJSONEnd(decoder)
}

func equalResolutionRecord(left, right ResolutionRecord) bool {
	leftJSON, leftErr := canonicalJSON(left)
	rightJSON, rightErr := canonicalJSON(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func validateStableString(name, value string, required bool) error {
	if value == "" && !required {
		return nil
	}
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s is required as stable UTF-8 without surrounding whitespace", name)
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return fmt.Errorf("%s must not contain control characters", name)
		}
	}
	return nil
}

func validateDeclaredOutputs(declarations []graph.OutputSpec, output values.ValueSet) error {
	if err := values.ValidatePersistableSet(output); err != nil {
		return err
	}
	declared := make(map[string]graph.OutputSpec, len(declarations))
	for _, declaration := range declarations {
		if err := graph.ValidateID(declaration.Name); err != nil {
			return fmt.Errorf("declared output %q: %w", declaration.Name, err)
		}
		if _, exists := declared[declaration.Name]; exists {
			return fmt.Errorf("declared output %q is duplicated", declaration.Name)
		}
		declared[declaration.Name] = declaration
	}
	if len(output) != len(declared) {
		return fmt.Errorf("child returned %d outputs for %d declarations", len(output), len(declared))
	}
	names := make([]string, 0, len(declared))
	for name := range declared {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value, exists := output[name]
		if !exists {
			return fmt.Errorf("declared child output %q is missing", name)
		}
		if err := values.ValidateValueSchema(declared[name].Schema, value); err != nil {
			return fmt.Errorf("declared child output %q: %w", name, err)
		}
	}
	return nil
}

func validateChildRunResult(parent stepkind.InvocationIdentity, childID workflowruntime.RunID, plan workflowruntime.PlanRef, policy graph.ParentClosePolicy, result ChildRunResult) error {
	if err := result.Run.Validate(); err != nil {
		return fmt.Errorf("child run: %w", err)
	}
	if result.Run.ID != childID || result.Run.Plan != plan {
		return fmt.Errorf("child run identity or plan differs from request")
	}
	if err := result.Link.Validate(); err != nil {
		return fmt.Errorf("child link: %w", err)
	}
	expectedInvocation := workflowruntime.NodeInvocationID{
		RunID: workflowruntime.RunID(parent.RunID), NodeID: parent.NodeID, Iteration: parent.Iteration,
	}
	if result.Link.ParentRunID != expectedInvocation.RunID || result.Link.Invocation != expectedInvocation ||
		result.Link.ChildRunID != childID || result.Link.Policy != policy {
		return fmt.Errorf("child run link differs from request")
	}
	if err := validateStableString("child events reference", result.EventsRef, true); err != nil {
		return err
	}
	if result.Cancellation.RunID != childID || result.Cancellation.Policy != policy {
		return fmt.Errorf("child cancellation handle differs from request")
	}
	return validateStableString("child cancellation reference", result.Cancellation.Ref, true)
}

func childHandleValues(parent stepkind.InvocationIdentity, result ChildRunResult) (values.ValueSet, error) {
	reference := strings.Join([]string{parent.RunID, parent.NodeID, parent.Iteration, strconv.Itoa(parent.Attempt)}, "/")
	metadata := func(output string) values.Metadata {
		return values.Metadata{
			Producer:  values.Producer{Kind: "call_run", Reference: reference, Output: output},
			MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		}
	}
	outputsRef := any(nil)
	if result.Run.Outputs != nil {
		outputsRef = map[string]any{"id": result.Run.Outputs.ID, "digest": result.Run.Outputs.Digest}
	}
	payloads := map[string]any{
		OutputRunID: string(result.Run.ID), OutputStatus: string(result.Run.Status),
		OutputEventsRef: result.EventsRef,
		OutputCancellation: map[string]any{
			"run_id": string(result.Cancellation.RunID), "policy": string(result.Cancellation.Policy), "ref": result.Cancellation.Ref,
		},
		OutputOutputsRef: outputsRef,
	}
	outputs := make(values.ValueSet, len(payloads))
	for _, name := range []string{OutputRunID, OutputStatus, OutputEventsRef, OutputCancellation, OutputOutputsRef} {
		value, err := values.NewInline(payloads[name], metadata(name))
		if err != nil {
			return nil, err
		}
		outputs[name] = value
	}
	return outputs, nil
}
