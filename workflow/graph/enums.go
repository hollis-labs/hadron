package graph

// BindingKind identifies how a binding obtains its value.
type BindingKind string

const (
	// BindingLiteral carries a literal JSON-compatible value.
	BindingLiteral BindingKind = "literal"
	// BindingExpression carries a raw expression-language value.
	BindingExpression BindingKind = "expression"
	// BindingInterpolation carries a string containing interpolations.
	BindingInterpolation BindingKind = "interpolation"
)

// Valid reports whether k is a supported binding kind.
func (k BindingKind) Valid() bool {
	return oneOf(k, BindingLiteral, BindingExpression, BindingInterpolation)
}

// EdgeKind identifies whether an edge exists for ordering or value visibility.
type EdgeKind string

const (
	// EdgeControl is an ordering-only dependency.
	EdgeControl EdgeKind = "control"
	// EdgeData permits downstream value visibility in addition to ordering.
	EdgeData EdgeKind = "data"
)

// Valid reports whether k is a supported edge kind.
func (k EdgeKind) Valid() bool { return oneOf(k, EdgeControl, EdgeData) }

// ReadyRule selects dependency readiness behavior.
type ReadyRule string

const (
	// ReadyAllSuccess requires every dependency to succeed.
	ReadyAllSuccess ReadyRule = "all_success"
	// ReadyAllDone requires every dependency to reach a terminal state.
	ReadyAllDone ReadyRule = "all_done"
	// ReadyOneFailed requires at least one dependency to fail.
	ReadyOneFailed ReadyRule = "one_failed"
	// ReadyAllFailed requires every dependency to fail.
	ReadyAllFailed ReadyRule = "all_failed"
	// ReadyNoneFailed requires no dependency to fail.
	ReadyNoneFailed ReadyRule = "none_failed"
	// ReadyAlways ignores dependency outcomes after they become observable.
	ReadyAlways ReadyRule = "always"
)

// Valid reports whether r is a supported readiness rule.
func (r ReadyRule) Valid() bool {
	return oneOf(r, ReadyAllSuccess, ReadyAllDone, ReadyOneFailed, ReadyAllFailed, ReadyNoneFailed, ReadyAlways)
}

// Effect classifies the externally observable effect of a node.
type Effect string

const (
	// EffectRead observes state without changing it.
	EffectRead Effect = "read"
	// EffectCompute performs deterministic or isolated computation.
	EffectCompute Effect = "compute"
	// EffectMaterialize creates derived artifacts or resources.
	EffectMaterialize Effect = "materialize"
	// EffectMutate changes existing external state.
	EffectMutate Effect = "mutate"
	// EffectDestructive removes or irreversibly changes external state.
	EffectDestructive Effect = "destructive"
)

// Valid reports whether e is a supported effect class.
func (e Effect) Valid() bool {
	return oneOf(e, EffectRead, EffectCompute, EffectMaterialize, EffectMutate, EffectDestructive)
}

// EffectSet is the declared set of effects for a graph node.
type EffectSet []Effect

// BackoffStrategy selects retry delay growth.
type BackoffStrategy string

const (
	// BackoffNone retries without an engine-introduced delay.
	BackoffNone BackoffStrategy = "none"
	// BackoffFixed uses the initial delay for every retry.
	BackoffFixed BackoffStrategy = "fixed"
	// BackoffLinear increases delay by a constant multiple.
	BackoffLinear BackoffStrategy = "linear"
	// BackoffExponential multiplies delay after each attempt.
	BackoffExponential BackoffStrategy = "exponential"
)

// Valid reports whether s is a supported backoff strategy.
func (s BackoffStrategy) Valid() bool {
	return oneOf(s, BackoffNone, BackoffFixed, BackoffLinear, BackoffExponential)
}

// IdempotencyMode describes how a node supports safe duplicate attempts.
type IdempotencyMode string

const (
	// IdempotencyNone makes no idempotency claim.
	IdempotencyNone IdempotencyMode = "none"
	// IdempotencyIntrinsic means the operation is inherently idempotent.
	IdempotencyIntrinsic IdempotencyMode = "intrinsic"
	// IdempotencyKeyed means the resolved key identifies duplicate operations.
	IdempotencyKeyed IdempotencyMode = "keyed"
)

// Valid reports whether m is a supported idempotency mode.
func (m IdempotencyMode) Valid() bool {
	return oneOf(m, IdempotencyNone, IdempotencyIntrinsic, IdempotencyKeyed)
}

// CallMode selects child workflow run identity.
type CallMode string

const (
	// CallInline executes a child definition inside the parent run identity.
	CallInline CallMode = "inline"
	// CallRun creates separately identifiable child-run state.
	CallRun CallMode = "run"
)

// Valid reports whether m is a supported call mode.
func (m CallMode) Valid() bool { return oneOf(m, CallInline, CallRun) }

// ParentClosePolicy controls a child run when its parent closes.
type ParentClosePolicy string

const (
	// ParentCloseCancel cancels the child with the parent.
	ParentCloseCancel ParentClosePolicy = "cancel"
	// ParentCloseAbandon leaves the child running.
	ParentCloseAbandon ParentClosePolicy = "abandon"
	// ParentCloseRequestCancel requests cancellation through the child host.
	ParentCloseRequestCancel ParentClosePolicy = "request_cancel"
)

// Valid reports whether p is a supported parent-close policy.
func (p ParentClosePolicy) Valid() bool {
	return oneOf(p, ParentCloseCancel, ParentCloseAbandon, ParentCloseRequestCancel)
}

// RunCompletionMode selects behavior after an unhandled branch failure.
type RunCompletionMode string

const (
	// CompletionFailFast stops or skips remaining eligible work according to policy.
	CompletionFailFast RunCompletionMode = "fail_fast"
	// CompletionRunToCompletion continues independent branches.
	CompletionRunToCompletion RunCompletionMode = "run_to_completion"
)

// Valid reports whether m is a supported run completion mode.
func (m RunCompletionMode) Valid() bool {
	return oneOf(m, CompletionFailFast, CompletionRunToCompletion)
}

// DurabilityMode selects the workflow persistence contract.
type DurabilityMode string

const (
	// DurabilityNone selects the validated non-durable subset.
	DurabilityNone DurabilityMode = "none"
	// DurabilitySteps persists node-level progress.
	DurabilitySteps DurabilityMode = "steps"
)

// Valid reports whether m is a supported durability mode.
func (m DurabilityMode) Valid() bool { return oneOf(m, DurabilityNone, DurabilitySteps) }

// SourceFormat identifies the frontend that produced graph material.
type SourceFormat string

const (
	// SourceWorkflow is the graph-native workflow source format.
	SourceWorkflow SourceFormat = "workflow"
	// SourceArchivedBlueprint is legacy blueprint reference material.
	SourceArchivedBlueprint SourceFormat = "archived-blueprint"
	// SourceArchivedPipeline is legacy pipeline reference material.
	SourceArchivedPipeline SourceFormat = "archived-pipeline"
	// SourceSDK is graph material emitted through an SDK.
	SourceSDK SourceFormat = "sdk"
	// SourceUI is graph material emitted through a user interface.
	SourceUI SourceFormat = "ui"
)

// Valid reports whether f is a supported source format.
func (f SourceFormat) Valid() bool {
	return oneOf(f, SourceWorkflow, SourceArchivedBlueprint, SourceArchivedPipeline, SourceSDK, SourceUI)
}

// OverlapPolicy controls overlapping source activations.
type OverlapPolicy string

const (
	// OverlapAllow permits overlapping runs.
	OverlapAllow OverlapPolicy = "allow"
	// OverlapForbid rejects a new activation while an earlier run is active.
	OverlapForbid OverlapPolicy = "forbid"
	// OverlapReplace replaces an earlier active run according to host policy.
	OverlapReplace OverlapPolicy = "replace"
)

// Valid reports whether p is a supported overlap policy.
func (p OverlapPolicy) Valid() bool { return oneOf(p, OverlapAllow, OverlapForbid, OverlapReplace) }

// RunIDReusePolicy controls activation behavior when a run ID already exists.
type RunIDReusePolicy string

const (
	// RunIDReuseReject rejects an existing run ID.
	RunIDReuseReject RunIDReusePolicy = "reject"
	// RunIDReuseAllowDuplicate allows a duplicate logical activation.
	RunIDReuseAllowDuplicate RunIDReusePolicy = "allow_duplicate"
	// RunIDReuseTerminateExisting terminates the prior run before replacement.
	RunIDReuseTerminateExisting RunIDReusePolicy = "terminate_existing"
)

// Valid reports whether p is a supported run ID reuse policy.
func (p RunIDReusePolicy) Valid() bool {
	return oneOf(p, RunIDReuseReject, RunIDReuseAllowDuplicate, RunIDReuseTerminateExisting)
}

func oneOf[T comparable](value T, allowed ...T) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}
