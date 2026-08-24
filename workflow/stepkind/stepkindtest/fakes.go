// Package stepkindtest provides application-neutral fake step kinds for
// compiler, runtime, and conformance tests.
package stepkindtest

import (
	"context"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
)

// Kind is a fake implementing only the required StepKind lifecycle.
type Kind struct {
	SpecValue          stepkind.StepKindSpec
	ValidateConfigFunc func(context.Context, graph.Config) []diagnostic.Diagnostic
	ExecuteFunc        func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error)
}

// NewNoopKind returns a valid no-op kind with no optional lifecycle hooks.
func NewNoopKind(name, version string) *Kind {
	return &Kind{SpecValue: NoopSpec(name, version)}
}

// NoopSpec returns complete metadata for a pure embedded fake kind.
func NoopSpec(name, version string) stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name:                  name,
		Version:               version,
		ConfigSchema:          graph.Schema{},
		InputSchema:           graph.Schema{},
		OutputSchema:          graph.Schema{},
		Effects:               graph.EffectSet{graph.EffectCompute},
		Idempotency:           graph.IdempotencyIntrinsic,
		RetrySafety:           stepkind.RetrySafe,
		Cancellation:          stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:           stepkind.ObservationSpec{Mode: stepkind.ObservationNone},
		EmbeddedModeSupported: true,
	}
}

// Spec implements stepkind.StepKind.
func (k *Kind) Spec() stepkind.StepKindSpec { return k.SpecValue }

// ValidateConfig implements stepkind.StepKind.
func (k *Kind) ValidateConfig(ctx context.Context, config graph.Config) []diagnostic.Diagnostic {
	if k.ValidateConfigFunc == nil {
		return nil
	}
	return k.ValidateConfigFunc(ctx, config)
}

// Execute implements stepkind.StepKind.
func (k *Kind) Execute(ctx context.Context, invocation stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if k.ExecuteFunc == nil {
		return stepkind.StepResult{}, nil
	}
	return k.ExecuteFunc(ctx, invocation)
}

// LifecycleKind is a fake implementing every optional lifecycle interface.
type LifecycleKind struct {
	*Kind
	PrepareFunc  func(context.Context, stepkind.Invocation) (stepkind.PreparedInvocation, error)
	ObserveFunc  func(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error)
	CancelFunc   func(context.Context, stepkind.ExternalOperationRef) error
	FinalizeFunc func(context.Context, stepkind.Finalization) error
}

// NewLifecycleKind returns a no-op kind that advertises every optional hook.
func NewLifecycleKind(name, version string) *LifecycleKind {
	required := NewNoopKind(name, version)
	required.SpecValue.Lifecycle.Prepare = true
	required.SpecValue.Lifecycle.Finalize = true
	required.SpecValue.Observation.Mode = stepkind.ObservationPoll
	required.SpecValue.Cancellation.Mode = stepkind.CancellationExplicit
	return &LifecycleKind{Kind: required}
}

// Prepare implements stepkind.Preparer.
func (k *LifecycleKind) Prepare(ctx context.Context, invocation stepkind.Invocation) (stepkind.PreparedInvocation, error) {
	if k.PrepareFunc == nil {
		return stepkind.PreparedInvocation{Invocation: invocation}, nil
	}
	return k.PrepareFunc(ctx, invocation)
}

// Observe implements stepkind.Observer.
func (k *LifecycleKind) Observe(ctx context.Context, ref stepkind.ExternalOperationRef) (stepkind.Observation, error) {
	if k.ObserveFunc == nil {
		return stepkind.Observation{Complete: true}, nil
	}
	return k.ObserveFunc(ctx, ref)
}

// Cancel implements stepkind.Canceler.
func (k *LifecycleKind) Cancel(ctx context.Context, ref stepkind.ExternalOperationRef) error {
	if k.CancelFunc == nil {
		return nil
	}
	return k.CancelFunc(ctx, ref)
}

// Finalize implements stepkind.Finalizer.
func (k *LifecycleKind) Finalize(ctx context.Context, finalization stepkind.Finalization) error {
	if k.FinalizeFunc == nil {
		return nil
	}
	return k.FinalizeFunc(ctx, finalization)
}

var (
	_ stepkind.StepKind  = (*Kind)(nil)
	_ stepkind.StepKind  = (*LifecycleKind)(nil)
	_ stepkind.Preparer  = (*LifecycleKind)(nil)
	_ stepkind.Observer  = (*LifecycleKind)(nil)
	_ stepkind.Canceler  = (*LifecycleKind)(nil)
	_ stepkind.Finalizer = (*LifecycleKind)(nil)
)
