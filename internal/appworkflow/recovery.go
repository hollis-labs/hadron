package appworkflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/runtime"
)

// RecoveryDependencySource exposes the frozen compiler dependency extractors
// used by a DefinitionProvider. Recovery must infer visibility with those
// exact semantics rather than widening context from graph topology.
type RecoveryDependencySource interface {
	RecoveryDependencyOptions() compile.DependencyOptions
}

func compileDependencyOptions(definitions DefinitionProvider) compile.DependencyOptions {
	provider, ok := definitions.(RecoveryDependencySource)
	if !ok || nilInterface(provider) {
		return compile.DependencyOptions{}
	}
	options := provider.RecoveryDependencyOptions()
	extractors := make(map[string]compile.VerificationExpressionExtractor, len(options.VerificationExtractors))
	for name, extractor := range options.VerificationExtractors {
		extractors[name] = extractor
	}
	return compile.DependencyOptions{VerificationExtractors: extractors}
}

// PinnedRecoveryPlanSource resolves recovery graphs only from the immutable
// root start journal or the exact child-start request. It never re-resolves a
// definition locator. This preserves the W03-T08 cancellation-tree ordering
// contract for both root and separately hosted call.mode:run children.
type PinnedRecoveryPlanSource struct {
	Roots    hoststate.Journal
	Children ChildRunDefinitionSource
	State    runtime.StateStore
	Replays  runtime.ReplayStore
	// DependencyOptions must be the same frozen host-owned verifier extractor
	// configuration used when the pinned definition was compiled.
	DependencyOptions compile.DependencyOptions
}

func (s PinnedRecoveryPlanSource) LoadRecoveryPlan(ctx context.Context, run runtime.RunSnapshot) (runtime.RecoveryPlan, error) {
	return s.loadRecoveryPlan(ctx, run, make(map[runtime.RunID]struct{}), 0)
}

func (s PinnedRecoveryPlanSource) loadRecoveryPlan(ctx context.Context, run runtime.RunSnapshot, seen map[runtime.RunID]struct{}, depth int) (runtime.RecoveryPlan, error) {
	if ctx == nil || nilInterface(s.Roots) {
		return runtime.RecoveryPlan{}, fmt.Errorf("%w: pinned recovery roots are required", ErrInvalidHost)
	}
	if depth > 64 {
		return runtime.RecoveryPlan{}, fmt.Errorf("%w: replay provenance depth exceeds 64", ErrInvalidHost)
	}
	if _, duplicate := seen[run.ID]; duplicate {
		return runtime.RecoveryPlan{}, fmt.Errorf("%w: replay provenance cycle at run %q", ErrInvalidHost, run.ID)
	}
	seen[run.ID] = struct{}{}
	defer delete(seen, run.ID)
	root, err := s.Roots.LoadStart(ctx, run.ID)
	if err == nil {
		ref := runtime.PlanRef{ID: root.Record.Plan.ID, Version: root.Record.Plan.Graph.Version, Digest: root.Record.Plan.Digest, SchemaVersion: root.Record.Plan.SchemaVersion}
		if ref != run.Plan || root.Record.Run.ID != run.ID || root.Record.Run.Plan != run.Plan {
			return runtime.RecoveryPlan{}, fmt.Errorf("%w: root recovery plan differs from run", ErrInvalidHost)
		}
		return recoveryPlan(root.Record.Plan, ref, s.DependencyOptions, true)
	}
	if !errors.Is(err, runtime.ErrNotFound) {
		return runtime.RecoveryPlan{}, err
	}
	if !nilInterface(s.Children) {
		child, childErr := s.Children.LoadChildRunRequest(ctx, run.ID)
		if childErr == nil {
			if child.ChildRunID != run.ID || child.Plan != run.Plan || child.Definition.Graph.ID != run.Plan.ID || child.Definition.Graph.Version != run.Plan.Version {
				return runtime.RecoveryPlan{}, fmt.Errorf("%w: child recovery plan differs from pinned start", ErrInvalidHost)
			}
			plan := compile.ExecutionPlan{SchemaVersion: child.Plan.SchemaVersion, ID: child.Plan.ID, Digest: child.Plan.Digest, Definition: child.Definition.Definition, Provenance: child.Definition.Graph.Provenance, Graph: child.Definition.Graph}
			return recoveryPlan(plan, child.Plan, s.DependencyOptions, false)
		}
		if !errors.Is(childErr, runtime.ErrNotFound) {
			return runtime.RecoveryPlan{}, childErr
		}
	}
	if nilInterface(s.State) || nilInterface(s.Replays) {
		return runtime.RecoveryPlan{}, runtime.ErrNotFound
	}
	provenance, replayErr := s.Replays.LoadReplayProvenance(ctx, run.ID)
	if replayErr != nil {
		return runtime.RecoveryPlan{}, replayErr
	}
	if provenance.RunID != run.ID || provenance.PlanDigest != run.Plan.Digest {
		return runtime.RecoveryPlan{}, fmt.Errorf("%w: replay provenance differs from target run plan", ErrInvalidHost)
	}
	source, sourceErr := s.State.LoadRun(ctx, provenance.SourceRunID)
	if sourceErr != nil {
		return runtime.RecoveryPlan{}, sourceErr
	}
	if source.Plan != run.Plan {
		return runtime.RecoveryPlan{}, fmt.Errorf("%w: replay source and target plans differ", ErrInvalidHost)
	}
	return s.loadRecoveryPlan(ctx, source, seen, depth+1)
}

func recoveryPlan(plan compile.ExecutionPlan, ref runtime.PlanRef, options compile.DependencyOptions, requireCanonicalDigest bool) (runtime.RecoveryPlan, error) {
	inferred := compile.InferValueDependencies(&plan, options)
	if inferred.Plan == nil || len(inferred.Diagnostics) != 0 {
		return runtime.RecoveryPlan{}, fmt.Errorf("%w: pinned recovery dependency inference failed", ErrInvalidHost)
	}
	if requireCanonicalDigest && inferred.Plan.Digest != ref.Digest {
		return runtime.RecoveryPlan{}, fmt.Errorf("%w: pinned recovery plan is missing canonical dependency inference", ErrInvalidHost)
	}
	// Dependency inference may add compiler-owned value arcs to an older
	// serialized plan. The immutable plan identity/digest remains the pinned
	// source authority; recovery executes the compiler-produced derived plan.
	derived := *inferred.Plan
	// Child-start records pin the exact inferred graph and PlanRef but do not
	// duplicate the root-only source map/bundle envelope. Bind that derived
	// graph back to the immutable child PlanRef after inference.
	if !requireCanonicalDigest {
		computed, digestErr := compile.GraphDigest(plan.Graph)
		if digestErr != nil || plan.Graph.Digest != ref.Digest || computed != ref.Digest {
			return runtime.RecoveryPlan{}, fmt.Errorf("%w: pinned child graph digest does not match its plan reference", ErrInvalidHost)
		}
		inferredDigest, digestErr := compile.GraphDigest(inferred.Plan.Graph)
		if digestErr != nil || inferredDigest != ref.Digest {
			return runtime.RecoveryPlan{}, fmt.Errorf("%w: dependency inference changes pinned child graph content", ErrInvalidHost)
		}
		originalGraph, inferredGraph := plan.Graph, inferred.Plan.Graph
		originalGraph.Digest, inferredGraph.Digest = "", ""
		if !reflect.DeepEqual(originalGraph, inferredGraph) {
			return runtime.RecoveryPlan{}, fmt.Errorf("%w: pinned child graph changes under dependency inference", ErrInvalidHost)
		}
		derived = plan
		derived.ID, derived.Digest, derived.SchemaVersion = ref.ID, ref.Digest, ref.SchemaVersion
	}
	result := runtime.RecoveryPlan{Ref: ref, Plan: derived, Visibility: inferred.Visibility}
	if err := result.Validate(); err != nil {
		return runtime.RecoveryPlan{}, fmt.Errorf("%w: %w", ErrInvalidHost, err)
	}
	return result, nil
}

// CoreRecoveryHook adapts the extraction-ready coordinator to Hadron's
// existing post-child-materialization/post-cancellation RecoveryHook seam.
// Waits belongs on Coordinator so crash/fail-fast admission fences are durable
// before a due wait publishes Ready work. Extension hooks run afterward.
type CoreRecoveryHook struct {
	Coordinator *runtime.RecoveryCoordinator
	Limit       int
}

func (h CoreRecoveryHook) RecoverWorkflow(ctx context.Context, _ runtime.RecoverySnapshot, now time.Time) error {
	if h.Coordinator == nil || h.Limit < 0 {
		return fmt.Errorf("%w: recovery coordinator and non-negative limit are required", ErrInvalidHost)
	}
	_, err := h.Coordinator.Recover(ctx, runtime.RecoveryRequest{Now: now, Limit: h.Limit})
	return err
}
