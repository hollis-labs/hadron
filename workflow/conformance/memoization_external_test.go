package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/conformance"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

func TestMemoizationConformanceFixturesUseProductionContracts(t *testing.T) {
	conformance.MemoizationSuite(t, conformance.EmbeddedFixtures(), func() (conformance.Runner, error) { return conformance.RunnerFunc(runMemoFixture), nil })
}

func runMemoFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input struct {
		Scenario string `json:"scenario"`
	}
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return err
	}
	base := time.Date(2026, time.August, 24, 18, 0, 0, 0, time.UTC)
	digest := values.SHA256Digest([]byte("fixture"))
	ref := values.ValueSetRef{ID: "values-1", Digest: digest}
	source := workflowruntime.NodeInvocationID{RunID: "source", NodeID: "work"}
	entry := workflowruntime.MemoEntry{Key: digest, PlanDigest: digest, NodeID: "work", Kind: "transform", KindVersion: "v1", MemoKeyDigest: digest, InputDigest: digest, OutputSchemaDigest: digest, OutputDigest: digest, Outputs: ref, Source: source, SourceAttempt: workflowruntime.AttemptID{Invocation: source, Number: 1}, SourceOrigin: workflowruntime.OriginExecuted, Effects: graph.EffectSet{graph.EffectCompute}, Policy: workflowruntime.ReusePolicyDecision{Allow: true, Code: "safe", Reason: "compute"}, CreatedAt: base, ExpiresAt: base.Add(time.Hour)}
	switch input.Scenario {
	case "safe_entry":
		return entry.Validate()
	case "expired":
		return entry.FreshAt(base.Add(2*time.Hour), time.Hour)
	case "pin_binding":
		return (workflowruntime.PinBinding{Target: workflowruntime.NodeInvocationID{RunID: "target", NodeID: "work"}, PlanDigest: digest, Outputs: ref, OutputSchemaDigest: digest, Source: source, SourcePlanDigest: digest, SourceOrigin: workflowruntime.OriginExecuted, Authority: workflowruntime.ReuseAuthority{Principal: "developer"}, Policy: workflowruntime.ReusePolicyDecision{Allow: true, Code: "pin", Reason: "authorized"}, BoundAt: base}).Validate()
	case "unsafe_effect":
		kind := stepkindtest.NewNoopKind("writer", "v1")
		kind.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
		registry := stepkind.NewRegistry()
		if err := registry.Register(kind); err != nil {
			return err
		}
		workflow := graph.Graph{ID: "unsafe", Version: "v1", Digest: digest, Nodes: []graph.Node{{ID: "write", Kind: "writer", KindVersion: "v1", Memoization: &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.key"}, MaxAge: "1h"}}}}
		findings := compile.ValidateGraph(ctx, workflow, compile.ValidationOptions{StepKinds: registry, Verifiers: verification.NewDefaultRegistry()})
		if len(findings) == 0 {
			return nil
		}
		return errors.New(string(findings[0].Code))
	default:
		return errors.New("unknown memoization scenario")
	}
}
