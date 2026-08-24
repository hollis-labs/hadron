package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/conformance"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

func TestVerificationConformanceFixturesReferenceProductionContracts(t *testing.T) {
	conformance.VerificationSuite(t, conformance.EmbeddedFixtures(), func() (conformance.Runner, error) {
		return conformance.RunnerFunc(runVerificationFixture), nil
	})
}

func runVerificationFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input struct {
		Scenario string `json:"scenario"`
	}
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return err
	}
	switch input.Scenario {
	case "deterministic_pass", "deterministic_fail":
		value, err := values.NewInline(input.Scenario == "deterministic_pass", values.Metadata{
			Producer: values.Producer{Kind: "conformance", Reference: "verification", Output: "ok"}, MediaType: "application/json",
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return err
		}
		verifier, _, err := verification.Resolve(verification.NewDefaultRegistry(), verification.CheckPredicate)
		if err != nil {
			return err
		}
		result, err := verifier.Verify(ctx, verification.Request{Check: graph.VerificationCheck{Kind: verification.CheckPredicate, Config: graph.Config{"expression": "inputs.ok"}}, Outputs: values.ValueSet{"ok": value}})
		if err != nil {
			return err
		}
		if result.Outcome != verification.CheckPassed {
			return errors.New(result.Code)
		}
		return nil
	case "missing_evidence":
		verifier, _, err := verification.Resolve(verification.NewDefaultRegistry(), verification.CheckExpectedToolCall)
		if err != nil {
			return err
		}
		result, err := verifier.Verify(ctx, verification.Request{Check: graph.VerificationCheck{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"tool": "write"}}})
		if err != nil {
			return err
		}
		if result.Outcome != verification.CheckPassed {
			return errors.New(result.Code)
		}
		return nil
	case "retry_safety":
		decision, err := (workflowruntime.RetryEvaluator{}).Evaluate(ctx, workflowruntime.RetryEvaluationRequest{
			Node:          graph.Node{ID: "unsafe", Retry: &graph.RetryPolicy{Attempts: 2, On: []string{"verification_failed"}}},
			Spec:          stepkind.StepKindSpec{Effects: graph.EffectSet{graph.EffectDestructive}, Idempotency: graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency},
			AttemptNumber: 1, Failure: workflowruntime.Failure{Code: "verification_failed", Message: "decision failed", Retryable: true},
			AttemptStatus: workflowruntime.NodeFailed, FailedAt: time.Date(2026, 8, 24, 20, 0, 0, 0, time.UTC),
		})
		if err != nil {
			return err
		}
		if decision.Retry || decision.Reason != workflowruntime.RetryReasonIdempotencyMissing {
			return fmt.Errorf("unsafe retry decision = %#v", decision)
		}
		return nil
	case "catch_route":
		store, runID, base, err := newControlFlowFixtureStore(ctx, "verification-catch")
		if err != nil {
			return err
		}
		if createErr := createControlNode(ctx, store, runID, "source", base); createErr != nil {
			return createErr
		}
		if createErr := createControlNode(ctx, store, runID, "handler", base); createErr != nil {
			return createErr
		}
		sourceID := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}
		if finishErr := finishControlNode(ctx, store, sourceID, workflowruntime.NodeFailed, "verification_failed", base.Add(time.Second)); finishErr != nil {
			return finishErr
		}
		node := graph.Node{ID: "source", Catch: []graph.CatchRule{{Errors: []string{"verification_failed"}, Targets: []string{"handler"}}}}
		decision, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).DecideCatch(ctx, workflowruntime.DecideCatchRequest{Source: sourceID, Node: node, At: base.Add(2 * time.Second)})
		if err != nil {
			return err
		}
		if decision.Decision.Outcome != workflowruntime.ControlSelected || len(decision.Decision.Targets) != 1 || decision.Decision.Targets[0].NodeID != "handler" {
			return fmt.Errorf("catch decision = %#v", decision)
		}
		return nil
	case "reviewer_malformed":
		_, err := verification.ParseReviewerDecision([]byte(`{"passed":true,"passed":false,"code":"ambiguous","message":"duplicate"}`))
		return err
	default:
		return fmt.Errorf("unknown verification scenario %q", input.Scenario)
	}
}
