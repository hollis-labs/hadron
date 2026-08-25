package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
)

func TestRetryEvaluatorBackoffClassesAttemptsAndTimeouts(t *testing.T) {
	base := time.Date(2026, time.August, 24, 14, 0, 0, 0, time.UTC)
	node := graph.Node{ID: "fetch", Retry: &graph.RetryPolicy{
		Attempts: 4, On: []string{"rate_limited", "heartbeat"},
		Backoff: graph.BackoffPolicy{Strategy: graph.BackoffExponential, InitialDelay: "2s", MaxDelay: "5s", Multiplier: 2},
	}}
	spec := stepkind.StepKindSpec{Effects: graph.EffectSet{graph.EffectRead}, Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetrySafe}
	evaluator := workflowruntime.RetryEvaluator{}

	decision, err := evaluator.Evaluate(context.Background(), workflowruntime.RetryEvaluationRequest{
		Node: node, Spec: spec, AttemptNumber: 1, Failure: workflowruntime.Failure{Code: "rate_limited", Message: "retry", Retryable: true}, AttemptStatus: workflowruntime.NodeFailed, FailedAt: base,
	})
	if err != nil || !decision.Retry || decision.MatchedClass != "rate_limited" || decision.Delay != 2*time.Second || !decision.FireAt.Equal(base.Add(2*time.Second)) {
		t.Fatalf("first retry = %#v, %v", decision, err)
	}
	decision, err = evaluator.Evaluate(context.Background(), workflowruntime.RetryEvaluationRequest{
		Node: node, Spec: spec, AttemptNumber: 3, Failure: workflowruntime.TimeoutFailure(workflowruntime.TimeoutHeartbeat, base), AttemptStatus: workflowruntime.NodeTimedOut, Timeout: workflowruntime.TimeoutHeartbeat, FailedAt: base,
	})
	if err != nil || !decision.Retry || decision.MatchedClass != "heartbeat" || decision.Delay != 5*time.Second {
		t.Fatalf("timeout retry = %#v, %v", decision, err)
	}
	decision, err = evaluator.Evaluate(context.Background(), workflowruntime.RetryEvaluationRequest{
		Node: node, Spec: spec, AttemptNumber: 4, Failure: workflowruntime.Failure{Code: "rate_limited", Message: "retry", Retryable: true}, AttemptStatus: workflowruntime.NodeFailed, FailedAt: base,
	})
	if err != nil || decision.Retry || decision.Reason != workflowruntime.RetryReasonAttemptsExhausted {
		t.Fatalf("exhausted retry = %#v, %v", decision, err)
	}
}

func TestRetryEvaluatorUsesTrustedEffectUnion(t *testing.T) {
	base := time.Date(2026, time.August, 24, 14, 0, 0, 0, time.UTC)
	node := graph.Node{
		ID: "delete", Effects: graph.EffectSet{graph.EffectRead},
		Retry:       &graph.RetryPolicy{Attempts: 2, On: []string{"retryable"}},
		Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed},
	}
	spec := stepkind.StepKindSpec{Effects: graph.EffectSet{graph.EffectDestructive}, Idempotency: graph.IdempotencyKeyed, RetrySafety: stepkind.RetrySafe}
	request := workflowruntime.RetryEvaluationRequest{
		Node: node, Spec: spec, AttemptNumber: 1, IdempotencyKey: "delete:42",
		Failure: workflowruntime.Failure{Code: "temporary", Message: "retry", Retryable: true}, AttemptStatus: workflowruntime.NodeFailed, FailedAt: base,
	}
	decision, err := (workflowruntime.RetryEvaluator{}).Evaluate(context.Background(), request)
	if err != nil || decision.Retry || decision.Reason != workflowruntime.RetryReasonEffectDenied {
		t.Fatalf("untrusted effect narrowing bypassed destructive gate: %#v, %v", decision, err)
	}
	authorized := workflowruntime.RetryEvaluator{Authorizer: retryAuthorizerFunc(func(context.Context, workflowruntime.RetryAuthorizationRequest) error { return nil })}
	decision, err = authorized.Evaluate(context.Background(), request)
	if err != nil || !decision.Retry {
		t.Fatalf("authorized destructive retry = %#v, %v", decision, err)
	}
	request.Spec.RetrySafety = stepkind.RetryRequiresIdempotency
	decision, err = authorized.Evaluate(context.Background(), request)
	if err != nil || !decision.Retry {
		t.Fatalf("authorized keyed destructive retry = %#v, %v", decision, err)
	}
	decision, err = (workflowruntime.RetryEvaluator{}).Evaluate(context.Background(), request)
	if err != nil || decision.Retry || decision.Reason != workflowruntime.RetryReasonEffectDenied {
		t.Fatalf("unauthorized keyed destructive retry = %#v, %v", decision, err)
	}
	request.Spec.Idempotency = graph.IdempotencyIntrinsic
	request.Node.Idempotency.Mode = graph.IdempotencyIntrinsic
	request.IdempotencyKey = ""
	decision, err = authorized.Evaluate(context.Background(), request)
	if err != nil || !decision.Retry {
		t.Fatalf("authorized intrinsic destructive retry = %#v, %v", decision, err)
	}
	request.Spec.Idempotency = graph.IdempotencyKeyed
	request.Node.Idempotency.Mode = graph.IdempotencyKeyed
	request.IdempotencyKey = ""
	decision, err = authorized.Evaluate(context.Background(), request)
	if err != nil || decision.Retry || decision.Reason != workflowruntime.RetryReasonIdempotencyMissing {
		t.Fatalf("missing idempotency key = %#v, %v", decision, err)
	}
	request.IdempotencyKey = "delete:42"
	request.Spec.Idempotency = graph.IdempotencyNone
	decision, err = authorized.Evaluate(context.Background(), request)
	if err != nil || decision.Retry || decision.Reason != workflowruntime.RetryReasonEffectDenied {
		t.Fatalf("untrusted key did not preserve destructive denial: %#v, %v", decision, err)
	}
	request.Spec.Idempotency = graph.IdempotencyKeyed
	request.Spec.RetrySafety = stepkind.RetryUnsupported
	decision, err = authorized.Evaluate(context.Background(), request)
	if err != nil || decision.Retry || decision.Reason != workflowruntime.RetryReasonKindUnsupported {
		t.Fatalf("unsupported destructive retry = %#v, %v", decision, err)
	}
}

func TestRetryEvaluatorDoesNotTrustGraphIdempotencyUpgrade(t *testing.T) {
	base := time.Date(2026, time.August, 24, 14, 0, 0, 0, time.UTC)
	request := workflowruntime.RetryEvaluationRequest{
		Node: graph.Node{
			ID: "keyed-kind", Retry: &graph.RetryPolicy{Attempts: 2, On: []string{"retryable"}},
			Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyIntrinsic},
		},
		Spec: stepkind.StepKindSpec{
			Effects: graph.EffectSet{graph.EffectRead}, Idempotency: graph.IdempotencyKeyed,
			RetrySafety: stepkind.RetryRequiresIdempotency,
		},
		AttemptNumber: 1,
		Failure:       workflowruntime.Failure{Code: "temporary", Message: "retry", Retryable: true},
		AttemptStatus: workflowruntime.NodeFailed,
		FailedAt:      base,
	}
	decision, err := (workflowruntime.RetryEvaluator{}).Evaluate(context.Background(), request)
	if err != nil || decision.Retry || decision.Reason != workflowruntime.RetryReasonIdempotencyMissing {
		t.Fatalf("graph intrinsic upgrade bypassed keyed executor requirement: %#v, %v", decision, err)
	}

	request.Spec.Idempotency = graph.IdempotencyNone
	request.Spec.RetrySafety = stepkind.RetrySafe
	request.IdempotencyKey = "untrusted-key"
	for _, effect := range []graph.Effect{graph.EffectMaterialize, graph.EffectMutate} {
		request.Spec.Effects = graph.EffectSet{effect}
		decision, err = (workflowruntime.RetryEvaluator{}).Evaluate(context.Background(), request)
		if err != nil || decision.Retry || decision.Reason != workflowruntime.RetryReasonEffectDenied {
			t.Fatalf("raw key bypassed unprotected %s: %#v, %v", effect, decision, err)
		}
	}
}

func TestEvaluateTimeoutsUsesOnlyActivePhase(t *testing.T) {
	base := time.Date(2026, time.August, 24, 14, 0, 0, 0, time.UTC)
	policy := &graph.TimeoutPolicy{Queue: "1s", Execution: "2s", Wait: "3s", Heartbeat: "4s", ScheduleToClose: "20s"}

	queue, err := workflowruntime.EvaluateTimeouts(policy, workflowruntime.TimeoutAnchor{ScheduledAt: base}, base.Add(2*time.Second))
	if err != nil || queue.Due == nil || queue.Due.Kind != workflowruntime.TimeoutQueue || len(queue.Deadlines) != 2 {
		t.Fatalf("queue phase = %#v, %v", queue, err)
	}
	execution, err := workflowruntime.EvaluateTimeouts(policy, workflowruntime.TimeoutAnchor{ScheduledAt: base, StartedAt: base.Add(10 * time.Second)}, base.Add(13*time.Second))
	if err != nil || execution.Due == nil || execution.Due.Kind != workflowruntime.TimeoutExecution || len(execution.Deadlines) != 2 {
		t.Fatalf("execution phase retained stale queue deadline: %#v, %v", execution, err)
	}
	external, err := workflowruntime.EvaluateTimeouts(policy, workflowruntime.TimeoutAnchor{ScheduledAt: base, StartedAt: base.Add(time.Second), ExternalAt: base.Add(10 * time.Second)}, base.Add(15*time.Second))
	if err != nil || external.Due == nil || external.Due.Kind != workflowruntime.TimeoutExternalWait || len(external.Deadlines) != 3 {
		t.Fatalf("external phase retained stale execution deadline: %#v, %v", external, err)
	}
	if got := deadlineFor(external, workflowruntime.TimeoutHeartbeat); !got.Equal(base.Add(14 * time.Second)) {
		t.Fatalf("first heartbeat deadline = %s, want ExternalAt+4s", got)
	}
	withHeartbeat, err := workflowruntime.EvaluateTimeouts(policy, workflowruntime.TimeoutAnchor{ScheduledAt: base, StartedAt: base.Add(time.Second), ExternalAt: base.Add(10 * time.Second), LastHeartbeatAt: base.Add(12 * time.Second)}, base.Add(15*time.Second))
	if err != nil || !deadlineFor(withHeartbeat, workflowruntime.TimeoutHeartbeat).Equal(base.Add(16*time.Second)) {
		t.Fatalf("heartbeat anchor = %#v, %v", withHeartbeat, err)
	}
}

func deadlineFor(evaluation workflowruntime.TimeoutEvaluation, kind workflowruntime.TimeoutKind) time.Time {
	for _, deadline := range evaluation.Deadlines {
		if deadline.Kind == kind {
			return deadline.Deadline
		}
	}
	return time.Time{}
}

type retryAuthorizerFunc func(context.Context, workflowruntime.RetryAuthorizationRequest) error

func (f retryAuthorizerFunc) AuthorizeRetry(ctx context.Context, request workflowruntime.RetryAuthorizationRequest) error {
	return f(ctx, request)
}
