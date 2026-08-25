package waitadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	waitadapter "github.com/hollis-labs/hadron/workflow/adapters/wait"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

var baseTime = time.Date(2026, time.August, 24, 15, 0, 0, 0, time.UTC)

func TestRegisterWaitKindsAndSpecs(t *testing.T) {
	if !waitadapter.SourceTimer.Valid() {
		t.Fatal("timer is missing from the closed wait source vocabulary")
	}
	registry := stepkind.NewRegistry()
	registration, err := waitadapter.Register(registry, waitOptions(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	if registration.Sleep == nil || registration.WaitFor == nil || registration.MessageWait == nil {
		t.Fatalf("registration = %#v", registration)
	}
	for _, name := range []string{waitadapter.SleepName, waitadapter.WaitForName, waitadapter.MessageWaitName} {
		kind, spec, err := stepkind.Resolve(registry, name, waitadapter.Version)
		if err != nil || kind == nil || !spec.CanSuspend || spec.Observation.Mode != stepkind.ObservationNone {
			t.Fatalf("resolve %s = %#v, %v", name, spec, err)
		}
		if name == waitadapter.WaitForName && spec.EmbeddedModeSupported {
			t.Fatal("callback-capable wait_for cannot claim static embedded-mode support")
		}
		if name == waitadapter.SleepName && !spec.EmbeddedModeSupported {
			t.Fatal("durable sleep unexpectedly lost embedded-mode support")
		}
		encoded, err := json.Marshal(spec.OutputSchema)
		if err != nil || !strings.Contains(string(encoded), `"timed_out":{"const":false}`) {
			t.Fatalf("%s output schema can claim unreachable timeout success: %s, %v", name, encoded, err)
		}
	}
}

func TestWaitForBuildsTypedWakeRecordsWithoutPolling(t *testing.T) {
	tests := []struct {
		name       string
		config     graph.Config
		kind       workflowwait.Kind
		wake       workflowwait.WakeSource
		sourceKind waitadapter.SourceKind
	}{
		{"signal", graph.Config{"signal": "review", "correlation": "corr", "timeout": "30m", "payload_schema": graph.Schema{"type": "object"}}, workflowwait.KindSignal, workflowwait.WakeSignal, waitadapter.SourceSignal},
		{"event", graph.Config{"event": map[string]any{"type": "deploy.complete", "source": "release"}, "correlation": "corr", "timeout": "30m"}, workflowwait.KindSignal, workflowwait.WakeSignal, waitadapter.SourceEvent},
		{"callback", graph.Config{"callback": map[string]any{"path": "/callbacks/provider"}, "correlation": "corr", "timeout": "30m"}, workflowwait.KindCallback, workflowwait.WakeCallback, waitadapter.SourceCallback},
		{"child_run", graph.Config{"child_run": map[string]any{"run_id": "child-1"}, "correlation": "corr", "timeout": "30m"}, workflowwait.KindChildRun, workflowwait.WakeChildRun, waitadapter.SourceChildRun},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var observed waitadapter.AuthorityRequest
			callback := waitadapter.CallbackIssuerFunc(func(_ context.Context, request waitadapter.CallbackRequest) (waitadapter.CallbackCredential, error) {
				if request.Path != "/callbacks/provider" || !request.ExpiresAt.Equal(baseTime.Add(30*time.Minute)) {
					t.Fatalf("callback request = %#v", request)
				}
				return waitadapter.CallbackCredential{URL: "https://callbacks.example/callbacks/provider", Token: "one-time-token"}, nil
			})
			options := waitadapter.Options{
				Authority: waitadapter.AuthorityResolverFunc(func(_ context.Context, request waitadapter.AuthorityRequest) (workflowwait.ResponderAuthority, error) {
					observed = request
					if request.Source.Attributes != nil {
						request.Source.Attributes["mutated"] = "host"
					}
					return workflowwait.ResponderAuthority{Kind: "test_policy", Reference: "policy"}, nil
				}),
				Callbacks: callback, Now: func() time.Time { return baseTime },
			}
			executor, err := waitadapter.NewWaitFor(options)
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.Execute(t.Context(), prepared(test.config))
			if err != nil || result.Outcome != stepkind.StepWaiting || result.Wait == nil {
				t.Fatalf("Execute = %#v, %v", result, err)
			}
			if result.Wait.Record.Kind != test.kind || result.Wait.Record.WakeSource != test.wake || result.Wait.Record.Status != workflowwait.StatusOpen ||
				!result.Wait.Record.Deadline.Equal(baseTime.Add(30*time.Minute)) || observed.Source.Kind != test.sourceKind {
				t.Fatalf("record/request = %#v / %#v", result.Wait.Record, observed)
			}
			if test.kind == workflowwait.KindSignal && result.Wait.Record.SignalName != observed.Source.Reference {
				t.Fatalf("named signal = %q, source reference = %q", result.Wait.Record.SignalName, observed.Source.Reference)
			}
			if test.sourceKind == waitadapter.SourceEvent {
				if result.Wait.Record.Authority.Attributes["wait_source"] != "event" || result.Wait.Record.Authority.Attributes["event_type"] != "deploy.complete" {
					t.Fatalf("event authority metadata = %#v", result.Wait.Record.Authority.Attributes)
				}
			}
			if test.sourceKind == waitadapter.SourceCallback {
				if result.Wait.ResumeToken != "one-time-token" || result.Wait.Record.ResumeTokenDigest == "" || strings.Contains(result.Wait.Record.ResumeURL, result.Wait.ResumeToken) {
					t.Fatalf("callback credential handling = %#v", result.Wait)
				}
				encoded, _ := json.Marshal(result.Wait)
				if strings.Contains(string(encoded), result.Wait.ResumeToken) {
					t.Fatalf("wait JSON leaked token: %s", encoded)
				}
			}
		})
	}
}

func TestWaitForResolvesTypedChildRunInputFailClosed(t *testing.T) {
	executor, err := waitadapter.NewWaitFor(waitOptions(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.Config{"child_run": map[string]any{"input": "run-id"}, "timeout": "30m"}
	invocation := prepared(config)
	invocation.Invocation.Inputs["run-id"] = inline(t, "call-child-run-1", values.RedactionPrivate, values.RetentionRun)
	result, err := executor.Execute(t.Context(), invocation)
	if err != nil || result.Wait == nil || result.Wait.Record.Kind != workflowwait.KindChildRun ||
		result.Wait.Record.WakeSource != workflowwait.WakeChildRun || result.Wait.Record.Correlation != "call-child-run-1" {
		t.Fatalf("dynamic child-run wait = %#v, %v", result, err)
	}

	explicit := prepared(graph.Config{"child_run": map[string]any{"input": "run-id"}, "correlation": "parent-correlation", "timeout": "30m"})
	explicit.Invocation.Inputs["run-id"] = inline(t, "call-child-run-1", values.RedactionPrivate, values.RetentionRun)
	result, err = executor.Execute(t.Context(), explicit)
	if err != nil || result.Wait.Record.Correlation != "parent-correlation" {
		t.Fatalf("explicit dynamic correlation = %#v, %v", result, err)
	}

	artifact := values.Value{Type: values.TypeArtifact, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
	secretRef, secretErr := values.ParseSecretRef("secret://project/agent/run")
	if secretErr != nil {
		t.Fatal(secretErr)
	}
	secret, secretErr := values.NewSecretRef(secretRef, values.Metadata{
		Producer: values.Producer{Kind: "test", Reference: "secret"}, MediaType: "application/json",
		Redaction: values.RedactionSecret, Retention: values.RetentionProject,
	})
	if secretErr != nil {
		t.Fatal(secretErr)
	}
	for _, test := range []struct {
		name  string
		value *values.Value
	}{
		{name: "missing"},
		{name: "number", value: pointerValue(inline(t, json.Number("1"), values.RedactionPrivate, values.RetentionRun))},
		{name: "empty", value: pointerValue(inline(t, "", values.RedactionPrivate, values.RetentionRun))},
		{name: "secret", value: &secret},
		{name: "artifact", value: &artifact},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := prepared(config)
			if test.value != nil {
				candidate.Invocation.Inputs["run-id"] = *test.value
			}
			if _, err := executor.Execute(t.Context(), candidate); err == nil {
				t.Fatal("unsafe dynamic child-run input accepted")
			}
		})
	}

	for _, invalid := range []graph.Config{
		{"child_run": map[string]any{"run_id": "child", "input": "run-id"}, "timeout": "1m"},
		{"child_run": map[string]any{}, "timeout": "1m"},
		{"child_run": map[string]any{"input": "Run ID"}, "timeout": "1m"},
		{"child_run": map[string]any{"input": 3}, "timeout": "1m"},
	} {
		if findings := executor.ValidateConfig(t.Context(), invalid); len(findings) == 0 {
			t.Fatalf("ambiguous dynamic child-run config accepted: %#v", invalid)
		}
	}
}

func TestChildRunTerminalPolicyCompletesOnlySuccessfulTypedOutputs(t *testing.T) {
	executor, err := waitadapter.NewWaitFor(waitOptions(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.Config{
		"child_run": map[string]any{"run_id": "child-1", "fail_on_unsuccessful": true}, "timeout": "30m",
		"payload_schema": graph.Schema{
			"type": "object", "additionalProperties": false, "required": []any{"handle", "status", "result"},
			"properties": map[string]any{"handle": map[string]any{"type": "object"}, "status": map[string]any{"const": "succeeded"}, "result": map[string]any{}},
		},
	}
	initial, err := executor.Execute(t.Context(), prepared(config))
	if err != nil || initial.Wait == nil {
		t.Fatalf("initial Execute = %#v, %v", initial, err)
	}
	successOutputs := map[string]any{"handle": map[string]any{"id": "session-1"}, "status": "succeeded", "result": json.Number("9007199254740993")}
	success := prepared(config)
	success.Invocation.Continuation = continuation(t, initial.Wait, inline(t, map[string]any{
		"status": "succeeded", "outputs": successOutputs,
	}, values.RedactionPrivate, values.RetentionRun), workflowwait.Responder{Kind: "child_run", Reference: "child-1"})
	completed, err := executor.Execute(t.Context(), success)
	if err != nil || completed.Outcome != stepkind.StepCompleted || !reflect.DeepEqual(completed.Outputs["payload"].Inline, successOutputs) {
		t.Fatalf("successful terminal = %#v, %v", completed, err)
	}
	if completed.Outputs["payload"].Redaction != values.RedactionPrivate || completed.Outputs["payload"].Retention != values.RetentionRun {
		t.Fatalf("successful output metadata = %#v", completed.Outputs["payload"])
	}

	for _, test := range []struct {
		status    string
		retryable bool
		wantClass stepkind.RetryClassification
	}{
		{"failed", true, stepkind.Retryable},
		{"canceled", false, stepkind.RetryPermanent},
		{"timed_out", false, stepkind.RetryPermanent},
		{"crashed", false, stepkind.RetryPermanent},
	} {
		t.Run(test.status, func(t *testing.T) {
			invocation := prepared(config)
			invocation.Invocation.Continuation = continuation(t, initial.Wait, inline(t, map[string]any{
				"status": test.status, "failure": map[string]any{"code": "child_" + test.status, "message": "child run ended", "retryable": test.retryable},
			}, values.RedactionPrivate, values.RetentionRun), workflowwait.Responder{Kind: "child_run", Reference: "child-1"})
			result, err := executor.Execute(t.Context(), invocation)
			var execution *stepkind.ExecutionError
			if result.Outcome != "" || !errors.As(err, &execution) || execution.Code != waitadapter.CodeChildRunFailed || execution.Classification != test.wantClass || strings.Contains(err.Error(), "child run ended") {
				t.Fatalf("terminal %s = %#v, %T %v", test.status, result, err, err)
			}
		})
	}

	malformed := prepared(config)
	malformed.Invocation.Continuation = continuation(t, initial.Wait, inline(t, map[string]any{
		"status": "failed", "outputs": successOutputs,
	}, values.RedactionPrivate, values.RetentionRun), workflowwait.Responder{Kind: "child_run", Reference: "child-1"})
	if _, err := executor.Execute(t.Context(), malformed); err == nil || !errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("ambiguous failed envelope = %v", err)
	}
}

func TestCallbackIssuanceIsRetryStableForAnImmutableInvocation(t *testing.T) {
	var requests []waitadapter.CallbackRequest
	issuer := waitadapter.CallbackIssuerFunc(func(_ context.Context, request waitadapter.CallbackRequest) (waitadapter.CallbackCredential, error) {
		requests = append(requests, request)
		return waitadapter.CallbackCredential{URL: "https://callbacks.example/callbacks/retry", Token: "stable-one-time-token"}, nil
	})
	clockCalls := 0
	executor, err := waitadapter.NewWaitFor(waitadapter.Options{
		Authority: waitOptions(nil, nil).Authority, Callbacks: issuer,
		Now: func() time.Time {
			result := baseTime.Add(time.Duration(clockCalls) * time.Minute)
			clockCalls++
			return result
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	config := graph.Config{"callback": map[string]any{"path": "/callbacks/retry"}, "correlation": "retry-correlation", "timeout": "30m"}
	first, err := executor.Execute(t.Context(), prepared(config))
	if err != nil {
		t.Fatal(err)
	}
	second, err := executor.Execute(t.Context(), prepared(config))
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 || requests[0].IdempotencyKey == "" || requests[0].IdempotencyKey != requests[1].IdempotencyKey ||
		requests[0].Identity != requests[1].Identity || requests[0].Path != requests[1].Path || requests[0].Correlation != requests[1].Correlation || !requests[1].ExpiresAt.After(requests[0].ExpiresAt) || first.Wait.ID != second.Wait.ID ||
		first.Wait.Record.ResumeURL != second.Wait.Record.ResumeURL || first.Wait.Record.ResumeTokenDigest != second.Wait.Record.ResumeTokenDigest ||
		first.Wait.ResumeToken != second.Wait.ResumeToken {
		t.Fatalf("callback retry identity = requests %#v, waits %#v / %#v", requests, first.Wait, second.Wait)
	}
}

func TestSleepInitialAndContinuationUseExactSuccessfulWakeAt(t *testing.T) {
	executor := waitadapter.NewSleep(func() time.Time { return baseTime })
	config := graph.Config{"duration": "15m"}
	initial, err := executor.Execute(t.Context(), prepared(config))
	if err != nil || initial.Outcome != stepkind.StepWaiting || initial.Wait == nil {
		t.Fatalf("initial sleep = %#v, %v", initial, err)
	}
	wakeAt := baseTime.Add(15 * time.Minute)
	if initial.Wait.Record.Kind != workflowwait.KindTimer || initial.Wait.Record.WakeSource != workflowwait.WakeTimer ||
		!initial.Wait.Record.WakeAt.Equal(wakeAt) || !initial.Wait.Record.Deadline.IsZero() || initial.Wait.ResumeToken != "" {
		t.Fatalf("sleep record = %#v", initial.Wait.Record)
	}
	payload := inline(t, map[string]any{"woke_at": wakeAt.Format(time.RFC3339Nano)}, values.RedactionPrivate, values.RetentionRun)
	invocation := prepared(config)
	invocation.Invocation.Continuation = continuation(t, initial.Wait, payload, workflowwait.Responder{Kind: "system", Reference: "wait-timer"})
	completed, err := executor.Execute(t.Context(), invocation)
	if err != nil || completed.Outcome != stepkind.StepCompleted || completed.Outputs["woke_at"].Inline != wakeAt.Format(time.RFC3339Nano) || completed.Outputs["timed_out"].Inline != false {
		t.Fatalf("sleep continuation = %#v, %v", completed, err)
	}
	resume := completed.Outputs["resume"].Inline.(map[string]any)
	if resume["source"] != "timer" || resume["resolved_at"] != wakeAt.Format(time.RFC3339Nano) {
		t.Fatalf("sleep resume metadata = %#v", resume)
	}
	for _, test := range []struct {
		name      string
		payload   values.Value
		responder workflowwait.Responder
	}{
		{"mismatched-woke-at", inline(t, map[string]any{"woke_at": wakeAt.Add(time.Second).Format(time.RFC3339Nano)}, values.RedactionPrivate, values.RetentionRun), workflowwait.Responder{Kind: "system", Reference: "wait-timer"}},
		{"forged-responder", payload, workflowwait.Responder{Kind: "user", Reference: "operator"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			forged := prepared(config)
			forged.Invocation.Continuation = continuation(t, initial.Wait, test.payload, test.responder)
			if _, err := executor.Execute(t.Context(), forged); err == nil || !strings.Contains(err.Error(), "sleep continuation") {
				t.Fatalf("forged continuation = %v", err)
			}
		})
	}
	if findings := executor.ValidateConfig(t.Context(), graph.Config{"duration": "0s", "poll_interval": "1s"}); len(findings) != 2 {
		t.Fatalf("sleep config diagnostics = %#v", findings)
	}
}

func TestSleepClockLateCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	executor := waitadapter.NewSleep(func() time.Time {
		cancel()
		return baseTime
	})
	if _, err := executor.Execute(ctx, prepared(graph.Config{"duration": "1m"})); !errors.Is(err, context.Canceled) {
		t.Fatalf("late clock cancellation = %v", err)
	}
}

func TestWaitForContinuationExactNumbersAndClassification(t *testing.T) {
	executor, err := waitadapter.NewWaitFor(waitOptions(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.Config{"signal": "review", "correlation": "corr", "timeout": "30m", "payload_schema": graph.Schema{"type": "object"}}
	initial, err := executor.Execute(t.Context(), prepared(config))
	if err != nil {
		t.Fatal(err)
	}
	payload := inline(t, map[string]any{"count": json.Number("9007199254740993")}, values.RedactionPrivate, values.RetentionRun)
	continued := prepared(config)
	continued.Invocation.Continuation = continuation(t, initial.Wait, payload, workflowwait.Responder{Kind: "user", Reference: "user-1"})
	result, err := executor.Execute(t.Context(), continued)
	if err != nil || result.Outcome != stepkind.StepCompleted {
		t.Fatalf("continued Execute = %#v, %v", result, err)
	}
	count := result.Outputs["payload"].Inline.(map[string]any)["count"]
	if number, ok := count.(json.Number); !ok || number.String() != "9007199254740993" {
		t.Fatalf("exact number = %#v", count)
	}
	if result.Outputs["payload"].Digest != payload.Digest || result.Outputs["timed_out"].Inline != false {
		t.Fatalf("outputs = %#v", result.Outputs)
	}
	resume := result.Outputs["resume"].Inline.(map[string]any)
	if resume["source"] != "signal" || strings.Contains(resume["wait_id"].(string), "token") {
		t.Fatalf("resume metadata = %#v", resume)
	}

	for _, unsafe := range []values.Value{
		inline(t, map[string]any{"ok": true}, values.RedactionPublic, values.RetentionRun),
		inline(t, map[string]any{"ok": true}, values.RedactionPrivate, values.RetentionNone),
	} {
		invocation := prepared(config)
		invocation.Invocation.Continuation = continuation(t, initial.Wait, unsafe, workflowwait.Responder{Kind: "user", Reference: "user-1"})
		if _, err := executor.Execute(t.Context(), invocation); err == nil {
			t.Fatalf("unsafe resume envelope accepted: %v", err)
		}
	}
	invalidSchema := prepared(config)
	invalidSchema.Invocation.Continuation = continuation(t, initial.Wait, inline(t, "not-an-object", values.RedactionPrivate, values.RetentionRun), workflowwait.Responder{Kind: "user", Reference: "user-1"})
	if _, err := executor.Execute(t.Context(), invalidSchema); err == nil || !errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("schema-invalid direct continuation = %v", err)
	}
}

func TestMessageWaitRecordAndContinuation(t *testing.T) {
	executor, err := waitadapter.NewMessageWait(waitOptions(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.Config{
		"substrate": "local", "to": "msg://agent/project/reviewer", "correlation": "review-1", "timeout": "1h",
		"payload_schema": graph.Schema{"type": "object", "required": []any{"approved"}},
	}
	initial, err := executor.Execute(t.Context(), prepared(config))
	if err != nil || initial.Wait.Record.Kind != workflowwait.KindMessage || initial.Wait.Record.WakeSource != workflowwait.WakeMessage || initial.Wait.ResumeToken != "" {
		t.Fatalf("initial message wait = %#v, %v", initial, err)
	}
	payload := inline(t, map[string]any{"approved": true}, values.RedactionPrivate, values.RetentionRun)
	invocation := prepared(config)
	invocation.Invocation.Continuation = continuation(t, initial.Wait, payload, workflowwait.Responder{Kind: "message_sender", Reference: "msg://user/project/alice", Attributes: map[string]string{"message_id": "msg-1"}})
	completed, err := executor.Execute(t.Context(), invocation)
	if err != nil || completed.Outputs["message"].Digest != payload.Digest || completed.Outputs["timed_out"].Inline != false {
		t.Fatalf("message continuation = %#v, %v", completed, err)
	}
}

func TestWaitConfigFailsClosedAndDeterministically(t *testing.T) {
	waitFor, _ := waitadapter.NewWaitFor(waitOptions(nil, nil))
	bad := graph.Config{
		"signal": "x", "event": map[string]any{"type": "y"}, "timeout": "0s", "payload_schema": graph.Schema{"$ref": "https://example/schema"}, "unknown": true,
	}
	first := waitFor.ValidateConfig(t.Context(), bad)
	for range 30 {
		if repeated := waitFor.ValidateConfig(t.Context(), bad); !reflect.DeepEqual(repeated, first) {
			t.Fatalf("nondeterministic diagnostics: %#v / %#v", first, repeated)
		}
	}
	joined := diagnostics(first)
	for _, path := range []string{"config.payload_schema", "config.timeout", "config.unknown"} {
		if !strings.Contains(joined, path) {
			t.Fatalf("diagnostics missing %s: %s", path, joined)
		}
	}
	callbackBad := []graph.Config{
		{"callback": map[string]any{"path": "https://host/path"}, "timeout": "1m"},
		{"callback": map[string]any{"path": "/a/../b"}, "timeout": "1m"},
		{"callback": map[string]any{"path": "/a?token=x"}, "timeout": "1m"},
		{"callback": map[string]any{"path": "/{{ secret }}"}, "timeout": "1m"},
	}
	for _, config := range callbackBad {
		if findings := waitFor.ValidateConfig(t.Context(), config); len(findings) == 0 {
			t.Fatalf("invalid callback accepted: %#v", config)
		}
	}
	message, _ := waitadapter.NewMessageWait(waitOptions(nil, nil))
	if findings := message.ValidateConfig(t.Context(), graph.Config{"substrate": "x", "to": "msg://agent/a/b", "correlation_id": "legacy", "timeout": "1m"}); len(findings) == 0 || !strings.Contains(diagnostics(findings), "correlation_id") {
		t.Fatalf("legacy message config diagnostics = %#v", findings)
	}
}

func TestWaitSeamsTypedNilCancellationAndConcurrency(t *testing.T) {
	var nilAuthority *authorityStub
	if _, err := waitadapter.NewWaitFor(waitadapter.Options{Authority: nilAuthority}); err == nil {
		t.Fatal("typed-nil authority accepted")
	}
	lateAuthority := waitadapter.AuthorityResolverFunc(func(ctx context.Context, _ waitadapter.AuthorityRequest) (workflowwait.ResponderAuthority, error) {
		if cancel, ok := ctx.Value(cancelKey{}).(context.CancelFunc); ok {
			cancel()
		}
		return workflowwait.ResponderAuthority{Kind: "test"}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, cancelKey{}, cancel)
	executor, _ := waitadapter.NewWaitFor(waitadapter.Options{Authority: lateAuthority, Now: func() time.Time { return baseTime }})
	if _, err := executor.Execute(ctx, prepared(graph.Config{"signal": "x", "timeout": "1m"})); !errors.Is(err, context.Canceled) {
		t.Fatalf("late authority cancellation = %v", err)
	}

	var nilCallbacks *callbackStub
	executor, err := waitadapter.NewWaitFor(waitOptions(nil, nilCallbacks))
	if err != nil {
		t.Fatal(err)
	}
	if _, executeErr := executor.Execute(t.Context(), prepared(graph.Config{"callback": map[string]any{"path": "/callbacks/typed-nil"}, "timeout": "1m"})); executeErr == nil {
		t.Fatal("typed-nil callback issuer accepted")
	}
	callbackCtx, callbackCancel := context.WithCancel(context.Background())
	lateCallback := waitadapter.CallbackIssuerFunc(func(context.Context, waitadapter.CallbackRequest) (waitadapter.CallbackCredential, error) {
		callbackCancel()
		return waitadapter.CallbackCredential{URL: "https://callbacks.example/callbacks/canceled", Token: "orphan-with-host-ttl"}, nil
	})
	executor, err = waitadapter.NewWaitFor(waitOptions(nil, lateCallback))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Execute(callbackCtx, prepared(graph.Config{"callback": map[string]any{"path": "/callbacks/canceled"}, "timeout": "1m"})); !errors.Is(err, context.Canceled) {
		t.Fatalf("late callback cancellation = %v", err)
	}

	concurrent, _ := waitadapter.NewWaitFor(waitOptions(nil, nil))
	const workers = 32
	results := make(chan string, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := concurrent.Execute(context.Background(), prepared(graph.Config{"signal": "x", "timeout": "1m"}))
			if err != nil {
				results <- "error:" + err.Error()
				return
			}
			results <- result.Wait.ID
		}()
	}
	group.Wait()
	close(results)
	var expected string
	for result := range results {
		if expected == "" {
			expected = result
		}
		if result != expected {
			t.Fatalf("concurrent wait IDs differ: %q / %q", expected, result)
		}
	}
}

func TestWaitAuthorityResultIsDefensivelyOwned(t *testing.T) {
	attributes := map[string]string{"role": "reviewer"}
	executor, err := waitadapter.NewWaitFor(waitOptions(waitadapter.AuthorityResolverFunc(func(context.Context, waitadapter.AuthorityRequest) (workflowwait.ResponderAuthority, error) {
		return workflowwait.ResponderAuthority{Kind: "test_policy", Reference: "stable", Attributes: attributes}, nil
	}), nil))
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(t.Context(), prepared(graph.Config{"signal": "review", "timeout": "1m"}))
	if err != nil {
		t.Fatal(err)
	}
	attributes["role"] = "mutated"
	attributes["new"] = "host"
	if result.Wait.Record.Authority.Attributes["role"] != "reviewer" || result.Wait.Record.Authority.Attributes["new"] != "" {
		t.Fatalf("host retained authority ownership: %#v", result.Wait.Record.Authority.Attributes)
	}
}

type authorityStub struct{}

func (*authorityStub) ResolveWaitAuthority(context.Context, waitadapter.AuthorityRequest) (workflowwait.ResponderAuthority, error) {
	return workflowwait.ResponderAuthority{Kind: "test"}, nil
}

type callbackStub struct{}

func (*callbackStub) IssueCallback(context.Context, waitadapter.CallbackRequest) (waitadapter.CallbackCredential, error) {
	return waitadapter.CallbackCredential{}, nil
}

type cancelKey struct{}

func waitOptions(authority waitadapter.AuthorityResolver, callbacks waitadapter.CallbackIssuer) waitadapter.Options {
	if authority == nil {
		authority = waitadapter.AuthorityResolverFunc(func(_ context.Context, _ waitadapter.AuthorityRequest) (workflowwait.ResponderAuthority, error) {
			return workflowwait.ResponderAuthority{Kind: "test_policy", Reference: "test"}, nil
		})
	}
	return waitadapter.Options{Authority: authority, Callbacks: callbacks, Now: func() time.Time { return baseTime }}
}

func prepared(config graph.Config) stepkind.PreparedInvocation {
	return stepkind.PreparedInvocation{Invocation: stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "wait-node", Iteration: "item-1", Attempt: 2},
		Config:   config, Inputs: values.ValueSet{},
	}}
}

func continuation(t *testing.T, waitResult *stepkind.WaitResult, payload values.Value, responder workflowwait.Responder) *stepkind.WaitContinuation {
	t.Helper()
	set := values.ValueSet{"resume": payload}
	ref, err := values.NewValueSetRef("resume-values", set)
	if err != nil {
		t.Fatal(err)
	}
	record := waitResult.Record
	record.Status = workflowwait.StatusResumed
	record.ResumeValues = &ref
	resolvedAt := baseTime.Add(time.Minute)
	if !record.WakeAt.IsZero() {
		resolvedAt = record.WakeAt
	}
	record.Resolution = &workflowwait.Resolution{
		Source: record.WakeSource, Responder: responder, PayloadDigest: ref.Digest,
		IdempotencyKey: "resume-1", ResolvedAt: resolvedAt,
	}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	return &stepkind.WaitContinuation{ID: waitResult.ID, Record: record, Values: set}
}

func inline(t *testing.T, value any, redaction values.RedactionClass, retention values.RetentionClass) values.Value {
	t.Helper()
	result, err := values.NewInline(value, values.Metadata{
		Producer: values.Producer{Kind: "resume", Reference: "resume-1"}, MediaType: "application/json",
		Redaction: redaction, Retention: retention,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func pointerValue(value values.Value) *values.Value { return &value }

func diagnostics(findings []diagnostic.Diagnostic) string {
	messages := make([]string, 0, len(findings))
	for _, finding := range findings {
		messages = append(messages, finding.Message)
	}
	return strings.Join(messages, "\n")
}
