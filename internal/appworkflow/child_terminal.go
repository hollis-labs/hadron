package appworkflow

import (
	"context"
	"errors"
	"fmt"

	waitadapter "github.com/hollis-labs/go-workflow/adapters/wait"
	"github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

func (h *Host) recoverChildTerminalWaits(ctx context.Context) error {
	source, ok := h.state.(runtime.ChildTerminalWaitStore)
	if !ok || nilInterface(source) || h.waits == nil {
		return nil
	}
	for {
		candidates, err := source.RecoverChildTerminalWaits(ctx, h.batchLimit)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			return nil
		}
		for _, candidate := range candidates {
			payload, payloadErr := h.childTerminalPayload(ctx, candidate)
			if payloadErr != nil {
				return payloadErr
			}
			at := maxTime(h.now(), candidate.Child.UpdatedAt)
			at = maxTime(at, candidate.Wait.UpdatedAt)
			_, resumeErr := h.waits.Resume(context.WithoutCancel(ctx), runtime.ResumeCommand{
				WaitID: candidate.Wait.Ref.ID, Correlation: candidate.Wait.Correlation, WakeSource: workflowwait.WakeChildRun,
				Responder: workflowwait.Responder{Kind: "child_run", Reference: string(candidate.Child.ID)}, Payload: payload,
				IdempotencyKey: childTerminalResumeKey(candidate.Child.ID), ReceivedAt: at,
			})
			var postCommit *runtime.PostCommitError
			if resumeErr != nil && !errors.Is(resumeErr, runtime.ErrWaitClosed) && !errors.As(resumeErr, &postCommit) {
				return fmt.Errorf("resume child terminal wait %s: %w", candidate.Wait.Ref.ID, resumeErr)
			}
		}
	}
}

func (h *Host) childTerminalPayload(ctx context.Context, candidate runtime.ChildTerminalWait) (values.Value, error) {
	status := waitadapter.ChildRunTerminalStatus(candidate.Child.Status)
	envelope := waitadapter.ChildRunTerminalEnvelope{Status: status}
	if candidate.Child.Status == runtime.RunSucceeded {
		envelope.Outputs = map[string]any{}
		if candidate.Child.Outputs != nil {
			outputs, err := h.state.LoadValues(ctx, *candidate.Child.Outputs)
			if err != nil {
				return values.Value{}, err
			}
			for name, output := range outputs {
				if output.Artifact != nil || output.SecretRef != nil {
					// The wait adapter deliberately accepts only inline typed JSON.
					// Convert an unexportable successful result to a deterministic,
					// safe child failure so recovery converges instead of bricking the
					// host on every restart.
					envelope = waitadapter.ChildRunTerminalEnvelope{Status: waitadapter.ChildRunFailed, Failure: &waitadapter.ChildRunFailure{
						Code: "child_output_not_inline", Message: "child run output cannot cross the inline wait boundary",
					}}
					break
				}
				envelope.Outputs[name] = output.Inline
			}
		}
	} else {
		envelope.Failure = &waitadapter.ChildRunFailure{Code: "child_run_" + string(candidate.Child.Status), Message: "child run reached terminal status " + string(candidate.Child.Status)}
	}
	if err := envelope.Validate(); err != nil {
		return values.Value{}, err
	}
	inline := map[string]any{"status": string(envelope.Status)}
	if envelope.Outputs != nil {
		inline["outputs"] = envelope.Outputs
	}
	if envelope.Failure != nil {
		inline["failure"] = map[string]any{"code": envelope.Failure.Code, "message": envelope.Failure.Message, "retryable": envelope.Failure.Retryable}
	}
	return values.NewInline(inline, values.Metadata{Producer: values.Producer{Kind: "child_run", Reference: string(candidate.Child.ID)},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
}

func childTerminalResumeKey(child runtime.RunID) string { return "child-terminal:" + string(child) }
