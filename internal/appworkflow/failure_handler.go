package appworkflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/offline"
	"github.com/hollis-labs/hadron/workflow/runtime"
)

func (h *Host) recoverFailureHooks(ctx context.Context) error {
	if h.failureHooks == nil {
		return nil
	}
	pending, err := h.failureHooks.RecoverFailureHooks(ctx, h.batchLimit)
	if err != nil {
		return err
	}
	for _, hook := range pending {
		if err := h.startFailureHook(ctx, hook); err != nil {
			return err
		}
	}
	if h.failureHandler == nil {
		return nil
	}
	for {
		runs, err := h.failureHooks.ListUnhandledFailedRuns(ctx, h.batchLimit)
		if err != nil {
			return err
		}
		if len(runs) == 0 {
			return nil
		}
		for _, runID := range runs {
			hook, bindErr := h.bindFailureHook(ctx, runID, *h.failureHandler)
			if bindErr != nil {
				return bindErr
			}
			if hook.Status == hoststate.FailureHookSuppressed {
				continue
			}
			if err := h.startFailureHook(ctx, hook); err != nil {
				return err
			}
		}
	}
}

func (h *Host) bindFailureHook(ctx context.Context, sourceRunID runtime.RunID, config FailureHandlerConfig) (hoststate.FailureHookSnapshot, error) {
	identity, sourcePlan, err := h.failureSourceBinding(ctx, sourceRunID)
	if err != nil {
		return hoststate.FailureHookSnapshot{}, err
	}
	resolved, err := h.definitions.ResolvePlan(ctx, config.Definition)
	if err != nil {
		return hoststate.FailureHookSnapshot{}, fmt.Errorf("resolve on_run_failed definition: %w", err)
	}
	if !reflect.DeepEqual(resolved.Definition, config.Definition) {
		return hoststate.FailureHookSnapshot{}, errors.New("on_run_failed definition did not resolve to its exact immutable digest")
	}
	handlerRunID := failureHandlerRunID(sourceRunID, config.Definition.Digest)
	hook, _, err := h.failureHooks.BindFailureHook(context.WithoutCancel(ctx), hoststate.BindFailureHookRequest{
		SourceRunID: sourceRunID, SourcePlan: sourcePlan, HandlerRunID: handlerRunID,
		Handler: config.Definition, Identity: identity, MaximumDepth: config.MaximumDepth, At: h.now(),
	})
	return hook, err
}

func (h *Host) startFailureHook(ctx context.Context, hook hoststate.FailureHookSnapshot) error {
	if hook.Status == hoststate.FailureHookStarted || hook.Status == hoststate.FailureHookSuppressed || hook.Status == hoststate.FailureHookFailed {
		return nil
	}
	plan, err := h.definitions.ResolvePlan(ctx, hook.Binding.Handler)
	if err != nil {
		return err
	}
	plan, err = cloneExecutionPlan(plan)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(plan.Definition, hook.Binding.Handler) {
		return errors.New("persisted on_run_failed binding changed its exact plan")
	}
	inputs := failureHandlerInputs(plan, hook.Binding)
	expectedIdentity := hook.Binding.Identity.Clone()
	result, startErr := h.startRunInternal(ctx, StartRunRequest{RunID: hook.Binding.HandlerRunID, Definition: hook.Binding.Handler, Inputs: inputs,
		IdempotencyKey: failureHandlerStartKey(hook.Binding.SourceRunID), Identity: identityRequestFromBinding(hook.Binding.Identity)}, &expectedIdentity, true)
	if startErr != nil {
		var runFailure *offline.RunFailureError
		if errors.As(startErr, &runFailure) && result.Run != nil {
			_, completeErr := h.failureHooks.CompleteFailureHook(context.WithoutCancel(ctx), hook.Binding.SourceRunID, hook.Generation, hoststate.FailureHookStarted, "", maxTime(h.now(), hook.UpdatedAt))
			return completeErr
		}
		if errors.Is(startErr, ErrPolicyDenied) || errors.Is(startErr, ErrConfirmationRequired) {
			message := boundedFailureHookError(startErr)
			_, completeErr := h.failureHooks.CompleteFailureHook(context.WithoutCancel(ctx), hook.Binding.SourceRunID, hook.Generation, hoststate.FailureHookFailed, message, maxTime(h.now(), hook.UpdatedAt))
			return completeErr
		}
		return startErr
	}
	if len(result.Diagnostics) != 0 {
		message := boundedFailureHookError(fmt.Errorf("handler definition rejected with %d diagnostics", len(result.Diagnostics)))
		_, completionErr := h.failureHooks.CompleteFailureHook(context.WithoutCancel(ctx), hook.Binding.SourceRunID, hook.Generation, hoststate.FailureHookFailed, message, maxTime(h.now(), hook.UpdatedAt))
		return completionErr
	}
	_, err = h.failureHooks.CompleteFailureHook(context.WithoutCancel(ctx), hook.Binding.SourceRunID, hook.Generation, hoststate.FailureHookStarted, "", maxTime(h.now(), hook.UpdatedAt))
	return err
}

func (h *Host) failureSourceBinding(ctx context.Context, runID runtime.RunID) (hoststate.IdentityBinding, runtime.PlanRef, error) {
	if start, err := h.journal.LoadStart(ctx, runID); err == nil {
		return start.Record.Identity, start.Record.Run.Plan, nil
	} else if !errors.Is(err, runtime.ErrNotFound) {
		return hoststate.IdentityBinding{}, runtime.PlanRef{}, err
	}
	audit, ok := h.journal.(hoststate.NonDurableJournal)
	if !ok || nilInterface(audit) {
		return hoststate.IdentityBinding{}, runtime.PlanRef{}, runtime.ErrNotFound
	}
	record, err := audit.LoadNonDurableStart(ctx, runID)
	return record.Identity, record.Plan, err
}

func failureHandlerInputs(plan *compile.ExecutionPlan, binding hoststate.FailureHookBinding) map[string]any {
	available := map[string]any{
		"failed_run_id":       string(binding.SourceRunID),
		"failure_status":      string(runtime.RunFailed),
		"failure_plan_digest": binding.SourcePlan.Digest,
		"failure_depth":       binding.Depth,
		"failure":             map[string]any{"run_id": string(binding.SourceRunID), "status": string(runtime.RunFailed), "plan_digest": binding.SourcePlan.Digest, "depth": binding.Depth},
	}
	result := make(map[string]any)
	for _, input := range plan.Graph.Inputs {
		if value, ok := available[input.Name]; ok {
			result[input.Name] = value
		}
	}
	return result
}

func identityRequestFromBinding(identity hoststate.IdentityBinding) IdentityRequest {
	request := IdentityRequest{PrincipalHint: identity.Principal, SourceAuthority: identity.SourceAuthority,
		RunScope: &hoststate.RunScopeSelector{Version: identity.RunScope.Version, Kind: identity.RunScope.Kind, ID: identity.RunScope.ID}}
	if identity.ExecutionTarget != nil {
		target := identity.ExecutionTarget
		request.ExecutionTarget = &hoststate.ExecutionTargetSelector{Version: target.Version, ID: target.ID, Kinds: []hoststate.ExecutionTargetKind{target.Kind},
			RequiredCapabilities: append([]string(nil), target.Capabilities...), RequiredLabels: cloneStringMap(target.Labels), SandboxModes: []hoststate.SandboxMode{target.Sandbox.Mode}}
	}
	return request
}

func failureHandlerRunID(source runtime.RunID, digest string) runtime.RunID {
	sum := sha256.Sum256([]byte(string(source) + "\x00" + digest))
	return runtime.RunID("failure-handler-" + hex.EncodeToString(sum[:]))
}

func failureHandlerStartKey(source runtime.RunID) string { return "failure-hook:" + string(source) }

func boundedFailureHookError(err error) string {
	message := strings.TrimSpace(err.Error())
	if len(message) > 2048 {
		message = message[:2048]
	}
	return message
}
