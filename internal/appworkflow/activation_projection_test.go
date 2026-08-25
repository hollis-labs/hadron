package appworkflow

import (
	"errors"
	"testing"

	"github.com/hollis-labs/hadron/workflow/runtime"
)

func TestSafeFireWorkflowActivationResultRejectsAmbiguousAndInconsistentResults(t *testing.T) {
	tests := []struct {
		name   string
		result ActivationStartResult
	}{
		{name: "missing outcome", result: ActivationStartResult{Start: StartRunResult{Run: &runtime.RunSnapshot{}}}},
		{name: "unknown outcome", result: ActivationStartResult{Outcome: "unknown", Start: StartRunResult{Run: &runtime.RunSnapshot{}, Outcome: "unknown"}}},
		{name: "both branches", result: ActivationStartResult{
			Outcome: runtime.IdempotencyApplied,
			Start:   StartRunResult{Run: &runtime.RunSnapshot{}, Outcome: runtime.IdempotencyApplied},
			Reactor: &ReactorDeliveryResult{Outcome: runtime.IdempotencyApplied},
		}},
		{name: "direct outcome divergence", result: ActivationStartResult{
			Outcome: runtime.IdempotencyApplied,
			Start:   StartRunResult{Run: &runtime.RunSnapshot{}, Outcome: runtime.IdempotencyReplayed},
		}},
		{name: "reactor outcome divergence", result: ActivationStartResult{
			Outcome: runtime.IdempotencyApplied,
			Reactor: &ReactorDeliveryResult{Outcome: runtime.IdempotencyReplayed},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := SafeFireWorkflowActivationResult(test.result); !errors.Is(err, ErrInvalidActivation) {
				t.Fatalf("SafeFireWorkflowActivationResult() error = %v", err)
			}
		})
	}
}
