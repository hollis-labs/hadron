package appworkflow

import (
	"errors"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/go-workflow/diagnostic"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
)

var (
	// ErrWorkflowUnauthenticated and ErrWorkflowHidden are safe transport
	// classifications. Authenticators keep credential details private.
	ErrWorkflowUnauthenticated = errors.New("workflow caller is unauthenticated")
	ErrWorkflowHidden          = errors.New("workflow is unavailable to this caller")
	ErrWorkflowInvalidRequest  = errors.New("workflow request is invalid")
)

// SafeWorkflowOperationError projects an application error without carrying
// implementation error text across a transport boundary.
func SafeWorkflowOperationError(err error, result *StartRunResult) WorkflowOperationError {
	if result != nil && result.RejectedBeforeAdmission() {
		return WorkflowOperationError{Code: WorkflowErrorCodePinRejected, Diagnostics: cloneOperationDiagnostics(result.Diagnostics), Result: result}
	}
	if errors.Is(err, ErrDryRunUnsupported) {
		return WorkflowOperationError{Code: WorkflowErrorCodeDryRunUnsupported, Diagnostics: resultDiagnostics(result), Result: result}
	}
	if errors.Is(err, ErrWorkflowUnauthenticated) {
		return WorkflowOperationError{Code: WorkflowErrorCodeUnauthenticated}
	}
	if errors.Is(err, ErrWorkflowInvalidRequest) {
		return WorkflowOperationError{Code: WorkflowErrorCodeInvalidRequest}
	}
	if errors.Is(err, ErrInvalidActivation) || errors.Is(err, ErrInvalidAgentAuthoring) || errors.Is(err, ErrInvalidContractService) || errors.Is(err, ErrContractTestFailed) || errors.Is(err, hadronregistry.ErrInvalidWorkflow) || errors.Is(err, hoststate.ErrInvalidRecord) {
		return WorkflowOperationError{Code: WorkflowErrorCodeInvalidRequest}
	}
	if errors.Is(err, ErrHostNotReady) || errors.Is(err, ErrInvalidHost) {
		return WorkflowOperationError{Code: WorkflowErrorCodeUnavailable}
	}
	if errors.Is(err, ErrWorkflowHidden) || errors.Is(err, ErrDefinitionUnauthorized) {
		return WorkflowOperationError{Code: WorkflowErrorCodeNotFound}
	}
	if errors.Is(err, ErrPolicyDenied) || errors.Is(err, ErrNamespaceUnauthorized) {
		return WorkflowOperationError{Code: WorkflowErrorCodePolicyDenied}
	}
	if errors.Is(err, ErrConfirmationRequired) {
		return WorkflowOperationError{Code: WorkflowErrorCodeConfirmationRequired, Diagnostics: resultDiagnostics(result), Result: result}
	}
	if errors.Is(err, workflowruntime.ErrNotFound) {
		return WorkflowOperationError{Code: WorkflowErrorCodeNotFound}
	}
	if errors.Is(err, hadronregistry.ErrWorkflowNotFound) {
		return WorkflowOperationError{Code: WorkflowErrorCodeNotFound}
	}
	if errors.Is(err, workflowruntime.ErrIdempotencyConflict) || errors.Is(err, hadronregistry.ErrWorkflowConflict) || errors.Is(err, hoststate.ErrConflict) {
		return WorkflowOperationError{Code: WorkflowErrorCodeIdempotencyConflict}
	}
	if errors.Is(err, ErrActivationConflict) || errors.Is(err, ErrActivationSkipped) {
		return WorkflowOperationError{Code: WorkflowErrorCodeActivationConflict, Diagnostics: resultDiagnostics(result), Result: result}
	}
	return WorkflowOperationError{Code: WorkflowErrorCodeInternal}
}

func resultDiagnostics(result *StartRunResult) []diagnostic.Diagnostic {
	if result == nil {
		return nil
	}
	return cloneOperationDiagnostics(result.Diagnostics)
}

func cloneOperationDiagnostics(input []diagnostic.Diagnostic) []diagnostic.Diagnostic {
	if len(input) == 0 {
		return nil
	}
	return append([]diagnostic.Diagnostic(nil), input...)
}
