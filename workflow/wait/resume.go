package wait

import "context"

// AuthorizationRequest is evaluated by the host before the atomic resume
// transaction. The store subsequently revalidates all immutable wait fields.
type AuthorizationRequest struct {
	Record    Record
	Source    WakeSource
	Responder Responder
}

// ResponderAuthorizer applies host identity and policy without leaking those
// concepts into the durable wait model.
type ResponderAuthorizer interface {
	AuthorizeResume(context.Context, AuthorizationRequest) error
}
