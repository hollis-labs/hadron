package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/workflow/values"
)

// ErrRetentionHook marks host retention preparation or recording failure.
var ErrRetentionHook = errors.New("workflow retention hook failed")

// RetentionGroup lists names sharing one retention class. Names are sorted;
// groups use the closed class order run, project, external.
type RetentionGroup struct {
	Class values.RetentionClass `json:"class"`
	Names []string              `json:"names"`
}

// RetentionPlan describes classification before a value set is persisted.
// Mixed retained classes are intentionally supported.
type RetentionPlan struct {
	Owner  ValueOwner       `json:"owner"`
	Groups []RetentionGroup `json:"groups"`
}

// RetentionRecord adds the immutable persisted reference to its plan.
type RetentionRecord struct {
	Plan RetentionPlan      `json:"plan"`
	Ref  values.ValueSetRef `json:"ref"`
}

// RetentionHook lets a host prepare and record lifecycle ownership without
// entering the core StateStore contract. Run values are eligible for run-end
// cleanup; project values follow project policy; external payload lifetimes
// remain externally owned and must not be deleted by the workflow store.
type RetentionHook interface {
	BeforePersist(context.Context, RetentionPlan) error
	AfterPersist(context.Context, RetentionRecord) error
}

// RetentionHookFuncs adapts optional callbacks to RetentionHook.
type RetentionHookFuncs struct {
	Before func(context.Context, RetentionPlan) error
	After  func(context.Context, RetentionRecord) error
}

// BeforePersist calls the optional callback.
func (f RetentionHookFuncs) BeforePersist(ctx context.Context, plan RetentionPlan) error {
	if f.Before == nil {
		return nil
	}
	return f.Before(ctx, plan)
}

// AfterPersist calls the optional callback.
func (f RetentionHookFuncs) AfterPersist(ctx context.Context, record RetentionRecord) error {
	if f.After == nil {
		return nil
	}
	return f.After(ctx, record)
}

// RetentionHookError identifies the failed phase. Ref is present for an after
// failure so a host can find an unreferenced immutable set for cleanup.
type RetentionHookError struct {
	Phase string
	Ref   *values.ValueSetRef
	Cause error
}

func (e *RetentionHookError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Ref != nil {
		return fmt.Sprintf("%s: %s phase for value set %s: %v", ErrRetentionHook, e.Phase, e.Ref.ID, e.Cause)
	}
	return fmt.Sprintf("%s: %s phase: %v", ErrRetentionHook, e.Phase, e.Cause)
}

// Unwrap exposes the stable sentinel and the callback cause.
func (e *RetentionHookError) Unwrap() []error {
	if e == nil || e.Cause == nil {
		return []error{ErrRetentionHook}
	}
	return []error{ErrRetentionHook, e.Cause}
}

// SaveValuesWithRetention validates persistence classification, invokes an
// optional host hook, and performs one StateStore write. A nil hook is valid
// and means no host lifecycle callback. An AfterPersist failure returns the
// successfully written immutable reference in RetentionHookError.Ref.
func SaveValuesWithRetention(
	ctx context.Context,
	store StateStore,
	hook RetentionHook,
	request SaveValuesRequest,
) (values.ValueSetRef, error) {
	if nilStateStore(store) {
		return values.ValueSetRef{}, fmt.Errorf("state store is required")
	}
	if err := request.Owner.Validate(); err != nil {
		return values.ValueSetRef{}, err
	}
	if err := values.ValidatePersistableSet(request.Values); err != nil {
		return values.ValueSetRef{}, err
	}
	plan := retentionPlan(request)
	if hook != nil {
		if err := hook.BeforePersist(ctx, cloneRetentionPlan(plan)); err != nil {
			return values.ValueSetRef{}, &RetentionHookError{Phase: "before_persist", Cause: err}
		}
	}
	ref, err := store.SaveValues(ctx, request)
	if err != nil {
		return values.ValueSetRef{}, err
	}
	if hook != nil {
		if err := hook.AfterPersist(ctx, RetentionRecord{Plan: cloneRetentionPlan(plan), Ref: ref}); err != nil {
			copyRef := ref
			return values.ValueSetRef{}, &RetentionHookError{Phase: "after_persist", Ref: &copyRef, Cause: err}
		}
	}
	return ref, nil
}

func retentionPlan(request SaveValuesRequest) RetentionPlan {
	grouped := map[values.RetentionClass][]string{
		values.RetentionRun: {}, values.RetentionProject: {}, values.RetentionExternal: {},
	}
	for name, value := range request.Values {
		grouped[value.Retention] = append(grouped[value.Retention], name)
	}
	groups := make([]RetentionGroup, 0, len(grouped))
	for _, class := range []values.RetentionClass{
		values.RetentionRun, values.RetentionProject, values.RetentionExternal,
	} {
		names := grouped[class]
		if len(names) == 0 {
			continue
		}
		sort.Strings(names)
		groups = append(groups, RetentionGroup{Class: class, Names: names})
	}
	return RetentionPlan{Owner: cloneRetentionOwner(request.Owner), Groups: groups}
}

func cloneRetentionPlan(plan RetentionPlan) RetentionPlan {
	cloned := RetentionPlan{Owner: cloneRetentionOwner(plan.Owner), Groups: make([]RetentionGroup, len(plan.Groups))}
	for index, group := range plan.Groups {
		cloned.Groups[index] = RetentionGroup{Class: group.Class, Names: append([]string(nil), group.Names...)}
	}
	return cloned
}

func cloneRetentionOwner(owner ValueOwner) ValueOwner {
	cloned := owner
	if owner.Invocation != nil {
		invocation := *owner.Invocation
		cloned.Invocation = &invocation
	}
	if owner.Attempt != nil {
		attempt := *owner.Attempt
		cloned.Attempt = &attempt
	}
	return cloned
}

// MaskEventRequest defensively copies an event request and masks all attribute
// values with known resolved material before persistence. No redactor or secret
// material is retained in the result.
func MaskEventRequest(request AppendEventRequest, redactor *values.Redactor) AppendEventRequest {
	result := request
	if request.Invocation != nil {
		copyID := *request.Invocation
		result.Invocation = &copyID
	}
	if request.Attempt != nil {
		copyID := *request.Attempt
		result.Attempt = &copyID
	}
	if request.Values != nil {
		copyRef := *request.Values
		result.Values = &copyRef
	}
	if request.Attributes != nil {
		result.Attributes = make(map[string]string, len(request.Attributes))
		for key, value := range request.Attributes {
			if redactor != nil {
				value = redactor.MaskString(value)
			}
			result.Attributes[key] = value
		}
	}
	return result
}

// AppendMaskedEvent applies adapter-boundary masking before StateStore sees the
// request. Graph-native command/prompt/message/HTTP/MCP adapters can share this
// path without adding their transport types to runtime.
func AppendMaskedEvent(
	ctx context.Context,
	store StateStore,
	request AppendEventRequest,
	redactor *values.Redactor,
) (Event, error) {
	if nilStateStore(store) {
		return Event{}, fmt.Errorf("state store is required")
	}
	return store.AppendEvent(ctx, MaskEventRequest(request, redactor))
}

// RenderedEvent is the stable display projection shared by CLI, HTTP, MCP,
// A2A, and UI callers. Value references contain no payload and remain visible.
type RenderedEvent struct {
	Sequence   uint64                `json:"sequence"`
	RunID      RunID                 `json:"run_id"`
	Invocation *NodeInvocationID     `json:"invocation,omitempty"`
	Attempt    *AttemptID            `json:"attempt,omitempty"`
	Type       string                `json:"type"`
	OccurredAt time.Time             `json:"occurred_at"`
	Attributes map[string]string     `json:"attributes,omitempty"`
	Values     *values.ValueSetRef   `json:"values,omitempty"`
	Redaction  values.RedactionClass `json:"redaction"`
	Retention  values.RetentionClass `json:"retention"`
	Masked     bool                  `json:"masked"`
}

// RenderEvent applies the same private policy as value rendering. Secret event
// attributes are always masked; private attributes require explicit reveal.
func RenderEvent(event Event, policy values.DisplayPolicy) (RenderedEvent, error) {
	if err := policy.Validate(); err != nil {
		return RenderedEvent{}, err
	}
	if err := event.Validate(); err != nil {
		return RenderedEvent{}, err
	}
	rendered := RenderedEvent{
		Sequence: event.Sequence, RunID: event.RunID, Type: event.Type,
		OccurredAt: event.OccurredAt, Redaction: event.Redaction, Retention: event.Retention,
	}
	if event.Invocation != nil {
		copyID := *event.Invocation
		rendered.Invocation = &copyID
	}
	if event.Attempt != nil {
		copyID := *event.Attempt
		rendered.Attempt = &copyID
	}
	if event.Values != nil {
		copyRef := *event.Values
		rendered.Values = &copyRef
	}
	mask := event.Redaction == values.RedactionSecret ||
		(event.Redaction == values.RedactionPrivate && !policy.RevealsPrivate())
	if event.Attributes != nil {
		rendered.Attributes = make(map[string]string, len(event.Attributes))
		for key, value := range event.Attributes {
			if mask {
				value = values.RedactedMarker
			}
			rendered.Attributes[key] = value
		}
	}
	rendered.Masked = mask
	return rendered, nil
}
