package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestSaveValuesWithRetentionGroupsMixedClassesAndNilHook(t *testing.T) {
	t.Parallel()
	store := inmemory.NewStore()
	request := classificationSaveRequest(t)
	var before workflowruntime.RetentionPlan
	var after workflowruntime.RetentionRecord
	hook := workflowruntime.RetentionHookFuncs{
		Before: func(_ context.Context, plan workflowruntime.RetentionPlan) error {
			before = plan
			return nil
		},
		After: func(_ context.Context, record workflowruntime.RetentionRecord) error {
			after = record
			return nil
		},
	}
	ref, err := workflowruntime.SaveValuesWithRetention(t.Context(), store, hook, request)
	if err != nil {
		t.Fatal(err)
	}
	wantGroups := []workflowruntime.RetentionGroup{
		{Class: values.RetentionRun, Names: []string{"alpha", "zeta"}},
		{Class: values.RetentionProject, Names: []string{"project"}},
		{Class: values.RetentionExternal, Names: []string{"external"}},
	}
	if before.Owner != request.Owner || !reflect.DeepEqual(before.Groups, wantGroups) ||
		after.Ref != ref || !reflect.DeepEqual(after.Plan, before) {
		t.Fatalf("retention hooks:\nbefore=%#v\nafter=%#v", before, after)
	}
	if _, err := store.LoadValues(t.Context(), ref); err != nil {
		t.Fatalf("saved mixed set: %v", err)
	}
	if _, err := workflowruntime.SaveValuesWithRetention(t.Context(), store, nil, request); err != nil {
		t.Fatalf("nil hook rejected: %v", err)
	}
}

func TestSaveValuesWithRetentionFailureBoundaries(t *testing.T) {
	t.Parallel()
	request := classificationSaveRequest(t)
	beforeCause := errors.New("prepare cleanup")
	beforeStore := inmemory.NewStore()
	_, err := workflowruntime.SaveValuesWithRetention(t.Context(), beforeStore, workflowruntime.RetentionHookFuncs{
		Before: func(context.Context, workflowruntime.RetentionPlan) error { return beforeCause },
	}, request)
	var beforeError *workflowruntime.RetentionHookError
	if !errors.Is(err, workflowruntime.ErrRetentionHook) || !errors.Is(err, beforeCause) ||
		!errors.As(err, &beforeError) || beforeError.Ref != nil || beforeError.Phase != "before_persist" {
		t.Fatalf("before error = %#v, %v", beforeError, err)
	}

	afterCause := errors.New("record cleanup")
	afterStore := inmemory.NewStore()
	_, err = workflowruntime.SaveValuesWithRetention(t.Context(), afterStore, workflowruntime.RetentionHookFuncs{
		After: func(context.Context, workflowruntime.RetentionRecord) error { return afterCause },
	}, request)
	var afterError *workflowruntime.RetentionHookError
	if !errors.Is(err, workflowruntime.ErrRetentionHook) || !errors.Is(err, afterCause) ||
		!errors.As(err, &afterError) || afterError.Ref == nil || afterError.Phase != "after_persist" {
		t.Fatalf("after error = %#v, %v", afterError, err)
	}
	if _, loadErr := afterStore.LoadValues(t.Context(), *afterError.Ref); loadErr != nil {
		t.Fatalf("post-persist error ref does not identify immutable set: %v", loadErr)
	}

	none, _ := values.NewInline("ephemeral", classificationRuntimeMetadata(values.RedactionPrivate, values.RetentionNone, "none"))
	noneRequest := workflowruntime.SaveValuesRequest{
		Owner: request.Owner, Values: values.ValueSet{"ephemeral": none},
	}
	called := false
	_, err = workflowruntime.SaveValuesWithRetention(t.Context(), inmemory.NewStore(), workflowruntime.RetentionHookFuncs{
		Before: func(context.Context, workflowruntime.RetentionPlan) error { called = true; return nil },
	}, noneRequest)
	if !errors.Is(err, values.ErrRetentionViolation) || called {
		t.Fatalf("retention none = %v, hook called=%v", err, called)
	}
	if _, err := beforeStore.SaveValues(t.Context(), noneRequest); !errors.Is(err, values.ErrRetentionViolation) {
		t.Fatalf("direct store accepted retention none: %v", err)
	}
}

func TestAppendMaskedEventMasksAdapterObservationsBeforePersistence(t *testing.T) {
	t.Parallel()
	ref, _ := values.ParseSecretRef("secret://project/service#token")
	resolved, _ := values.NewResolvedSecret(ref, []byte("token-123"))
	redactor, _ := values.NewRedactor(resolved)
	store := inmemory.NewStore()
	now := time.Date(2026, 8, 24, 16, 0, 0, 0, time.UTC)
	for index, channel := range []string{"command", "prompt", "message", "http", "mcp"} {
		request := workflowruntime.AppendEventRequest{
			RunID: "run-masked", Type: channel + "_observation", OccurredAt: now.Add(time.Duration(index) * time.Second),
			Attributes: map[string]string{"channel": channel, "payload": "Bearer token-123"},
			Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
		}
		prepared := workflowruntime.MaskEventRequest(request, redactor)
		if prepared.Attributes["payload"] != "Bearer "+values.RedactedMarker || request.Attributes["payload"] != "Bearer token-123" {
			t.Fatalf("MaskEventRequest(%s) = %#v; input=%#v", channel, prepared.Attributes, request.Attributes)
		}
		if _, err := workflowruntime.AppendMaskedEvent(t.Context(), store, request, redactor); err != nil {
			t.Fatalf("AppendMaskedEvent(%s): %v", channel, err)
		}
	}
	events, err := store.ListEvents(t.Context(), workflowruntime.EventQuery{RunID: "run-masked"})
	if err != nil || len(events) != 5 {
		t.Fatalf("ListEvents = %d, %v", len(events), err)
	}
	for _, event := range events {
		if event.Attributes["payload"] != "Bearer "+values.RedactedMarker {
			t.Fatalf("persisted event leaked: %#v", event)
		}
	}
}

func TestRenderEventUsesSharedFailClosedDisplayPolicy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 24, 16, 0, 0, 0, time.UTC)
	base := workflowruntime.Event{
		Sequence: 1, RunID: "run-render", Type: "observation", OccurredAt: now,
		Attributes: map[string]string{"body": "payload"}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}
	masked, err := workflowruntime.RenderEvent(base, values.DisplayPolicy{})
	if err != nil || masked.Attributes["body"] != values.RedactedMarker || !masked.Masked {
		t.Fatalf("default event render = %#v, %v", masked, err)
	}
	revealed, err := workflowruntime.RenderEvent(base, values.DisplayPolicy{Private: values.PrivateDisplayReveal})
	if err != nil || revealed.Attributes["body"] != "payload" || revealed.Masked {
		t.Fatalf("revealed event = %#v, %v", revealed, err)
	}
	secret := base
	secret.Redaction = values.RedactionSecret
	stillMasked, err := workflowruntime.RenderEvent(secret, values.DisplayPolicy{Private: values.PrivateDisplayReveal})
	if err != nil || stillMasked.Attributes["body"] != values.RedactedMarker || !stillMasked.Masked {
		t.Fatalf("secret event revealed = %#v, %v", stillMasked, err)
	}
	if _, err := workflowruntime.RenderEvent(base, values.DisplayPolicy{Private: "show"}); err == nil {
		t.Fatal("invalid display policy accepted")
	}
}

func classificationSaveRequest(t *testing.T) workflowruntime.SaveValuesRequest {
	t.Helper()
	alpha, _ := values.NewInline("alpha", classificationRuntimeMetadata(values.RedactionPrivate, values.RetentionRun, "alpha"))
	zeta, _ := values.NewInline("zeta", classificationRuntimeMetadata(values.RedactionPrivate, values.RetentionRun, "zeta"))
	project, _ := values.NewInline("project", classificationRuntimeMetadata(values.RedactionPublic, values.RetentionProject, "project"))
	external, err := values.NewArtifact(values.ArtifactRef{
		Store: "external", URI: "artifact://outside/blob", Digest: values.SHA256Digest([]byte("outside")),
		MediaType: "application/octet-stream", SizeBytes: 7,
		Producer:  classificationRuntimeMetadata(values.RedactionSecret, values.RetentionExternal, "external").Producer,
		Redaction: values.RedactionSecret, Retention: values.RetentionExternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	return workflowruntime.SaveValuesRequest{
		Owner:  workflowruntime.ValueOwner{Kind: "test-values", RunID: "run-retention"},
		Values: values.ValueSet{"zeta": zeta, "project": project, "external": external, "alpha": alpha},
	}
}

func classificationRuntimeMetadata(
	redaction values.RedactionClass,
	retention values.RetentionClass,
	reference string,
) values.Metadata {
	return values.Metadata{
		Producer:  values.Producer{Kind: "test", Reference: reference, Output: "value"},
		MediaType: "application/json", Redaction: redaction, Retention: retention,
	}
}
