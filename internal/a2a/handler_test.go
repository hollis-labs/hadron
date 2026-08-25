package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

type fakeCorrelations struct {
	records map[string]hoststate.A2ATaskCorrelation
	puts    []hoststate.A2ATaskCorrelation
	gets    []appworkflow.A2ATaskOperation
}

func (f *fakeCorrelations) Put(_ context.Context, _ appworkflow.IdentityRequest, input hoststate.A2ATaskCorrelation) (hoststate.A2ATaskCorrelation, workflowruntime.IdempotencyOutcome, error) {
	if input.TaskID == "" {
		input.TaskID = "task-" + strings.TrimPrefix(values.SHA256Digest([]byte(input.IdempotencyKey)), "sha256:")
	}
	if input.RunID == "" {
		input.RunID = appworkflow.RunID("a2a-" + strings.TrimPrefix(values.SHA256Digest([]byte(input.TaskID+"\x00"+input.RequestDigest)), "sha256:"))
	}
	if input.HostStartKey == "" {
		input.HostStartKey = "a2a-start-" + strings.TrimPrefix(values.SHA256Digest([]byte(input.TaskID+"\x00"+input.IdempotencyKey)), "sha256:")
	}
	f.puts = append(f.puts, input)
	prior, ok := f.records[input.TaskID]
	if ok {
		if prior.RunID != input.RunID || prior.Definition != input.Definition || prior.RequestDigest != input.RequestDigest || prior.IdempotencyKey != input.IdempotencyKey || prior.HostStartKey != input.HostStartKey {
			return hoststate.A2ATaskCorrelation{}, "", workflowruntime.ErrIdempotencyConflict
		}
		return prior, workflowruntime.IdempotencyReplayed, nil
	}
	f.records[input.TaskID] = input
	return input, workflowruntime.IdempotencyApplied, nil
}

func (f *fakeCorrelations) Get(_ context.Context, _ appworkflow.IdentityRequest, taskID string, operation appworkflow.A2ATaskOperation) (hoststate.A2ATaskCorrelation, error) {
	f.gets = append(f.gets, operation)
	record, ok := f.records[taskID]
	if !ok {
		return hoststate.A2ATaskCorrelation{}, workflowruntime.ErrNotFound
	}
	return record, nil
}

type fakeWorkflowService struct {
	runs       []appworkflow.RunWorkflowRequest
	inspects   []appworkflow.InspectWorkflowRunRequest
	cancels    []appworkflow.CancelWorkflowRunRequest
	resumes    []appworkflow.ResumeWorkflowRunRequest
	inspect    rundiagnostics.Result
	inspectErr error
	waits      appworkflow.WorkflowWaitListResult
	values     appworkflow.WorkflowValueListResult
	events     appworkflow.WorkflowEventListResult
	readCalls  []appworkflow.WorkflowRunReadRequest
	runErrors  []error
}

func (*fakeWorkflowService) ValidateWorkflow(context.Context, appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
	return appworkflow.ValidateWorkflowResult{Diagnostics: []diagnostic.Diagnostic{}}, nil
}

func (*fakeWorkflowService) ExplainWorkflow(context.Context, appworkflow.ExplainWorkflowRequest) (appworkflow.StartRunResult, error) {
	return appworkflow.StartRunResult{}, nil
}

func (f *fakeWorkflowService) RunWorkflow(_ context.Context, request appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
	f.runs = append(f.runs, request)
	status := workflowruntime.RunRunning
	outcome := workflowruntime.IdempotencyApplied
	if len(f.runs) > 1 {
		outcome = workflowruntime.IdempotencyReplayed
	}
	var err error
	if len(f.runs) <= len(f.runErrors) {
		err = f.runErrors[len(f.runs)-1]
	}
	return appworkflow.StartRunResult{Run: &workflowruntime.RunSnapshot{ID: request.RunID, Status: status}, Outcome: outcome}, err
}

func (f *fakeWorkflowService) InspectWorkflowRun(_ context.Context, request appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error) {
	f.inspects = append(f.inspects, request)
	return f.inspect, f.inspectErr
}

func (f *fakeWorkflowService) CancelWorkflowRun(_ context.Context, request appworkflow.CancelWorkflowRunRequest) (appworkflow.CancelWorkflowRunResult, error) {
	f.cancels = append(f.cancels, request)
	return appworkflow.CancelWorkflowRunResult{}, nil
}

func (f *fakeWorkflowService) ResumeWorkflowRun(_ context.Context, request appworkflow.ResumeWorkflowRunRequest) (appworkflow.ResumeWorkflowRunResult, error) {
	f.resumes = append(f.resumes, request)
	outcome := workflowruntime.ResumeApplied
	if len(f.resumes) > 1 {
		outcome = workflowruntime.ResumeReplayed
	}
	return appworkflow.ResumeWorkflowRunResult{Outcome: outcome}, nil
}

func (*fakeWorkflowService) RerunWorkflow(context.Context, appworkflow.RerunWorkflowRequest) (appworkflow.RerunWorkflowResult, error) {
	return appworkflow.RerunWorkflowResult{}, nil
}

func (f *fakeWorkflowService) ListWorkflowWaits(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowWaitListResult, error) {
	f.readCalls = append(f.readCalls, request)
	return f.waits, nil
}

func (f *fakeWorkflowService) FetchWorkflowValues(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowValueListResult, error) {
	f.readCalls = append(f.readCalls, request)
	return f.values, nil
}

func (f *fakeWorkflowService) FetchWorkflowEvents(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
	f.readCalls = append(f.readCalls, request)
	return f.events, nil
}

func TestSubmitTaskUsesExactWorkflowIntentAndDeterministicReplay(t *testing.T) {
	correlations := &fakeCorrelations{records: make(map[string]hoststate.A2ATaskCorrelation)}
	workflows := &fakeWorkflowService{}
	handler := testHandler(t, correlations, workflows)
	definition := testDefinition()
	request := TaskRequest{
		ID: "client-task", Skill: definition, Input: map[string]any{"count": json.Number("9007199254740993")},
		IdempotencyKey: "start-key", Confirmed: true,
		RunScope:        &hoststate.RunScopeSelector{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "project-one"},
		ExecutionTarget: &hoststate.ExecutionTargetSelector{Version: hoststate.ScopeTargetVersionV1, ID: "runner-one"},
	}
	first, err := handler.SubmitTask(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := handler.SubmitTask(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.RunID == "a2a-client-task" || first.RunID != second.RunID || !strings.HasPrefix(string(first.RunID), "a2a-") {
		t.Fatalf("deterministic collision-resistant run ids = %q / %q", first.RunID, second.RunID)
	}
	if first.Outcome != workflowruntime.IdempotencyApplied || second.Outcome != workflowruntime.IdempotencyReplayed || len(workflows.runs) != 2 {
		t.Fatalf("start outcomes = %q/%q calls=%d", first.Outcome, second.Outcome, len(workflows.runs))
	}
	for _, run := range workflows.runs {
		if run.RunID != first.RunID || run.Definition != definition || run.IdempotencyKey == "start-key" || !strings.HasPrefix(run.IdempotencyKey, "a2a-start-") || run.Identity.SourceAuthority != "a2a" || run.Identity.PrincipalHint != "" || len(run.Identity.Attributes) != 0 || !reflect.DeepEqual(run.Identity.RunScope, request.RunScope) || !reflect.DeepEqual(run.Identity.ExecutionTarget, request.ExecutionTarget) {
			t.Fatalf("workflow request = %#v", run)
		}
	}
	changes := map[string]func(*TaskRequest){
		"input": func(changed *TaskRequest) { changed.Input = map[string]any{"count": json.Number("2")} },
		"definition": func(changed *TaskRequest) {
			changed.Skill.Version = "v2"
			changed.Skill.Digest = values.SHA256Digest([]byte("workflow-v2"))
		},
		"scope":  func(changed *TaskRequest) { changed.RunScope.ID = "project-two" },
		"target": func(changed *TaskRequest) { changed.ExecutionTarget.ID = "runner-two" },
	}
	for name, mutate := range changes {
		changed := request
		if request.RunScope != nil {
			scope := *request.RunScope
			changed.RunScope = &scope
		}
		if request.ExecutionTarget != nil {
			target := *request.ExecutionTarget
			changed.ExecutionTarget = &target
		}
		mutate(&changed)
		if _, err := handler.SubmitTask(t.Context(), changed); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
			t.Fatalf("changed %s task intent = %v", name, err)
		}
	}
	if len(workflows.runs) != 2 {
		t.Fatal("conflicting correlation reached workflow start")
	}
}

func TestSubmitTaskConfirmationEscalationReusesCorrelationAndRun(t *testing.T) {
	correlations := &fakeCorrelations{records: make(map[string]hoststate.A2ATaskCorrelation)}
	workflows := &fakeWorkflowService{runErrors: []error{appworkflow.ErrConfirmationRequired, nil}}
	handler := testHandler(t, correlations, workflows)
	request := TaskRequest{ID: "confirmation-task", Skill: testDefinition(), Input: map[string]any{"count": json.Number("1")}, IdempotencyKey: "confirmation-key"}
	first, err := handler.SubmitTask(t.Context(), request)
	if !errors.Is(err, appworkflow.ErrConfirmationRequired) {
		t.Fatalf("initial confirmation error = %v", err)
	}
	request.Confirmed = true
	second, err := handler.SubmitTask(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || first.RunID == "" || first.RunID != second.RunID || len(correlations.records) != 1 || len(workflows.runs) != 2 || workflows.runs[0].Confirmed || !workflows.runs[1].Confirmed {
		t.Fatalf("confirmation replay first=%#v second=%#v runs=%#v", first, second, workflows.runs)
	}
}

func TestSubmitTaskWithoutIDReplaysAcrossHandlerInstancesAndConflictsOnChangedIntent(t *testing.T) {
	records := make(map[string]hoststate.A2ATaskCorrelation)
	workflows := &fakeWorkflowService{}
	firstHandler := testHandler(t, &fakeCorrelations{records: records}, workflows)
	secondHandler := testHandler(t, &fakeCorrelations{records: records}, workflows)
	request := TaskRequest{Skill: testDefinition(), Input: map[string]any{"count": json.Number("1")}, IdempotencyKey: "restart-stable-key"}

	first, err := firstHandler.SubmitTask(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := secondHandler.SubmitTask(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == "" || first.ID != second.ID || first.RunID == "" || first.RunID != second.RunID || len(records) != 1 {
		t.Fatalf("generated replay identities = %#v / %#v records=%d", first, second, len(records))
	}
	changed := request
	changed.Input = map[string]any{"count": json.Number("2")}
	if _, err := secondHandler.SubmitTask(t.Context(), changed); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed generated-id intent error = %v", err)
	}
	if len(workflows.runs) != 2 {
		t.Fatalf("conflicting generated-id request reached workflow start: %d", len(workflows.runs))
	}
}

func TestGetTaskProjectsWaitsRedactionEventsAndTerminalOutput(t *testing.T) {
	correlation := testCorrelation("task-final")
	correlations := &fakeCorrelations{records: map[string]hoststate.A2ATaskCorrelation{correlation.TaskID: correlation}}
	outputRef := values.ValueSetRef{ID: "outputs", Digest: values.SHA256Digest([]byte("outputs"))}
	masked := values.RenderedValue{Type: values.TypeString, Payload: values.RedactedMarker, Producer: values.Producer{Kind: "node", Reference: "root", Output: "secret"}, MediaType: "application/json", Digest: values.SHA256Digest([]byte("secret")), Redaction: values.RedactionSecret, Retention: values.RetentionRun, Masked: true}
	workflows := &fakeWorkflowService{
		inspect: rundiagnostics.Result{Run: rundiagnostics.RunDiagnostic{ID: correlation.RunID, Status: workflowruntime.RunSucceeded, Outputs: &outputRef}},
		waits:   appworkflow.WorkflowWaitListResult{RunID: correlation.RunID, Waits: []appworkflow.WorkflowWaitListItem{{Wait: rundiagnostics.WaitDiagnostic{ID: "wait-one", Status: workflowwait.StatusResumed, WakeSource: workflowwait.WakeGate}}}},
		values:  appworkflow.WorkflowValueListResult{RunID: correlation.RunID, Values: []rundiagnostics.ValueSetDiagnostic{{Ref: outputRef, Roles: []string{"run_outputs"}, Values: values.RenderedValueSet{"secret": masked}}}},
		events:  appworkflow.WorkflowEventListResult{RunID: correlation.RunID, Events: []workflowruntime.RenderedEvent{{Sequence: 1, RunID: correlation.RunID, Type: "run.completed", Redaction: values.RedactionPrivate, Masked: true}}},
	}
	response, err := testHandler(t, correlations, workflows).GetTask(t.Context(), correlation.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if !response.Available || response.Status.State != "completed" || response.Result == nil || !response.Result.Output["secret"].Masked || len(response.Waits) != 1 || len(response.Events) != 1 {
		t.Fatalf("task response = %#v", response)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "resolved-secret-material") || !strings.Contains(string(encoded), string(values.RedactedMarker)) {
		t.Fatalf("unsafe redaction projection: %s", encoded)
	}
}

func TestGetTaskReportsOwnerAuthorizedPreAdmissionCorrelationAsUnavailable(t *testing.T) {
	correlation := testCorrelation("task-pending")
	correlations := &fakeCorrelations{records: map[string]hoststate.A2ATaskCorrelation{correlation.TaskID: correlation}}
	workflows := &fakeWorkflowService{inspectErr: appworkflow.ErrHostNotReady}
	response, err := testHandler(t, correlations, workflows).GetTask(t.Context(), correlation.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if response.Available || response.Status.State != "submitted" || len(response.Omissions) != 1 || len(workflows.inspects) != 1 {
		t.Fatalf("pre-admission response = %#v", response)
	}
}

func TestGetTaskDoesNotProjectDeniedInspectionAsPending(t *testing.T) {
	correlation := testCorrelation("task-denied")
	correlations := &fakeCorrelations{records: map[string]hoststate.A2ATaskCorrelation{correlation.TaskID: correlation}}
	workflows := &fakeWorkflowService{inspectErr: appworkflow.ErrPolicyDenied}
	response, err := testHandler(t, correlations, workflows).GetTask(t.Context(), correlation.TaskID)
	if response != nil || !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("denied inspection = %#v, %v", response, err)
	}
	if len(workflows.readCalls) != 0 {
		t.Fatalf("denied inspection reached run reads: %#v", workflows.readCalls)
	}
}

func TestCancelAndResumeUseCorrelatedRunAndExplicitIdempotency(t *testing.T) {
	correlation := testCorrelation("task-control")
	correlations := &fakeCorrelations{records: map[string]hoststate.A2ATaskCorrelation{correlation.TaskID: correlation}}
	workflows := &fakeWorkflowService{inspect: rundiagnostics.Result{Run: rundiagnostics.RunDiagnostic{ID: correlation.RunID, Status: workflowruntime.RunCanceled}}}
	handler := testHandler(t, correlations, workflows)
	if response, err := handler.CancelTask(t.Context(), correlation.TaskID, CancelTaskRequest{IdempotencyKey: "cancel-key", Reason: "operator"}); err != nil || response.Status.State != "canceled" {
		t.Fatalf("cancel response = %#v, %v", response, err)
	}
	if response, err := handler.CancelTask(t.Context(), correlation.TaskID, CancelTaskRequest{IdempotencyKey: "cancel-key", Reason: "operator"}); err != nil || response.Status.State != "canceled" {
		t.Fatalf("cancel replay response = %#v, %v", response, err)
	}
	payload, err := values.NewInline("approve", values.Metadata{Producer: values.Producer{Kind: "a2a", Reference: correlation.TaskID}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	resume := ResumeTaskRequest{WaitID: "wait-one", Correlation: "approval-one", WakeSource: workflowwait.WakeGate, Payload: payload, IdempotencyKey: "resume-key"}
	if response, err := handler.ResumeTask(t.Context(), correlation.TaskID, resume); err != nil || response.Resume.Outcome != workflowruntime.ResumeApplied {
		t.Fatalf("resume response = %#v, %v", response, err)
	}
	if response, err := handler.ResumeTask(t.Context(), correlation.TaskID, resume); err != nil || response.Resume.Outcome != workflowruntime.ResumeReplayed {
		t.Fatalf("resume replay response = %#v, %v", response, err)
	}
	if len(workflows.cancels) != 2 || !reflect.DeepEqual(workflows.cancels[0], workflows.cancels[1]) || workflows.cancels[0].RunID != correlation.RunID || workflows.cancels[0].IdempotencyKey != "cancel-key" || len(workflows.resumes) != 2 || workflows.resumes[0].RunID != correlation.RunID || workflows.resumes[0].IdempotencyKey != "resume-key" || workflows.resumes[0].WaitID != "wait-one" || workflows.resumes[0].Correlation != "approval-one" || !reflect.DeepEqual(workflows.resumes[0], workflows.resumes[1]) {
		t.Fatalf("control requests cancel=%#v resume=%#v", workflows.cancels, workflows.resumes)
	}
	if len(workflows.inspects) != 2 || workflows.inspects[0].Identity.RunScope == nil || workflows.inspects[0].Identity.RunScope.ID != "project-one" || workflows.inspects[0].Identity.ExecutionTarget == nil || workflows.inspects[0].Identity.ExecutionTarget.ID != "runner-one" || workflows.cancels[0].Identity.RunScope == nil || workflows.cancels[0].Identity.RunScope.ID != "project-one" || workflows.cancels[0].Identity.ExecutionTarget == nil || workflows.cancels[0].Identity.ExecutionTarget.ID != "runner-one" {
		t.Fatalf("owner selectors inspect=%#v cancel=%#v", workflows.inspects, workflows.cancels)
	}
	if len(workflows.readCalls) != 6 {
		t.Fatalf("read calls = %#v", workflows.readCalls)
	}
	for _, read := range workflows.readCalls {
		if read.Identity.RunScope == nil || read.Identity.RunScope.ID != "project-one" || read.Identity.ExecutionTarget == nil || read.Identity.ExecutionTarget.ID != "runner-one" {
			t.Fatalf("read owner selectors = %#v", read.Identity)
		}
	}
	resume.IdempotencyKey = ""
	if _, err := handler.ResumeTask(t.Context(), correlation.TaskID, resume); !errors.Is(err, appworkflow.ErrWorkflowInvalidRequest) {
		t.Fatalf("missing resume key error = %v", err)
	}
	if got := correlations.gets[len(correlations.gets)-1]; got != appworkflow.A2ATaskResume {
		t.Fatalf("resume correlation operation = %q", got)
	}
}

func testHandler(t *testing.T, correlations *fakeCorrelations, workflows *fakeWorkflowService) *Handler {
	t.Helper()
	handler, err := NewHandler(Options{Correlations: correlations, Workflows: workflows, Reads: workflows})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func testDefinition() graph.DefinitionRef {
	return graph.DefinitionRef{Kind: "registry", ID: "team/workflow", Version: "v1", Digest: values.SHA256Digest([]byte("workflow"))}
}

func testCorrelation(taskID string) hoststate.A2ATaskCorrelation {
	return hoststate.A2ATaskCorrelation{
		TaskID: taskID, RunID: appworkflow.RunID("run-" + taskID), Definition: testDefinition(), RequestDigest: values.SHA256Digest([]byte(taskID)), IdempotencyKey: "start-key", HostStartKey: "a2a-start-" + taskID, CreatedAt: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
		Owner: hoststate.IdentityBinding{
			RunScope:        hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "project-one"},
			ExecutionTarget: &hoststate.ExecutionTarget{Version: hoststate.ScopeTargetVersionV1, ID: "runner-one"},
		},
	}
}
