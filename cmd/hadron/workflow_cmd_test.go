package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/go-workflow/diagnostic"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

type workflowServiceSpy struct {
	validate func(context.Context, appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error)
	explain  func(context.Context, appworkflow.ExplainWorkflowRequest) (appworkflow.StartRunResult, error)
	run      func(context.Context, appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error)
	inspect  func(context.Context, appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error)
	cancel   func(context.Context, appworkflow.CancelWorkflowRunRequest) (appworkflow.CancelWorkflowRunResult, error)
	resume   func(context.Context, appworkflow.ResumeWorkflowRunRequest) (appworkflow.ResumeWorkflowRunResult, error)
	rerun    func(context.Context, appworkflow.RerunWorkflowRequest) (appworkflow.RerunWorkflowResult, error)
}

func (s *workflowServiceSpy) ValidateWorkflow(ctx context.Context, request appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
	return s.validate(ctx, request)
}
func (s *workflowServiceSpy) ExplainWorkflow(ctx context.Context, request appworkflow.ExplainWorkflowRequest) (appworkflow.StartRunResult, error) {
	return s.explain(ctx, request)
}
func (s *workflowServiceSpy) RunWorkflow(ctx context.Context, request appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
	return s.run(ctx, request)
}
func (s *workflowServiceSpy) InspectWorkflowRun(ctx context.Context, request appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error) {
	return s.inspect(ctx, request)
}
func (s *workflowServiceSpy) CancelWorkflowRun(ctx context.Context, request appworkflow.CancelWorkflowRunRequest) (appworkflow.CancelWorkflowRunResult, error) {
	return s.cancel(ctx, request)
}
func (s *workflowServiceSpy) ResumeWorkflowRun(ctx context.Context, request appworkflow.ResumeWorkflowRunRequest) (appworkflow.ResumeWorkflowRunResult, error) {
	return s.resume(ctx, request)
}
func (s *workflowServiceSpy) RerunWorkflow(ctx context.Context, request appworkflow.RerunWorkflowRequest) (appworkflow.RerunWorkflowResult, error) {
	return s.rerun(ctx, request)
}

func TestWorkflowValidateRoutesFileRefWithoutStartingRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sample.workflow.yaml")
	if err := os.WriteFile(path, []byte("workflow: {name: Sample, version: v1}\nsteps: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var captured appworkflow.ValidateWorkflowRequest
	runs := 0
	plan := workflowruntime.PlanRef{ID: "sample", Version: "v1", Digest: values.SHA256Digest([]byte("sample")), SchemaVersion: "1"}
	service := completeWorkflowSpy()
	service.validate = func(_ context.Context, request appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
		captured = request
		return appworkflow.ValidateWorkflowResult{Definition: request.Definition, Plan: &plan}, nil
	}
	service.run = func(context.Context, appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
		runs++
		return appworkflow.StartRunResult{}, nil
	}
	output, err := executeWorkflowCommand(t, service, "validate", path)
	if err != nil {
		t.Fatal(err)
	}
	want := readWorkflowGolden(t, "validate_valid.golden")
	if output != want {
		t.Fatalf("output:\n%s\nwant:\n%s", output, want)
	}
	if captured.Definition.Kind != appworkflow.DefinitionKindFile || captured.Definition.Locator != filepath.Clean(path) || captured.Definition.ID != "" || runs != 0 {
		t.Fatalf("request=%#v run calls=%d", captured, runs)
	}
}

func TestWorkflowValidateDiagnosticGolden(t *testing.T) {
	service := completeWorkflowSpy()
	service.validate = func(_ context.Context, request appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
		return appworkflow.ValidateWorkflowResult{Definition: request.Definition, Diagnostics: []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Code: "HADR-CLI-TEST", Message: "node requires a declared dependency", Source: &graph.SourceRef{Locator: "invalid.workflow.yaml", StartLine: 9, StartColumn: 5}}}}, nil
	}
	output, err := executeWorkflowCommand(t, service, "validate", "invalid.workflow.yaml")
	if err == nil {
		t.Fatal("invalid workflow command succeeded")
	}
	if want := readWorkflowGolden(t, "validate_diagnostic.golden"); output != want {
		t.Fatalf("output:\n%s\nwant:\n%s", output, want)
	}
}

func TestWorkflowValidateJSONIsStructuredAndStillFails(t *testing.T) {
	service := completeWorkflowSpy()
	service.validate = func(_ context.Context, request appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
		return appworkflow.ValidateWorkflowResult{Definition: request.Definition, Diagnostics: []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Code: "HADR-CLI-JSON", Message: "invalid graph"}}}, nil
	}
	output, err := executeWorkflowCommand(t, service, "validate", "invalid.workflow.yaml", "--json")
	if err == nil || !strings.Contains(output, `"code": "HADR-CLI-JSON"`) {
		t.Fatalf("output=%s error=%v", output, err)
	}
}

func TestWorkflowRunBuildsTypedCanonicalRequest(t *testing.T) {
	service := completeWorkflowSpy()
	var captured appworkflow.RunWorkflowRequest
	service.run = func(_ context.Context, request appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
		captured = request
		return appworkflow.StartRunResult{Bound: &workflowruntime.BoundRun{ID: request.RunID}, Phase: hoststate.StartRunning, Run: &workflowruntime.RunSnapshot{ID: request.RunID, Status: workflowruntime.RunRunning}}, nil
	}
	pinDigest := values.SHA256Digest([]byte("pin"))
	output, err := executeWorkflowCommand(t, service,
		"run", "torque/task-bulk-create@1.2.0", "--input-json", `{"count":2,"nested":{"ok":true}}`,
		"--scope-kind", "project", "--scope-id", "hadron", "--target-kind", "remote_runner,local",
		"--target-capability", "network,compute", "--target-label", "region=central", "--target-sandbox", "isolated",
		"--pin", `apply={"id":"values-one","digest":"`+pinDigest+`"}`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, "phase=running status=running") {
		t.Fatalf("output=%q", output)
	}
	if captured.Definition != (graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: "torque/task-bulk-create", Version: "1.2.0"}) {
		t.Fatalf("definition=%#v", captured.Definition)
	}
	if _, ok := captured.Inputs["count"].(json.Number); !ok {
		t.Fatalf("input number type=%T", captured.Inputs["count"])
	}
	if captured.Identity.RunScope == nil || captured.Identity.RunScope.ID != "hadron" || captured.Identity.ExecutionTarget == nil || !reflect.DeepEqual(captured.Identity.ExecutionTarget.RequiredCapabilities, []string{"compute", "network"}) {
		t.Fatalf("identity=%#v", captured.Identity)
	}
	if len(captured.Pins) != 1 || captured.Pins[0].NodeID != "apply" || captured.Pins[0].Outputs.Digest != pinDigest {
		t.Fatalf("pins=%#v", captured.Pins)
	}
}

func TestWorkflowRunEmitsTypedJSON(t *testing.T) {
	service := completeWorkflowSpy()
	inputsRef := values.ValueSetRef{ID: "run-json-inputs", Digest: values.SHA256Digest([]byte("run-json-inputs"))}
	service.run = func(_ context.Context, request appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
		return appworkflow.StartRunResult{Bound: &workflowruntime.BoundRun{ID: request.RunID, InputsRef: inputsRef}, Run: &workflowruntime.RunSnapshot{ID: request.RunID, Status: workflowruntime.RunRunning, Inputs: &inputsRef}, Phase: hoststate.StartRunning}, nil
	}
	output, err := executeWorkflowCommand(t, service, "run", "team/name@v1", "--run-id", "run-json", "--idempotency-key", "run-json-key", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var result appworkflow.StartRunResult
	if err := json.Unmarshal([]byte(output), &result); err != nil || result.Bound == nil || result.Bound.ID != "run-json" || result.Run == nil || result.Run.Status != workflowruntime.RunRunning {
		t.Fatalf("typed result=%#v error=%v output=%s", result, err, output)
	}
}

func TestWorkflowRunJSONPreservesTerminalPinRejection(t *testing.T) {
	service := completeWorkflowSpy()
	inputsRef := values.ValueSetRef{ID: "pin-rejected-inputs", Digest: values.SHA256Digest([]byte("pin-rejected-inputs"))}
	service.run = func(_ context.Context, request appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
		return appworkflow.StartRunResult{
			Bound: &workflowruntime.BoundRun{ID: request.RunID, InputsRef: inputsRef},
			Run:   &workflowruntime.RunSnapshot{ID: request.RunID, Status: workflowruntime.RunCanceled, Inputs: &inputsRef},
			Phase: hoststate.StartPinsRejected,
		}, errors.New("pinned output rejected")
	}
	command := buildWorkflowCmdWithDependencies(testWorkflowDependencies(service))
	var stdout, stderr bytes.Buffer
	command.SetOut(&stdout)
	command.SetErr(&stderr)
	command.SetArgs([]string{"run", "team/name@v1", "--run-id", "pin-rejected-run", "--idempotency-key", "pin-rejected-key", "--json"})
	err := command.Execute()
	if err == nil {
		t.Fatal("rejected pinned start returned success")
	}
	var result appworkflow.StartRunResult
	if decodeErr := json.Unmarshal(stdout.Bytes(), &result); decodeErr != nil || !result.RejectedBeforeAdmission() || result.Run.ID != "pin-rejected-run" {
		t.Fatalf("structured rejection=%#v decode=%v stdout=%s stderr=%s", result, decodeErr, stdout.String(), stderr.String())
	}
}

func TestWorkflowJSONParsingRejectsAmbiguityAndDuplicateData(t *testing.T) {
	for _, input := range []string{`{"a":1,"a":2}`, `{"a":1} trailing`, `[1]`} {
		if _, err := readWorkflowInputs("", input); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
	for _, ref := range []string{"bare", "team/name", "team/name@", "team/../name@v1", "team/name@v1@v2"} {
		if _, err := parseWorkflowDefinitionRef(ref); err == nil {
			t.Fatalf("accepted ref %q", ref)
		}
	}
	digest := values.SHA256Digest([]byte("exact"))
	ref, err := parseWorkflowDefinitionRef("team/name@" + digest)
	if err != nil || ref.ID != "team/name" || ref.Digest != digest || ref.Version != "" {
		t.Fatalf("digest ref=%#v, %v", ref, err)
	}
	deep := strings.Repeat("[", 130) + "0" + strings.Repeat("]", 130)
	if _, err := decodeUniqueWorkflowJSON([]byte(deep)); err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("deep JSON error=%v", err)
	}
	for _, payload := range []string{`{"type":"string","type":"string"}`, `{"unknown":true}`} {
		if _, err := readTypedWorkflowValue("", payload); err == nil {
			t.Fatalf("accepted typed payload %s", payload)
		}
	}
	pinStrictDigest := values.SHA256Digest([]byte("pin-strict"))
	for _, pin := range []string{
		`node={"id":"one","id":"two","digest":"` + pinStrictDigest + `"}`,
		`node={"id":"one","digest":"` + pinStrictDigest + `","unknown":true}`,
	} {
		if _, err := parseWorkflowPins([]string{pin}); err == nil {
			t.Fatalf("accepted pin %s", pin)
		}
	}
}

func TestWorkflowCommandImportBoundary(t *testing.T) {
	for _, name := range []string{"workflow_cmd.go", "workflow_lifecycle_cmd.go"} {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"internal/persistence", "workflow/compile", "workflow/runtime"} {
			if bytes.Contains(source, []byte(`"github.com/hollis-labs/hadron/`+forbidden+`"`)) {
				t.Fatalf("%s imports forbidden semantic layer %s", name, forbidden)
			}
		}
	}
}

func TestWorkflowExplainFailsClosedWhenServiceRejectsDryRun(t *testing.T) {
	service := completeWorkflowSpy()
	service.explain = func(context.Context, appworkflow.ExplainWorkflowRequest) (appworkflow.StartRunResult, error) {
		return appworkflow.StartRunResult{Decision: hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "executor cannot preview"}, Facts: hoststate.PolicyFacts{Effects: graph.EffectSet{graph.EffectMutate}, RequiredCapabilities: []string{"network"}, DryRunAvailable: false}}, appworkflow.ErrDryRunUnsupported
	}
	output, err := executeWorkflowCommand(t, service, "explain", "team/name@v1")
	if !errors.Is(err, appworkflow.ErrDryRunUnsupported) || !strings.Contains(output, "effects: mutate") || !strings.Contains(output, "dry-run-supported: false") || !strings.Contains(output, "work-admitted: false") {
		t.Fatalf("output=%s error=%v", output, err)
	}
}

func TestWorkflowExplainPreservesStructuredValidationDiagnostics(t *testing.T) {
	service := completeWorkflowSpy()
	service.explain = func(context.Context, appworkflow.ExplainWorkflowRequest) (appworkflow.StartRunResult, error) {
		return appworkflow.StartRunResult{Diagnostics: []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Code: "HADR-CLI-EXPLAIN", Message: "unknown input"}}}, nil
	}
	output, err := executeWorkflowCommand(t, service, "explain", "team/name@v1", "--json")
	if err == nil || !strings.Contains(output, `"code": "HADR-CLI-EXPLAIN"`) || strings.Contains(err.Error(), "invalid non-effecting preview") {
		t.Fatalf("output=%s error=%v", output, err)
	}
}

func TestWorkflowExplainGoldenShowsPolicyEffectsAndBlastRadius(t *testing.T) {
	service := completeWorkflowSpy()
	service.explain = func(context.Context, appworkflow.ExplainWorkflowRequest) (appworkflow.StartRunResult, error) {
		return appworkflow.StartRunResult{Bound: &workflowruntime.BoundRun{ID: "explain"}, Decision: hoststate.PolicyDecision{Outcome: hoststate.PolicyConfirm, Reason: "destructive effect requires confirmation"}, Facts: hoststate.PolicyFacts{Effects: graph.EffectSet{graph.EffectCompute, graph.EffectDestructive}, RequiredCapabilities: []string{"filesystem.write", "network"}, BlastRadius: map[string]int{"destructive": 1, "compute": 2}, DryRunAvailable: true}, Phase: hoststate.StartDryRunComplete, DryRun: true}, nil
	}
	output, err := executeWorkflowCommand(t, service, "explain", "team/name@v1")
	if err != nil {
		t.Fatal(err)
	}
	if want := readWorkflowGolden(t, "explain_policy.golden"); output != want {
		t.Fatalf("output:\n%s\nwant:\n%s", output, want)
	}
}

func TestWorkflowInspectJSONPreservesMaskedTypedValues(t *testing.T) {
	service := completeWorkflowSpy()
	secretRef := values.ValueSetRef{ID: "secret-values", Digest: values.SHA256Digest([]byte("secret"))}
	service.inspect = func(context.Context, appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error) {
		return rundiagnostics.Result{SchemaVersion: "1", Run: rundiagnostics.RunDiagnostic{ID: "run-one", Status: workflowruntime.RunRunning}, Values: []rundiagnostics.ValueSetDiagnostic{{Ref: secretRef, Roles: []string{"run.inputs"}, Values: values.RenderedValueSet{"token": {Type: values.TypeString, Payload: values.RedactedMarker, Producer: values.Producer{Kind: "secret", Reference: "vault"}, MediaType: "application/json", Digest: values.SHA256Digest([]byte("payload")), Redaction: values.RedactionSecret, Retention: values.RetentionRun, Masked: true}}}}}, nil
	}
	output, err := executeWorkflowCommand(t, service, "inspect", "run-one", "--json")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output, "supersecret") || !strings.Contains(output, `"payload": "[REDACTED]"`) || !strings.Contains(output, `"masked": true`) {
		t.Fatalf("output=%s", output)
	}
}

func TestWorkflowResumeUsesTokenFileAndRedactsErrors(t *testing.T) {
	token := "one-time-supersecret-token"
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	payload, err := values.NewInline("approved", values.Metadata{Producer: values.Producer{Kind: "cli", Reference: "test"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	payloadJSON, _ := json.Marshal(payload)
	service := completeWorkflowSpy()
	service.resume = func(_ context.Context, request appworkflow.ResumeWorkflowRunRequest) (appworkflow.ResumeWorkflowRunResult, error) {
		if request.Token != token || request.Payload.Digest != payload.Digest {
			t.Fatalf("request=%#v", request)
		}
		return appworkflow.ResumeWorkflowRunResult{}, errors.New("denied presented token " + token)
	}
	_, commandErr := executeWorkflowCommand(t, service, "resume", "run-one", "--wait", "wait-one", "--correlation", "corr", "--source", "gate", "--token-file", tokenPath, "--payload-json", string(payloadJSON))
	if commandErr == nil || strings.Contains(commandErr.Error(), token) || !strings.Contains(commandErr.Error(), "[redacted]") {
		t.Fatalf("error=%v", commandErr)
	}
}

func TestWorkflowCancelAndRerunRouteExactAuthorizedIntent(t *testing.T) {
	service := completeWorkflowSpy()
	var canceled appworkflow.CancelWorkflowRunRequest
	service.cancel = func(_ context.Context, request appworkflow.CancelWorkflowRunRequest) (appworkflow.CancelWorkflowRunResult, error) {
		canceled = request
		return appworkflow.CancelWorkflowRunResult{}, nil
	}
	if _, err := executeWorkflowCommand(t, service, "cancel", "run-source", "--reason", "operator requested", "--idempotency-key", "cancel-key", "--principal", "hint"); err != nil {
		t.Fatal(err)
	}
	if canceled.RunID != "run-source" || canceled.Reason != "operator requested" || canceled.IdempotencyKey != "cancel-key" || canceled.Identity.PrincipalHint != "hint" {
		t.Fatalf("cancel=%#v", canceled)
	}
	var rerun appworkflow.RerunWorkflowRequest
	service.rerun = func(_ context.Context, request appworkflow.RerunWorkflowRequest) (appworkflow.RerunWorkflowResult, error) {
		rerun = request
		return appworkflow.RerunWorkflowResult{Run: workflowruntime.RunSnapshot{ID: request.RunID}}, nil
	}
	if _, err := executeWorkflowCommand(t, service, "rerun", "run-source", "--from", "apply", "--run-id", "run-replay", "--idempotency-key", "rerun-key"); err != nil {
		t.Fatal(err)
	}
	if rerun.SourceRunID != "run-source" || rerun.RunID != "run-replay" || rerun.FromNodeID != "apply" || rerun.IdempotencyKey != "rerun-key" {
		t.Fatalf("rerun=%#v", rerun)
	}
}

func TestWorkflowDaemonClientUsesTypedRoutesBoundsAndRedactedErrors(t *testing.T) {
	var route, contentType string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		route, contentType = request.URL.Path, request.Header.Get("Content-Type")
		if strings.Contains(route, "large-response") {
			if _, err := fmt.Fprint(writer, strings.Repeat("x", maximumWorkflowResponseBytes+1)); err != nil {
				t.Errorf("write large response: %v", err)
			}
			return
		}
		if strings.Contains(route, "denied") {
			writer.WriteHeader(http.StatusForbidden)
			if _, err := fmt.Fprint(writer, `{"code":"policy_denied","message":"server-secret"}`); err != nil {
				t.Errorf("write denied response: %v", err)
			}
			return
		}
		plan := workflowruntime.PlanRef{ID: "remote", Version: "v1", Digest: values.SHA256Digest([]byte("remote")), SchemaVersion: "1"}
		if err := json.NewEncoder(writer).Encode(appworkflow.ValidateWorkflowResult{Plan: &plan}); err != nil {
			t.Errorf("encode validation response: %v", err)
		}
	}))
	defer server.Close()
	client := workflowDaemonClient{baseURL: func() string { return server.URL }, client: server.Client()}
	result, err := client.ValidateWorkflow(t.Context(), appworkflow.ValidateWorkflowRequest{Definition: graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: "team/remote", Version: "v1"}})
	if err != nil || result.Plan == nil || result.Plan.ID != "remote" || route != "/v1/workflows/validate" || contentType != "application/json" {
		t.Fatalf("result=%#v route=%q type=%q error=%v", result, route, contentType, err)
	}
	var ignored appworkflow.ValidateWorkflowResult
	err = client.post(t.Context(), "/denied", map[string]any{}, &ignored)
	if err == nil || strings.Contains(err.Error(), "server-secret") || !strings.Contains(err.Error(), "policy_denied") {
		t.Fatalf("redacted error=%v", err)
	}
	err = client.post(t.Context(), "/oversized", map[string]string{"value": strings.Repeat("x", maximumWorkflowRequestBytes)}, &ignored)
	if err == nil || !strings.Contains(err.Error(), "exceeds 2 MiB") {
		t.Fatalf("oversized request error=%v", err)
	}
	err = client.post(t.Context(), "/large-response", map[string]any{}, &ignored)
	if err == nil || !strings.Contains(err.Error(), "exceeds 8 MiB") {
		t.Fatalf("oversized response error=%v", err)
	}
	dryRunServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusUnprocessableEntity)
		if encodeErr := json.NewEncoder(writer).Encode(appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeDryRunUnsupported, Diagnostics: []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Code: "HADR-DRY-RUN", Message: "preview unavailable"}}, Result: &appworkflow.StartRunResult{Decision: hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "executor cannot preview"}, Facts: hoststate.PolicyFacts{Effects: graph.EffectSet{graph.EffectMutate}, RequiredCapabilities: []string{"network"}, DryRunAvailable: false}}}); encodeErr != nil {
			t.Errorf("encode dry-run response: %v", encodeErr)
		}
	}))
	defer dryRunServer.Close()
	dryRunClient := workflowDaemonClient{baseURL: func() string { return dryRunServer.URL }, client: dryRunServer.Client()}
	preview, err := dryRunClient.ExplainWorkflow(t.Context(), appworkflow.ExplainWorkflowRequest{RunID: "preview", Definition: graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: "team/remote", Version: "v1"}, IdempotencyKey: "preview-key", Identity: appworkflow.IdentityRequest{SourceAuthority: "cli"}})
	if !errors.Is(err, appworkflow.ErrDryRunUnsupported) || preview.Facts.DryRunAvailable || len(preview.Diagnostics) != 1 || preview.Facts.Effects[0] != graph.EffectMutate {
		t.Fatalf("preserved preview=%#v error=%v", preview, err)
	}
	pinRejectServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusConflict)
		inputsRef := values.ValueSetRef{ID: "daemon-pin-rejected-inputs", Digest: values.SHA256Digest([]byte("daemon-pin-rejected-inputs"))}
		result := appworkflow.StartRunResult{Bound: &workflowruntime.BoundRun{ID: "daemon-pin-rejected", InputsRef: inputsRef}, Run: &workflowruntime.RunSnapshot{ID: "daemon-pin-rejected", Status: workflowruntime.RunCanceled, Inputs: &inputsRef}, Phase: hoststate.StartPinsRejected}
		if encodeErr := json.NewEncoder(writer).Encode(appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodePinRejected, Result: &result}); encodeErr != nil {
			t.Errorf("encode pin rejection response: %v", encodeErr)
		}
	}))
	defer pinRejectServer.Close()
	pinRejectClient := workflowDaemonClient{baseURL: func() string { return pinRejectServer.URL }, client: pinRejectServer.Client()}
	rejected, err := pinRejectClient.RunWorkflow(t.Context(), appworkflow.RunWorkflowRequest{RunID: "daemon-pin-rejected", Definition: graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: "team/remote", Version: "v1"}, IdempotencyKey: "daemon-pin-rejected-key", Identity: appworkflow.IdentityRequest{SourceAuthority: "cli"}})
	if err == nil || !rejected.RejectedBeforeAdmission() {
		t.Fatalf("preserved pin rejection=%#v error=%v", rejected, err)
	}
}

func TestWorkflowDaemonClientIsCompatibleWithGraphNativeAPIServer(t *testing.T) {
	service := completeWorkflowSpy()
	plan := workflowruntime.PlanRef{ID: "http-compatible", Version: "v1", Digest: values.SHA256Digest([]byte("http-compatible")), SchemaVersion: "1"}
	service.validate = func(ctx context.Context, request appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
		if ctx.Value(workflowHTTPCompatibilityKey{}) != "authenticated" || request.Identity.SourceAuthority != "http" || request.Identity.PrincipalHint != "" || request.Identity.Attributes != nil {
			t.Fatalf("context=%v identity=%#v", ctx.Value(workflowHTTPCompatibilityKey{}), request.Identity)
		}
		return appworkflow.ValidateWorkflowResult{Definition: request.Definition, Plan: &plan}, nil
	}
	server := httptest.NewServer(api.NewServer("", api.Dependencies{
		Workflows: service,
		WorkflowAuth: api.WorkflowRequestAuthenticatorFunc(func(request *http.Request, _ appworkflow.WorkflowAccessIntent) (context.Context, error) {
			return context.WithValue(request.Context(), workflowHTTPCompatibilityKey{}, "authenticated"), nil
		}),
	}).Handler())
	defer server.Close()
	client := workflowDaemonClient{baseURL: func() string { return server.URL }, client: server.Client()}
	result, err := client.ValidateWorkflow(t.Context(), appworkflow.ValidateWorkflowRequest{
		Definition: graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: "team/http", Version: "v1"},
		Identity:   appworkflow.IdentityRequest{PrincipalHint: "untrusted-cli-hint", SourceAuthority: "cli", Attributes: map[string]string{"exposure_ref": "forged"}},
	})
	if err != nil || result.Plan == nil || *result.Plan != plan {
		t.Fatalf("result=%#v error=%v", result, err)
	}
}

type workflowHTTPCompatibilityKey struct{}

func TestWorkflowCommandPropagatesOutputWriteFailures(t *testing.T) {
	writeErr := errors.New("output unavailable")
	service := completeWorkflowSpy()
	plan := workflowruntime.PlanRef{ID: "sample", Version: "v1", Digest: values.SHA256Digest([]byte("sample")), SchemaVersion: "1"}
	service.validate = func(_ context.Context, request appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
		return appworkflow.ValidateWorkflowResult{Definition: request.Definition, Plan: &plan}, nil
	}
	command := buildWorkflowCmdWithDependencies(testWorkflowDependencies(service))
	command.SetOut(workflowErrorWriter{err: writeErr})
	command.SetErr(bytes.NewBuffer(nil))
	command.SetArgs([]string{"validate", "team/name@v1"})
	if err := command.Execute(); !errors.Is(err, writeErr) {
		t.Fatalf("write error=%v", err)
	}
}

type workflowErrorWriter struct{ err error }

func (w workflowErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func completeWorkflowSpy() *workflowServiceSpy {
	notImplemented := errors.New("unexpected workflow service call")
	return &workflowServiceSpy{
		validate: func(context.Context, appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
			return appworkflow.ValidateWorkflowResult{}, notImplemented
		},
		explain: func(context.Context, appworkflow.ExplainWorkflowRequest) (appworkflow.StartRunResult, error) {
			return appworkflow.StartRunResult{}, notImplemented
		},
		run: func(context.Context, appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
			return appworkflow.StartRunResult{}, notImplemented
		},
		inspect: func(context.Context, appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error) {
			return rundiagnostics.Result{}, notImplemented
		},
		cancel: func(context.Context, appworkflow.CancelWorkflowRunRequest) (appworkflow.CancelWorkflowRunResult, error) {
			return appworkflow.CancelWorkflowRunResult{}, notImplemented
		},
		resume: func(context.Context, appworkflow.ResumeWorkflowRunRequest) (appworkflow.ResumeWorkflowRunResult, error) {
			return appworkflow.ResumeWorkflowRunResult{}, notImplemented
		},
		rerun: func(context.Context, appworkflow.RerunWorkflowRequest) (appworkflow.RerunWorkflowResult, error) {
			return appworkflow.RerunWorkflowResult{}, notImplemented
		},
	}
}

func executeWorkflowCommand(t *testing.T, service appworkflow.WorkflowOperations, arguments ...string) (string, error) {
	t.Helper()
	command := buildWorkflowCmdWithDependencies(testWorkflowDependencies(service))
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs(arguments)
	err := command.Execute()
	return output.String(), err
}

func testWorkflowDependencies(service appworkflow.WorkflowOperations) workflowCommandDependencies {
	return workflowCommandDependencies{service: service, now: func() time.Time { return time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) }, random: func(buffer []byte) error {
		for index := range buffer {
			buffer[index] = byte(index + 1)
		}
		return nil
	}}
}

func readWorkflowGolden(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "workflow", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
