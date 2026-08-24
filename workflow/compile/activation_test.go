package compile_test

import (
	"bytes"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
)

const activationLocator = "testdata/activations.workflow.yaml"

func TestCompileActivationDeclarationsAndSnapshots(t *testing.T) {
	plan := activationPlan(t, activationLocator)
	if len(plan.Graph.Activations) != 6 {
		t.Fatalf("activations = %#v", plan.Graph.Activations)
	}
	wantOrder := []string{"agent-message", "daily-schedule", "inbox-files", "incoming-hook", "project-event", "setup-callback"}
	for index, want := range wantOrder {
		if got := plan.Graph.Activations[index].ID; got != want {
			t.Errorf("activations[%d].ID = %q, want %q", index, got, want)
		}
	}

	webhook := activationByKind(t, plan, "webhook")
	if webhook.Config["path"] != "/hooks/tasks" || webhook.Inputs["tasks"].Expression.Text != "body.tasks" ||
		webhook.Inputs["request_id"].Interpolation != "request-{{ message.id }}" {
		t.Fatalf("webhook = %#v", webhook)
	}
	if webhook.Policy.Overlap != graph.OverlapForbid || webhook.Policy.StartingDeadline != "30s" || webhook.Policy.Catchup ||
		webhook.Policy.DeduplicationKey == nil || webhook.Policy.DeduplicationKey.Text != "inputs.project_id" ||
		webhook.Policy.RunIDReuse != graph.RunIDReuseTerminateExisting {
		t.Fatalf("webhook policy = %#v", webhook.Policy)
	}
	if webhook.Metadata["purpose"] != "ingestion" {
		t.Fatalf("webhook metadata = %#v", webhook.Metadata)
	}

	schedule := activationByKind(t, plan, "schedule")
	if schedule.Config["cron"] != "0 6 * * *" || schedule.Policy.Overlap != graph.OverlapAllow || !schedule.Policy.Catchup {
		t.Fatalf("schedule = %#v", schedule)
	}
	message := activationByKind(t, plan, "message")
	if message.Config["to"] != "msg://agent/hadron/bulk-create" || message.Inputs["project_id"].Expression.Text != "message.project_id" {
		t.Fatalf("message = %#v", message)
	}
	file := activationByKind(t, plan, "file")
	if file.Config["path"] != "inbox/tasks.json" || !reflect.DeepEqual(file.Config["events"], []string{"create", "write", "rename", "remove"}) {
		t.Fatalf("file = %#v", file)
	}
	event := activationByKind(t, plan, "event")
	if event.Config["type"] != "project.task.created" || event.Config["source"] != "project://torque" {
		t.Fatalf("event = %#v", event)
	}
	oneShot := activationByKind(t, plan, "one_shot")
	if oneShot.Config["path"] != "/callbacks/setup" || oneShot.Config["ttl"] != "15m" {
		t.Fatalf("one_shot = %#v", oneShot)
	}

	for _, activation := range plan.Graph.Activations {
		if activation.Provenance.Authority != "project" || activation.Provenance.Origin != "workflow-source" ||
			activation.Provenance.Locator != activationLocator || activation.Provenance.Revision != "source-rev-42" ||
			activation.Provenance.Digest != plan.SourceDigests[0].Digest {
			t.Errorf("%s provenance = %#v", activation.ID, activation.Provenance)
		}
		mapped, exists := plan.SourceMap.Activations[activation.ID]
		if !exists || activation.Source == nil || !reflect.DeepEqual(mapped, *activation.Source) {
			t.Errorf("%s source map = %#v, source = %#v", activation.ID, mapped, activation.Source)
		}
	}
	if err := plan.Graph.ValidateEnums(); err != nil {
		t.Fatalf("ValidateEnums() = %v", err)
	}

	encoded := stableJSON(t, plan)
	for _, forbidden := range []string{`"registration_id"`, `"enabled"`, `"expires_at"`, `"fire_history"`, `"callback_token"`, `"host_owner"`} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Errorf("immutable plan contains operational field %s", forbidden)
		}
	}
	assertSnapshot(t, "activations.plan.json", encoded)
	assertSnapshot(t, "activations.source-map.json", stableJSON(t, plan.SourceMap))
}

func TestCompileActivationSourceLocationsAreExact(t *testing.T) {
	plan := activationPlan(t, activationLocator)
	webhook := activationByKind(t, plan, "webhook")
	schedule := activationByKind(t, plan, "schedule")
	message := activationByKind(t, plan, "message")
	tests := []struct {
		name string
		ref  *graph.SourceRef
		path []string
		line int
		col  int
	}{
		{name: "webhook declaration", ref: webhook.Source, path: []string{"on", "webhook"}, line: 9, col: 5},
		{name: "webhook extraction binding", ref: webhook.Inputs["tasks"].Source, path: []string{"on", "webhook", "extract", "tasks"}, line: 13, col: 14},
		{name: "webhook extraction expression", ref: webhook.Inputs["tasks"].Expression.Source, path: []string{"on", "webhook", "extract", "tasks"}, line: 13, col: 14},
		{name: "dedup expression", ref: webhook.Policy.DeduplicationKey.Source, path: []string{"on", "webhook", "deduplication_key"}, line: 19, col: 24},
		{name: "schedule declaration", ref: schedule.Source, path: []string{"on", "schedule"}, line: 24, col: 5},
		{name: "message declaration", ref: message.Source, path: []string{"on", "message"}, line: 29, col: 5},
		{name: "file declaration", ref: activationByKind(t, plan, "file").Source, path: []string{"on", "file"}, line: 34, col: 5},
		{name: "event declaration", ref: activationByKind(t, plan, "event").Source, path: []string{"on", "event"}, line: 38, col: 5},
		{name: "one-shot declaration", ref: activationByKind(t, plan, "one_shot").Source, path: []string{"on", "one_shot"}, line: 42, col: 5},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.ref == nil || test.ref.Locator != activationLocator || !slices.Equal(test.ref.Path, test.path) ||
				test.ref.StartLine != test.line || test.ref.StartColumn != test.col {
				t.Fatalf("source = %#v, want path=%v line=%d column=%d", test.ref, test.path, test.line, test.col)
			}
		})
	}
}

func TestActivationDigestsAreDeterministicRelocatableAndMapOrderStable(t *testing.T) {
	data, err := os.ReadFile(activationLocator)
	if err != nil {
		t.Fatal(err)
	}
	first := compileBytes(t, "/first/activations.workflow.yaml", data)
	second := compileBytes(t, "/second/activations.workflow.yaml", data)
	repeated := compileBytes(t, "/first/activations.workflow.yaml", data)
	if first.Digest != second.Digest || first.Graph.Digest != second.Graph.Digest || first.SourceDigests[0] != second.SourceDigests[0] {
		t.Fatalf("relocation changed activation digests: %#v / %#v", first, second)
	}
	if !reflect.DeepEqual(first, repeated) {
		t.Fatal("repeated activation compile was not deterministic")
	}
	if activationByKind(t, first, "webhook").Provenance.Locator == activationByKind(t, second, "webhook").Provenance.Locator {
		t.Fatal("activation provenance lost relocation-specific locator")
	}
	commented := compileBytes(t, "/first/activations.workflow.yaml", append(append([]byte(nil), data...), []byte("\n# source-only revision\n")...))
	if commented.Graph.Digest != first.Graph.Digest || commented.Digest == first.Digest || commented.SourceDigests[0].Digest == first.SourceDigests[0].Digest {
		t.Fatalf("source-only activation change produced incorrect digest behavior: %#v / %#v", first, commented)
	}
	changed := compileBytes(t, "/first/activations.workflow.yaml", bytes.Replace(data, []byte("/hooks/tasks"), []byte("/hooks/tasks-v2"), 1))
	if changed.Graph.Digest == first.Graph.Digest || changed.Digest == first.Digest || changed.SourceDigests[0].Digest == first.SourceDigests[0].Digest {
		t.Fatal("semantic activation change did not alter graph, plan, and source digests")
	}

	const firstOrder = `workflow:
  name: Map Order
on:
  webhook:
    path: /map-order
    extract:
      alpha: body.alpha
      beta: body.beta
  schedule: "0 6 * * *"
steps:
  - name: Done
    transform: {expression: inputs.value}
`
	const secondOrder = `workflow:
  name: Map Order
on:
  schedule: "0 6 * * *"
  webhook:
    extract:
      beta: body.beta
      alpha: body.alpha
    path: /map-order
steps:
  - name: Done
    transform: {expression: inputs.value}
`
	ordered := compileBytes(t, "first.workflow.yaml", []byte(firstOrder))
	reordered := compileBytes(t, "second.workflow.yaml", []byte(secondOrder))
	if ordered.Graph.Digest != reordered.Graph.Digest {
		t.Fatalf("YAML map order changed semantic graph digest: %q / %q", ordered.Graph.Digest, reordered.Graph.Digest)
	}
	if ordered.SourceDigests[0].Digest == reordered.SourceDigests[0].Digest || ordered.Digest == reordered.Digest {
		t.Fatal("source/plan digests failed to retain distinct immutable source bytes")
	}
}

func TestActivationScheduleScalarAndMultipleDeclarations(t *testing.T) {
	const source = `workflow:
  name: Multiple Activations
on:
  schedule: "*/15 0-23/2 1,15 * 1-5"
  webhook:
    - name: First Hook
      path: /hooks/first
    - name: Second Hook
      path: /hooks/second
steps:
  - name: Done
    transform: {expression: inputs.value}
`
	plan := compileBytes(t, "multiple.workflow.yaml", []byte(source))
	if len(plan.Graph.Activations) != 3 || activationByKind(t, plan, "schedule").Config["cron"] != "*/15 0-23/2 1,15 * 1-5" {
		t.Fatalf("activations = %#v", plan.Graph.Activations)
	}
}

func TestFileActivationRejectsURIPathsAndAcceptsFilesystemPaths(t *testing.T) {
	const sourceTemplate = `workflow:
  name: File Path
on:
  file:
    path: $PATH
    events: [create]
steps:
  - name: Done
    transform: {expression: inputs.value}
`
	for _, invalid := range []string{"https://host/file", "file:///var/lib/tasks.json"} {
		t.Run("reject_"+strings.ReplaceAll(invalid, "/", "_"), func(t *testing.T) {
			loaded := workflowcompile.LoadBytes("file-path.workflow.yaml", []byte(strings.Replace(sourceTemplate, "$PATH", invalid, 1)))
			assertLoaded(t, loaded, "file-path.workflow.yaml")
			result := workflowcompile.Compile(loaded.Source)
			if result.Plan != nil || len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != workflowcompile.CodeInvalidActivation ||
				result.Diagnostics[0].Source == nil || !slices.Equal(result.Diagnostics[0].Source.Path, []string{"on", "file", "path"}) {
				t.Fatalf("Compile() = %#v", result)
			}
		})
	}
	for _, valid := range []string{"inbox/tasks.json", "/var/lib/tasks.json"} {
		t.Run("accept_"+strings.ReplaceAll(valid, "/", "_"), func(t *testing.T) {
			plan := compileBytes(t, "file-path.workflow.yaml", []byte(strings.Replace(sourceTemplate, "$PATH", valid, 1)))
			if got := activationByKind(t, plan, "file").Config["path"]; got != valid {
				t.Fatalf("file path = %#v, want %q", got, valid)
			}
		})
	}
}

func TestActivationRunIDReuseUsesGraphEnumVocabulary(t *testing.T) {
	const sourceTemplate = `workflow:
  name: Run ID Reuse
on:
  schedule:
    cron: "0 6 * * *"
    run_id_reuse: $POLICY
steps:
  - name: Done
    transform: {expression: inputs.value}
`
	tests := []struct {
		source string
		want   graph.RunIDReusePolicy
	}{
		{source: "reject", want: graph.RunIDReuseReject},
		{source: "allow_duplicate", want: graph.RunIDReuseAllowDuplicate},
		{source: "terminate_existing", want: graph.RunIDReuseTerminateExisting},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			plan := compileBytes(t, "run-id-reuse.workflow.yaml", []byte(strings.Replace(sourceTemplate, "$POLICY", test.source, 1)))
			if got := activationByKind(t, plan, "schedule").Policy.RunIDReuse; got != test.want {
				t.Fatalf("run_id_reuse = %q, want %q", got, test.want)
			}
		})
	}
}

func TestActivationDiagnosticsAreStructuredDeterministicAndFailClosed(t *testing.T) {
	tests := []struct {
		name      string
		file      string
		wantCode  diagnostic.Code
		wantCount int
	}{
		{name: "duplicate", file: "testdata/activation-duplicate.workflow.yaml", wantCode: workflowcompile.CodeDuplicateActivationID, wantCount: 1},
		{name: "malformed", file: "testdata/activation-malformed.workflow.yaml", wantCode: workflowcompile.CodeInvalidActivation, wantCount: 13},
		{name: "authority", file: "testdata/activation-authority.workflow.yaml", wantCode: workflowcompile.CodeUnsupportedActivationAuthority, wantCount: 1},
		{name: "operational", file: "testdata/activation-operational.workflow.yaml", wantCode: workflowcompile.CodeUnsupportedSourceField, wantCount: 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			loaded, err := workflowcompile.LoadFile(test.file)
			if err != nil {
				t.Fatal(err)
			}
			assertLoaded(t, loaded, test.file)
			first := workflowcompile.Compile(loaded.Source)
			second := workflowcompile.Compile(loaded.Source)
			if first.Plan != nil || !reflect.DeepEqual(first.Diagnostics, second.Diagnostics) {
				t.Fatalf("Compile() is not deterministic/fail-closed: %#v / %#v", first, second)
			}
			if len(first.Diagnostics) != test.wantCount {
				t.Fatalf("diagnostic count = %d, want %d: %#v", len(first.Diagnostics), test.wantCount, first.Diagnostics)
			}
			for index, finding := range first.Diagnostics {
				if finding.Code != test.wantCode {
					t.Errorf("diagnostic[%d].Code = %q, want %q", index, finding.Code, test.wantCode)
				}
				if finding.Source == nil || len(finding.Source.Path) < 2 || finding.Remediation == nil {
					t.Errorf("diagnostic[%d] is not actionable/source-mapped: %#v", index, finding)
				}
				if err := finding.Validate(); err != nil {
					t.Errorf("diagnostic[%d].Validate() = %v", index, err)
				}
			}
			if test.name == "duplicate" && (len(first.Diagnostics[0].Related) != 1 || first.Diagnostics[0].Related[0].Source.StartLine != 5) {
				t.Fatalf("duplicate related source = %#v", first.Diagnostics[0].Related)
			}
		})
	}
}

func TestActivationRequiredFieldsAndClosedPolicies(t *testing.T) {
	const source = `workflow:
  name: Missing Fields
on:
  webhook: {}
  schedule: {cron: "@daily"}
  message: {}
  file: {path: inbox}
  event: {}
  one_shot: {path: /once}
steps:
  - name: Done
    transform: {expression: inputs.value}
`
	loaded := workflowcompile.LoadBytes("missing.workflow.yaml", []byte(source))
	assertLoaded(t, loaded, "missing.workflow.yaml")
	result := workflowcompile.Compile(loaded.Source)
	if result.Plan != nil || len(result.Diagnostics) != 6 {
		t.Fatalf("Compile() = %#v", result)
	}
	for _, finding := range result.Diagnostics {
		if finding.Code != workflowcompile.CodeInvalidActivation {
			t.Fatalf("diagnostic = %#v", finding)
		}
	}
}

func TestActivationRejectsPrivilegedAuthorityTokens(t *testing.T) {
	for _, authority := range []string{"host", "operator", "hadron", "registry"} {
		t.Run(authority, func(t *testing.T) {
			source := `workflow:
  name: Privileged Authority
on:
  webhook:
    authority: ` + authority + `
    path: /hooks/privileged
steps:
  - name: Done
    transform: {expression: inputs.value}
`
			loaded := workflowcompile.LoadBytes("authority.workflow.yaml", []byte(source))
			assertLoaded(t, loaded, "authority.workflow.yaml")
			result := workflowcompile.Compile(loaded.Source)
			if result.Plan != nil || len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != workflowcompile.CodeUnsupportedActivationAuthority ||
				result.Diagnostics[0].Source == nil || !slices.Equal(result.Diagnostics[0].Source.Path, []string{"on", "webhook", "authority"}) {
				t.Fatalf("Compile() = %#v", result)
			}
		})
	}
}

func TestActivationExpressionsCannotReferenceSteps(t *testing.T) {
	const source = `workflow:
  name: Invalid Activation Values
on:
  webhook:
    path: /hooks/tasks
    extract:
      result: steps.producer.outputs.value
    deduplication_key: steps.producer.outputs.id
steps:
  - name: Producer
    transform: {expression: inputs.value}
`
	plan := compileBytes(t, "activation-values.workflow.yaml", []byte(source))
	result := workflowcompile.InferValueDependencies(&plan, workflowcompile.DependencyOptions{})
	if result.Plan != nil || len(result.Diagnostics) != 2 {
		t.Fatalf("InferValueDependencies() = %#v", result)
	}
	for _, finding := range result.Diagnostics {
		if finding.Code != workflowcompile.CodeUnavailableValueReference || finding.Source == nil ||
			!strings.HasPrefix(strings.Join(finding.Source.Path, "."), "on.webhook") {
			t.Errorf("activation dependency diagnostic = %#v", finding)
		}
	}
}

func activationPlan(t *testing.T, locator string) workflowcompile.ExecutionPlan {
	t.Helper()
	loaded, err := workflowcompile.LoadFile(locator)
	if err != nil {
		t.Fatal(err)
	}
	assertLoaded(t, loaded, locator)
	result := workflowcompile.Compile(loaded.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile() = %#v", result)
	}
	return *result.Plan
}

func activationByKind(t *testing.T, plan workflowcompile.ExecutionPlan, kind string) graph.ActivationDeclaration {
	t.Helper()
	for _, activation := range plan.Graph.Activations {
		if activation.Kind == kind {
			return activation
		}
	}
	t.Fatalf("activation kind %q not found in %#v", kind, plan.Graph.Activations)
	return graph.ActivationDeclaration{}
}
