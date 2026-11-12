package mcptools

import (
	gmcp "github.com/hollis-labs/libs/mcp/plugin-mcp/go-mcp/server"

	"github.com/hollis-labs/hadron/internal/runtime"
)

func obj(props map[string]any, required ...string) map[string]any {
	schema := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		req := make([]any, len(required))
		for i, r := range required {
			req[i] = r
		}
		schema["required"] = req
	}
	return schema
}

func str() map[string]any   { return map[string]any{"type": "string"} }
func intp() map[string]any  { return map[string]any{"type": "integer"} }
func boolp() map[string]any { return map[string]any{"type": "boolean"} }
func objp() map[string]any  { return map[string]any{"type": "object"} }

// ref is the exact definition reference every definition-addressed tool takes.
func ref(extra map[string]any, required ...string) map[string]any {
	props := map[string]any{"name": str(), "version": str(), "digest": str()}
	for k, v := range extra {
		props[k] = v
	}
	return obj(props, append([]string{"name", "version", "digest"}, required...)...)
}

func (s *surface) tools() []tool {
	run, manage := runtime.ScopeRun, runtime.ScopeManage
	read := gmcp.Reads("reads workflow state").Idempotent()
	write := gmcp.Writes().Idempotent()
	resumeProps := map[string]any{
		"run_id": str(), "wait_id": str(), "correlation": str(), "token": str(), "payload": objp(),
		"idempotency_key": str(),
		"wake_source":     map[string]any{"type": "string", "enum": []any{"callback", "gate", "message"}, "description": "what is waking the run; default callback"},
	}
	return []tool{
		{"hadron_workflow_validate", "Validate one exact workflow definition.", ref(nil), run, read, s.validate},
		{"hadron_workflow_run", "Start a run of one exact workflow definition. A run id and idempotency key are generated when omitted.", ref(map[string]any{
			"inputs": objp(), "run_id": str(), "idempotency_key": str(), "confirmed": boolp(),
		}), run, gmcp.Writes(), s.run},
		{"hadron_workflow_run_inspect", "Inspect a run: nodes, attempts, waits, values and events, bounded.", obj(map[string]any{"run_id": str(), "reveal_private": boolp()}, "run_id"), run, read, s.inspect},
		{"hadron_workflow_run_cancel", "Cancel a run.", obj(map[string]any{"run_id": str(), "idempotency_key": str(), "reason": str()}, "run_id", "idempotency_key"), run, gmcp.Destroys("a canceled run cannot be resumed").Idempotent(), s.cancel},
		{"hadron_workflow_run_events", "Read a run's events after a cursor; wait_ms (max 30000) holds the call until a newer event exists.", obj(map[string]any{
			"run_id": str(), "limit": intp(), "after": map[string]any{"type": "string", "description": "next_cursor from an earlier call for this run"},
			"wait_ms": intp(), "reveal_private": boolp(),
		}, "run_id"), run, read, s.events},
		{"hadron_workflow_run_resume", "Resume a waiting run with a typed payload (a callback, a gate decision or a message).", obj(resumeProps, "run_id", "wait_id", "correlation", "payload"), run, write, s.resume},
		{"hadron_workflow_signal", "Deliver a named signal to a run.", obj(map[string]any{
			"run_id": str(), "name": str(), "correlation": str(), "payload": objp(), "idempotency_key": str(), "confirmed": boolp(),
		}, "run_id", "name", "correlation", "payload", "idempotency_key"), run, write, s.signal},
		{"hadron_workflow_catalog_search", "Search one namespace of the workflow catalog: bounded ranked matches and the next authoring step.", obj(map[string]any{"query": str(), "namespace": str(), "limit": intp()}, "namespace"), run, read, s.catalogSearch},
		{"hadron_workflow_catalog_inspect", "Inspect one exact catalog version.", ref(nil), run, read, s.catalogInspect},
		{"hadron_workflow_author_validate", "Validate one bounded graph-native draft without mutating the catalog.", obj(map[string]any{"draft": workflowDraftProperty()}, "draft"), manage, read, s.authorValidate},
		{"hadron_workflow_author_scaffold", "Validate a draft and generate an editable deterministic contract-test scaffold.", obj(map[string]any{"draft": workflowDraftProperty()}, "draft"), manage, read, s.authorScaffold},
		{"hadron_workflow_author_test", "Validate a draft and run its deterministic contract suite without registering it.", obj(map[string]any{"draft": workflowDraftProperty(), "suite": workflowContractSuiteProperty()}, "draft", "suite"), manage, read, s.authorTest},
		{"hadron_workflow_author_register", "Validate and test a draft, then register its exact immutable version in an authorized namespace.", obj(map[string]any{
			"draft": workflowDraftProperty(), "suite": workflowContractSuiteProperty(),
			"make_current": map[string]any{"type": "boolean", "description": "also move the registry current alias to this exact version"},
		}, "draft", "suite"), manage, write, s.authorRegister},
		{"hadron_workflow_registry_package", "Package one exact version with its contract suite.", ref(map[string]any{"suite": workflowContractSuiteProperty()}, "suite"), manage, read, s.registryPackage},
		{"hadron_workflow_registry_publish", "Publish one exact version.", ref(nil), manage, write, s.registryMutation("publish")},
		{"hadron_workflow_registry_pin_version", "Pin one exact version.", ref(nil), manage, write, s.registryMutation("pin")},
		{"hadron_workflow_registry_unpin_version", "Unpin one exact version.", ref(nil), manage, write, s.registryMutation("unpin")},
		{"hadron_workflow_registry_clear_current", "Clear the current alias if it names this exact version.", ref(nil), manage, write, s.registryMutation("clear_current")},
		{"hadron_workflow_exposure_inspect", "Inspect an exposure profile.", obj(map[string]any{"profile_id": str()}, "profile_id"), manage, read, s.exposureInspect},
		{"hadron_workflow_exposure_pin_definition", "Pin one exact definition in an exposure profile.", ref(map[string]any{
			"profile_id": str(), "expected_generation": map[string]any{"type": "integer", "minimum": 1},
		}, "profile_id", "expected_generation"), manage, write, s.exposureMutation(false)},
		{"hadron_workflow_exposure_unpin_definition", "Unpin one exact definition from an exposure profile.", ref(map[string]any{
			"profile_id": str(), "expected_generation": map[string]any{"type": "integer", "minimum": 1},
		}, "profile_id", "expected_generation"), manage, write, s.exposureMutation(true)},
	}
}
