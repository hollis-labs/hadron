package workflowschema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/hadron/workflow/authoring"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	graphschema "github.com/hollis-labs/hadron/workflow/graph/schema"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	ID      = "https://schemas.hollis-labs.dev/hadron/workflow-api/v1/workflow-api.schema.json"
	Version = "1"
)

type operation struct {
	Name           string `json:"name"`
	Method         string `json:"method"`
	Path           string `json:"path"`
	PathField      string `json:"path_field,omitempty"`
	Request        string `json:"request"`
	Response       string `json:"response"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// Generate returns the deterministic transport schema derived from the Go
// application-service contracts. Graph types remain external references to the
// committed graph schema rather than a copied parallel model.
func Generate() ([]byte, error) {
	g := generator{definitions: make(map[string]any), building: make(map[reflect.Type]bool)}
	aliases := map[string]reflect.Type{
		"AgentAuthoringRequest":                reflect.TypeFor[appworkflow.AgentAuthoringRequest](),
		"AgentAuthoringResult":                 reflect.TypeFor[appworkflow.AgentAuthoringResult](),
		"AuthoringEnvelope":                    reflect.TypeFor[authoring.Envelope](),
		"CancelWorkflowRunRequest":             reflect.TypeFor[appworkflow.CancelWorkflowRunRequest](),
		"CancelWorkflowRunResult":              reflect.TypeFor[appworkflow.CancelWorkflowRunResult](),
		"ExplainWorkflowRequest":               reflect.TypeFor[appworkflow.ExplainWorkflowRequest](),
		"InspectWorkflowRunRequest":            reflect.TypeFor[appworkflow.InspectWorkflowRunRequest](),
		"RerunWorkflowRequest":                 reflect.TypeFor[appworkflow.RerunWorkflowRequest](),
		"ResumeWorkflowRunRequest":             reflect.TypeFor[appworkflow.ResumeWorkflowRunRequest](),
		"RunWorkflowRequest":                   reflect.TypeFor[appworkflow.RunWorkflowRequest](),
		"ValidateWorkflowRequest":              reflect.TypeFor[appworkflow.ValidateWorkflowRequest](),
		"WorkflowAttemptDiagnostic":            reflect.TypeFor[rundiagnostics.AttemptDiagnostic](),
		"WorkflowAttemptID":                    reflect.TypeFor[runtime.AttemptID](),
		"WorkflowControlDecisionDiagnostic":    reflect.TypeFor[rundiagnostics.ControlDecisionDiagnostic](),
		"WorkflowDefinitionRef":                reflect.TypeFor[graph.DefinitionRef](),
		"WorkflowDiagnosticFinding":            reflect.TypeFor[diagnostic.Diagnostic](),
		"WorkflowEdgeValueFlowDiagnostic":      reflect.TypeFor[rundiagnostics.EdgeValueFlowDiagnostic](),
		"WorkflowFailureDiagnostic":            reflect.TypeFor[rundiagnostics.FailureDiagnostic](),
		"WorkflowGraph":                        reflect.TypeFor[graph.Graph](),
		"WorkflowGraphDiagnostic":              reflect.TypeFor[rundiagnostics.Result](),
		"WorkflowInvocationValueDiagnostic":    reflect.TypeFor[rundiagnostics.InvocationValueDiagnostic](),
		"WorkflowNodeDiagnostic":               reflect.TypeFor[rundiagnostics.NodeDiagnostic](),
		"WorkflowNodeInvocationID":             reflect.TypeFor[runtime.NodeInvocationID](),
		"WorkflowNodeResourceDiagnostic":       reflect.TypeFor[rundiagnostics.NodeResourceDiagnostic](),
		"WorkflowPlanDiagnostic":               reflect.TypeFor[rundiagnostics.PlanDiagnostic](),
		"WorkflowPlanEdgeDiagnostic":           reflect.TypeFor[rundiagnostics.PlanEdgeDiagnostic](),
		"WorkflowPlanNodeDiagnostic":           reflect.TypeFor[rundiagnostics.PlanNodeDiagnostic](),
		"WorkflowPlanRef":                      reflect.TypeFor[runtime.PlanRef](),
		"WorkflowPositionDiagnostic":           reflect.TypeFor[rundiagnostics.PositionDiagnostic](),
		"WorkflowRenderedEvent":                reflect.TypeFor[runtime.RenderedEvent](),
		"WorkflowRenderedValue":                reflect.TypeFor[values.RenderedValue](),
		"WorkflowRerunResult":                  reflect.TypeFor[appworkflow.RerunWorkflowResult](),
		"WorkflowResumeRequest":                reflect.TypeFor[appworkflow.ResumeWorkflowRunRequest](),
		"WorkflowResumeResult":                 reflect.TypeFor[appworkflow.ResumeWorkflowRunResult](),
		"WorkflowRetryDiagnostic":              reflect.TypeFor[rundiagnostics.RetryDiagnostic](),
		"WorkflowSchedulerResourceHolder":      reflect.TypeFor[runtime.SchedulerResourceHolder](),
		"WorkflowSchedulerResourceID":          reflect.TypeFor[runtime.SchedulerResourceID](),
		"WorkflowSchedulerResourceRequirement": reflect.TypeFor[runtime.SchedulerResourceRequirement](),
		"WorkflowSchedulerResourceWaiter":      reflect.TypeFor[runtime.SchedulerResourceWaiter](),
		"WorkflowSourceDiagnostic":             reflect.TypeFor[rundiagnostics.SourceDiagnostic](),
		"WorkflowStartPolicyDiagnostic":        reflect.TypeFor[rundiagnostics.StartPolicyDiagnostic](),
		"WorkflowStartResult":                  reflect.TypeFor[appworkflow.StartRunResult](),
		"WorkflowValidateResult":               reflect.TypeFor[appworkflow.ValidateWorkflowResult](),
		"WorkflowValueSetDiagnostic":           reflect.TypeFor[rundiagnostics.ValueSetDiagnostic](),
		"WorkflowValueSetRef":                  reflect.TypeFor[values.ValueSetRef](),
		"WorkflowWaitDiagnostic":               reflect.TypeFor[rundiagnostics.WaitDiagnostic](),
	}
	aliasNames := make([]string, 0, len(aliases))
	for name := range aliases {
		aliasNames = append(aliasNames, name)
	}
	sort.Strings(aliasNames)
	for _, name := range aliasNames {
		schema := g.schemaFor(aliases[name])
		if ref, ok := schema["$ref"].(string); ok && ref == "#/$defs/"+name {
			// schemaFor has already materialized the named definition. Replacing it
			// with its own reference would emit a recursive TypeScript alias.
			continue
		}
		g.definitions[name] = schema
	}

	operations := []operation{
		{Name: "validateWorkflow", Method: "POST", Path: "/v1/workflows/validate", Request: "ValidateWorkflowRequest", Response: "WorkflowValidateResult"},
		{Name: "explainWorkflow", Method: "POST", Path: "/v1/workflows/explain", Request: "ExplainWorkflowRequest", Response: "WorkflowStartResult", IdempotencyKey: "idempotency_key"},
		{Name: "runWorkflow", Method: "POST", Path: "/v1/workflows/runs", Request: "RunWorkflowRequest", Response: "WorkflowStartResult", IdempotencyKey: "idempotency_key"},
		{Name: "inspectWorkflowRun", Method: "POST", Path: "/v1/workflows/runs/{run_id}/inspect", PathField: "run_id", Request: "InspectWorkflowRunRequest", Response: "WorkflowGraphDiagnostic"},
		{Name: "cancelWorkflowRun", Method: "POST", Path: "/v1/workflows/runs/{run_id}/cancel", PathField: "run_id", Request: "CancelWorkflowRunRequest", Response: "CancelWorkflowRunResult", IdempotencyKey: "idempotency_key"},
		{Name: "resumeWorkflowRun", Method: "POST", Path: "/v1/workflows/runs/{run_id}/resume", PathField: "run_id", Request: "ResumeWorkflowRunRequest", Response: "WorkflowResumeResult", IdempotencyKey: "idempotency_key"},
		{Name: "rerunWorkflow", Method: "POST", Path: "/v1/workflows/runs/{source_run_id}/rerun", PathField: "source_run_id", Request: "RerunWorkflowRequest", Response: "WorkflowRerunResult", IdempotencyKey: "idempotency_key"},
	}
	document := map[string]any{
		"$schema": "https://json-schema.org/draft/2020-12/schema", "$id": ID,
		"title": "Hadron Workflow HTTP API", "description": "Generated from transport-neutral Go workflow contracts.",
		"type": "object", "additionalProperties": false, "$defs": g.definitions,
		"x-hadron-schema-version":         Version,
		"x-hadron-graph-schema":           map[string]any{"id": graphschema.ID, "version": graphschema.Version},
		"x-hadron-authoring-schema":       map[string]any{"id": authoring.EnvelopeSchemaID, "version": authoring.EnvelopeSchemaVersion},
		"x-hadron-workflow-source-schema": map[string]any{"id": authoring.WorkflowSourceSchemaID, "version": authoring.WorkflowSourceSchemaVersion},
		"x-hadron-agent-result-schema":    map[string]any{"id": appworkflow.AgentAuthoringResultSchemaID, "version": appworkflow.AgentAuthoringResultSchemaVersion},
		"x-hadron-operations":             operations,
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal workflow API schema: %w", err)
	}
	return append(encoded, '\n'), nil
}

type generator struct {
	definitions map[string]any
	building    map[reflect.Type]bool
}

func (g *generator) schemaFor(input reflect.Type) map[string]any {
	for input.Kind() == reflect.Pointer {
		input = input.Elem()
	}
	if input == reflect.TypeFor[authoring.MaterialFormat]() {
		return map[string]any{"type": "string", "enum": []string{string(authoring.FormatGraphIR), string(authoring.FormatWorkflowSource)}}
	}
	if input == reflect.TypeFor[json.RawMessage]() {
		return map[string]any{"$ref": "#/$defs/AuthoringEnvelope"}
	}
	if input == reflect.TypeFor[values.Value]() {
		return g.valueSchema()
	}
	if input == reflect.TypeFor[time.Time]() {
		return map[string]any{"type": "string", "format": "date-time"}
	}
	if input.PkgPath() == "" {
		return g.inline(input)
	}
	if input.Name() != "" {
		if input.PkgPath() == reflect.TypeFor[graph.Graph]().PkgPath() {
			return map[string]any{"$ref": graphschema.ID + "#/$defs/" + input.Name()}
		}
		name := definitionName(input)
		g.ensure(input, name)
		return map[string]any{"$ref": "#/$defs/" + name}
	}
	return g.inline(input)
}

func (g *generator) valueSchema() map[string]any {
	common := map[string]any{
		"type":       g.schemaFor(reflect.TypeFor[values.Type]()),
		"producer":   g.schemaFor(reflect.TypeFor[values.Producer]()),
		"media_type": map[string]any{"type": "string"},
		"digest":     map[string]any{"type": "string"},
		"redaction":  g.schemaFor(reflect.TypeFor[values.RedactionClass]()),
		"retention":  g.schemaFor(reflect.TypeFor[values.RetentionClass]()),
	}
	variant := func(name string, value map[string]any) map[string]any {
		properties := make(map[string]any, len(common)+1)
		for key, schema := range common {
			properties[key] = schema
		}
		properties[name] = value
		return map[string]any{
			"type": "object", "additionalProperties": false, "properties": properties,
			"required": []string{"digest", "media_type", name, "producer", "redaction", "retention", "type"},
		}
	}
	return map[string]any{"oneOf": []any{
		variant("inline", map[string]any{}),
		variant("artifact", g.schemaFor(reflect.TypeFor[values.ArtifactRef]())),
		variant("secret_ref", g.schemaFor(reflect.TypeFor[values.SecretRef]())),
	}}
}

func (g *generator) ensure(input reflect.Type, name string) {
	if _, ok := g.definitions[name]; ok || g.building[input] {
		return
	}
	g.building[input] = true
	g.definitions[name] = g.inline(input)
	delete(g.building, input)
}

func (g *generator) inline(input reflect.Type) map[string]any {
	switch input.Kind() {
	case reflect.Interface:
		return map[string]any{}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Slice, reflect.Array:
		if input.Elem().Kind() == reflect.Uint8 {
			return map[string]any{"type": "string", "contentEncoding": "base64"}
		}
		return map[string]any{"type": "array", "items": g.schemaFor(input.Elem())}
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": g.schemaFor(input.Elem())}
	case reflect.Struct:
		properties := make(map[string]any)
		var required []string
		for i := 0; i < input.NumField(); i++ {
			field := input.Field(i)
			if field.PkgPath != "" {
				continue
			}
			name, omit, skip := jsonField(field)
			if skip {
				continue
			}
			properties[name] = g.schemaFor(field.Type)
			if !omit {
				required = append(required, name)
			}
		}
		sort.Strings(required)
		result := map[string]any{"type": "object", "additionalProperties": false, "properties": properties}
		if len(required) > 0 {
			result["required"] = required
		}
		return result
	default:
		return map[string]any{}
	}
}

func definitionName(input reflect.Type) string {
	path := input.PkgPath()
	segment := path
	if at := strings.LastIndex(path, "/"); at >= 0 {
		segment = path[at+1:]
	}
	return exportedName(segment) + input.Name()
}

func exportedName(input string) string {
	var result strings.Builder
	upper := true
	for _, current := range input {
		if !unicode.IsLetter(current) && !unicode.IsDigit(current) {
			upper = true
			continue
		}
		if upper {
			current = unicode.ToUpper(current)
			upper = false
		}
		result.WriteRune(current)
	}
	return result.String()
}

func jsonField(field reflect.StructField) (string, bool, bool) {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", false, true
	}
	parts := strings.Split(tag, ",")
	name := parts[0]
	if name == "" {
		name = field.Name
	}
	omit := false
	for _, option := range parts[1:] {
		if option == "omitempty" {
			omit = true
		}
	}
	return name, omit, false
}
