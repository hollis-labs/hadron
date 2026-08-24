// Package planschema generates the committed ExecutionPlan JSON Schema from
// compile.ExecutionPlan while referencing graph types from the graph schema.
package planschema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
)

const (
	compilePackagePath = "github.com/hollis-labs/hadron/workflow/compile"
	graphPackagePath   = "github.com/hollis-labs/hadron/workflow/graph"
	graphSchemaID      = "https://schemas.hollis-labs.dev/workflow/graph/v1/workflow.schema.json"
	planSchemaID       = "https://schemas.hollis-labs.dev/workflow/plan/v1/execution-plan.schema.json"
)

// Generate returns the deterministic JSON Schema document for ExecutionPlan.
func Generate() ([]byte, error) {
	generator := schemaGenerator{
		definitions: make(map[string]any),
		building:    make(map[reflect.Type]bool),
	}
	root := generator.schemaFor(reflect.TypeFor[workflowcompile.ExecutionPlan]())
	document := map[string]any{
		"$schema":     "https://json-schema.org/draft/2020-12/schema",
		"$id":         planSchemaID,
		"title":       "Workflow Execution Plan",
		"description": "Immutable workflow execution-plan envelope generated from the Go compile types.",
		"$ref":        root["$ref"],
		"x-workflow-boundary": map[string]any{
			"component":   "execution-plan",
			"graphSchema": graphSchemaID,
			"version":     workflowcompile.ExecutionPlanSchemaVersion,
		},
		"$defs": generator.definitions,
	}
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal workflow execution-plan schema: %w", err)
	}
	return append(data, '\n'), nil
}

type schemaGenerator struct {
	definitions map[string]any
	building    map[reflect.Type]bool
}

func (g *schemaGenerator) schemaFor(typ reflect.Type) map[string]any {
	if typ.Kind() == reflect.Pointer {
		return g.schemaFor(typ.Elem())
	}
	if typ.Name() != "" {
		switch typ.PkgPath() {
		case graphPackagePath:
			return map[string]any{"$ref": graphSchemaID + "#/$defs/" + typ.Name()}
		case compilePackagePath:
			g.ensureDefinition(typ)
			return map[string]any{"$ref": "#/$defs/" + typ.Name()}
		}
	}
	return g.inlineSchema(typ)
}

func (g *schemaGenerator) ensureDefinition(typ reflect.Type) {
	if _, exists := g.definitions[typ.Name()]; exists || g.building[typ] {
		return
	}
	g.building[typ] = true
	g.definitions[typ.Name()] = g.inlineSchema(typ)
	delete(g.building, typ)
}

func (g *schemaGenerator) inlineSchema(typ reflect.Type) map[string]any {
	switch typ.Kind() {
	case reflect.Interface:
		return map[string]any{}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": g.schemaFor(typ.Elem())}
	case reflect.Map:
		if typ.Key().Kind() != reflect.String {
			panic(fmt.Sprintf("unsupported execution-plan map key type %s", typ.Key()))
		}
		return map[string]any{"type": "object", "additionalProperties": g.schemaFor(typ.Elem())}
	case reflect.Struct:
		return g.structSchema(typ)
	default:
		panic(fmt.Sprintf("unsupported execution-plan schema type %s", typ))
	}
}

func (g *schemaGenerator) structSchema(typ reflect.Type) map[string]any {
	properties := make(map[string]any, typ.NumField())
	var required []string
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name, omitEmpty, skip := jsonField(field)
		if skip {
			continue
		}
		properties[name] = g.schemaFor(field.Type)
		if !omitEmpty {
			required = append(required, name)
		}
	}
	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) != 0 {
		schema["required"] = required
	}
	return schema
}

func jsonField(field reflect.StructField) (name string, omitEmpty bool, skip bool) {
	parts := strings.Split(field.Tag.Get("json"), ",")
	if parts[0] == "-" {
		return "", false, true
	}
	name = parts[0]
	if name == "" {
		name = field.Name
	}
	for _, option := range parts[1:] {
		if option == "omitempty" {
			omitEmpty = true
		}
	}
	return name, omitEmpty, false
}
