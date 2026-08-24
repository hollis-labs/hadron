// Package schemagen generates the committed workflow graph JSON Schema from
// the graph package's Go types and enum declarations.
package schemagen

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/workflow/graph"
)

const (
	graphPackagePath = "github.com/hollis-labs/hadron/workflow/graph"
	schemaID         = "https://schemas.hollis-labs.dev/workflow/graph/v1/workflow.schema.json"
)

// Generate returns the deterministic JSON Schema document for graph.Graph.
func Generate() ([]byte, error) {
	enums, err := graphEnumValues()
	if err != nil {
		return nil, err
	}

	generator := schemaGenerator{
		definitions: make(map[string]any),
		building:    make(map[reflect.Type]bool),
		enums:       enums,
	}
	root := generator.schemaFor(reflect.TypeFor[graph.Graph]())

	document := map[string]any{
		"$schema":     "https://json-schema.org/draft/2020-12/schema",
		"$id":         schemaID,
		"title":       "Workflow Graph IR",
		"description": "Canonical graph-native workflow IR generated from the Go graph types.",
		"$ref":        root["$ref"],
		"x-workflow-boundaries": map[string]any{
			"sourceAuthoring": map[string]any{
				"format":         "workflow",
				"preferredFiles": []string{"*.workflow.yaml", "workflow.yaml"},
				"schema":         "#/$defs/Graph",
			},
			"serializedExecutionPlan": map[string]any{
				"component": "graph",
				"schema":    "#/$defs/Graph",
			},
		},
		"$defs": generator.definitions,
	}

	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal workflow graph schema: %w", err)
	}
	return append(data, '\n'), nil
}

type schemaGenerator struct {
	definitions map[string]any
	building    map[reflect.Type]bool
	enums       map[string][]string
}

func (g *schemaGenerator) schemaFor(typ reflect.Type) map[string]any {
	if typ.Kind() == reflect.Pointer {
		return g.schemaFor(typ.Elem())
	}
	if typ.Name() != "" && typ.PkgPath() == graphPackagePath {
		g.ensureDefinition(typ)
		return map[string]any{"$ref": "#/$defs/" + typ.Name()}
	}
	return g.inlineSchema(typ)
}

func (g *schemaGenerator) ensureDefinition(typ reflect.Type) {
	name := typ.Name()
	if _, exists := g.definitions[name]; exists || g.building[typ] {
		return
	}

	g.building[typ] = true
	g.definitions[name] = g.inlineSchema(typ)
	delete(g.building, typ)
}

func (g *schemaGenerator) inlineSchema(typ reflect.Type) map[string]any {
	switch typ.Kind() {
	case reflect.Interface:
		return map[string]any{}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.String:
		schema := map[string]any{"type": "string"}
		if values := g.enums[typ.Name()]; len(values) > 0 {
			schema["enum"] = values
		}
		return schema
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Slice, reflect.Array:
		return map[string]any{
			"type":  "array",
			"items": g.schemaFor(typ.Elem()),
		}
	case reflect.Map:
		if typ.Key().Kind() != reflect.String {
			panic(fmt.Sprintf("unsupported graph map key type %s", typ.Key()))
		}
		return map[string]any{
			"type":                 "object",
			"additionalProperties": g.schemaFor(typ.Elem()),
		}
	case reflect.Struct:
		return g.structSchema(typ)
	default:
		panic(fmt.Sprintf("unsupported graph schema type %s", typ))
	}
}

func (g *schemaGenerator) structSchema(typ reflect.Type) map[string]any {
	properties := make(map[string]any, typ.NumField())
	required := make([]string, 0, typ.NumField())

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name, omitEmpty, skip := jsonField(field)
		if skip {
			continue
		}

		fieldSchema := g.schemaFor(field.Type)
		annotateExtensionPoint(typ, field, fieldSchema)
		properties[name] = fieldSchema
		if !omitEmpty {
			required = append(required, name)
		}
	}

	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func jsonField(field reflect.StructField) (name string, omitEmpty bool, skip bool) {
	tag := field.Tag.Get("json")
	parts := strings.Split(tag, ",")
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

func annotateExtensionPoint(owner reflect.Type, field reflect.StructField, schema map[string]any) {
	if owner != reflect.TypeFor[graph.Node]() {
		return
	}

	switch field.Name {
	case "Kind":
		schema["description"] = "Open identifier resolved through the registered step-kind registry."
		schema["x-workflow-extension-point"] = "registered-step-kind"
	case "Config":
		schema["description"] = "Opaque adapter-owned configuration validated by the registered step kind."
		schema["x-workflow-extension-point"] = "adapter-config"
	}
}

func graphEnumValues() (map[string][]string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("locate schema generator source")
	}
	enumsPath := filepath.Join(filepath.Dir(filename), "..", "..", "enums.go")

	parsed, err := parser.ParseFile(token.NewFileSet(), enumsPath, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parse graph enum declarations: %w", err)
	}

	enums := make(map[string][]string)
	for _, declaration := range parsed.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.CONST {
			continue
		}
		var inheritedType string
		for _, specification := range generic.Specs {
			values := specification.(*ast.ValueSpec)
			if values.Type != nil {
				identifier, ok := values.Type.(*ast.Ident)
				if !ok {
					continue
				}
				inheritedType = identifier.Name
			}
			if inheritedType == "" {
				continue
			}
			for _, expression := range values.Values {
				literal, ok := expression.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					return nil, fmt.Errorf("decode %s enum value: %w", inheritedType, err)
				}
				enums[inheritedType] = append(enums[inheritedType], value)
			}
		}
	}
	return enums, nil
}
