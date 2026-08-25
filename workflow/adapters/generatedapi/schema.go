package generatedapi

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	maxSchemaDepth = 64
	maxSchemaBytes = 1 << 20
	maxSchemaNodes = 100_000
)

type schemaResolver struct {
	components map[string]any
}

func (r schemaResolver) resolve(input any) (graph.Schema, error) {
	count := 0
	resolved, err := r.walk(input, 0, make(map[string]bool), &count)
	if err != nil {
		return nil, err
	}
	schema, ok := resolved.(map[string]any)
	if !ok {
		return nil, invalidSource("schemas must be objects")
	}
	encoded, err := json.Marshal(schema)
	if err != nil || len(encoded) > maxSchemaBytes {
		return nil, invalidSource("resolved schema exceeds %d-byte bound", maxSchemaBytes)
	}
	result := graph.Schema(schema)
	if err := values.ValidateSchema(result); err != nil {
		return nil, invalidSource("schema is not valid local JSON Schema: %v", err)
	}
	return result, nil
}

func (r schemaResolver) walk(input any, depth int, active map[string]bool, count *int) (any, error) {
	if depth > maxSchemaDepth {
		return nil, invalidSource("schema exceeds reference/depth bound")
	}
	(*count)++
	if *count > maxSchemaNodes {
		return nil, invalidSource("resolved schema exceeds structural size bound")
	}
	switch typed := input.(type) {
	case map[string]any:
		if reference, hasRef := typed["$ref"]; hasRef {
			text, ok := reference.(string)
			if !ok || len(typed) != 1 {
				return nil, invalidSource("schema $ref must be the only field and a string")
			}
			name, err := componentSchemaName(text)
			if err != nil {
				return nil, err
			}
			if active[name] {
				return nil, invalidSource("schema reference cycle at component %q", name)
			}
			target, ok := r.components[name]
			if !ok {
				return nil, invalidSource("schema component %q was not found", name)
			}
			active[name] = true
			resolved, resolveErr := r.walk(target, depth+1, active, count)
			delete(active, name)
			return resolved, resolveErr
		}
		result := make(map[string]any, len(typed))
		nullable := false
		for key, value := range typed {
			switch key {
			case "nullable":
				flag, ok := value.(bool)
				if !ok {
					return nil, invalidSource("schema nullable must be boolean")
				}
				nullable = flag
				continue
			case "readOnly", "writeOnly", "discriminator", "xml", "externalDocs":
				return nil, invalidSource("schema keyword %q is outside the portable generated subset", key)
			case "$id", "$schema", "$anchor", "$dynamicRef", "$dynamicAnchor":
				return nil, invalidSource("schema resource keyword %q is not allowed", key)
			case "exclusiveMinimum", "exclusiveMaximum":
				if _, boolean := value.(bool); boolean {
					return nil, invalidSource("OpenAPI 3.0 boolean %s is not supported", key)
				}
			}
			child, err := r.walkKeyword(key, value, depth+1, active, count)
			if err != nil {
				return nil, err
			}
			result[key] = child
		}
		if nullable {
			valueType, ok := result["type"].(string)
			if !ok || valueType == "null" {
				return nil, invalidSource("nullable schemas require one non-null string type")
			}
			result["type"] = []any{valueType, "null"}
		}
		return result, nil
	case []any:
		result := make([]any, len(typed))
		for index, child := range typed {
			resolved, err := r.cloneLiteral(child, depth+1, count)
			if err != nil {
				return nil, err
			}
			result[index] = resolved
		}
		return result, nil
	case nil, bool, string, json.Number:
		return typed, nil
	default:
		return nil, invalidSource("schema contains non-JSON value %T", input)
	}
}

func (r schemaResolver) walkKeyword(key string, value any, depth int, active map[string]bool, count *int) (any, error) {
	switch key {
	case "properties", "patternProperties", "$defs", "definitions", "dependentSchemas":
		object, ok := value.(map[string]any)
		if !ok {
			return nil, invalidSource("schema keyword %q must be an object", key)
		}
		result := make(map[string]any, len(object))
		for name, child := range object {
			resolved, err := r.walk(child, depth+1, active, count)
			if err != nil {
				return nil, err
			}
			result[name] = resolved
		}
		return result, nil
	case "items", "additionalProperties", "unevaluatedProperties", "contains", "not", "if", "then", "else", "propertyNames":
		return r.walk(value, depth+1, active, count)
	case "allOf", "anyOf", "oneOf", "prefixItems":
		items, ok := value.([]any)
		if !ok {
			return nil, invalidSource("schema keyword %q must be an array", key)
		}
		result := make([]any, len(items))
		for index, child := range items {
			resolved, err := r.walk(child, depth+1, active, count)
			if err != nil {
				return nil, err
			}
			result[index] = resolved
		}
		return result, nil
	default:
		return r.cloneLiteral(value, depth+1, count)
	}
}

func (r schemaResolver) cloneLiteral(input any, depth int, count *int) (any, error) {
	if depth > maxSchemaDepth {
		return nil, invalidSource("schema exceeds structural depth bound")
	}
	(*count)++
	if *count > maxSchemaNodes {
		return nil, invalidSource("resolved schema exceeds structural size bound")
	}
	switch typed := input.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, child := range typed {
			cloned, err := r.cloneLiteral(child, depth+1, count)
			if err != nil {
				return nil, err
			}
			result[key] = cloned
		}
		return result, nil
	case []any:
		result := make([]any, len(typed))
		for index, child := range typed {
			cloned, err := r.cloneLiteral(child, depth+1, count)
			if err != nil {
				return nil, err
			}
			result[index] = cloned
		}
		return result, nil
	case nil, bool, string, json.Number:
		return typed, nil
	default:
		return nil, invalidSource("schema contains non-JSON value %T", input)
	}
}

func componentSchemaName(reference string) (string, error) {
	const prefix = "#/components/schemas/"
	if !strings.HasPrefix(reference, prefix) {
		return "", invalidSource("external or non-schema reference %q is not supported", reference)
	}
	raw := strings.TrimPrefix(reference, prefix)
	if raw == "" || strings.Contains(raw, "/") {
		return "", invalidSource("schema reference %q must address one component", reference)
	}
	var result strings.Builder
	for index := 0; index < len(raw); index++ {
		if raw[index] != '~' {
			result.WriteByte(raw[index])
			continue
		}
		if index+1 >= len(raw) {
			return "", invalidSource("schema reference %q has invalid pointer escaping", reference)
		}
		index++
		switch raw[index] {
		case '0':
			result.WriteByte('~')
		case '1':
			result.WriteByte('/')
		default:
			return "", invalidSource("schema reference %q has invalid pointer escaping", reference)
		}
	}
	if result.Len() == 0 {
		return "", fmt.Errorf("%w: schema reference name is empty", ErrInvalidSource)
	}
	return result.String(), nil
}

func primitiveSchema(schema graph.Schema, allowArray bool) (array bool, err error) {
	valueType, ok := schema["type"].(string)
	if !ok {
		return false, invalidSource("parameter schemas require one explicit type")
	}
	switch valueType {
	case "string", "integer", "number", "boolean":
		return false, nil
	case "array":
		if !allowArray {
			return false, invalidSource("only query parameters may be arrays")
		}
		items, ok := schema["items"].(map[string]any)
		if !ok {
			return false, invalidSource("array parameters require an item schema")
		}
		if _, err := primitiveSchema(graph.Schema(items), false); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, invalidSource("parameter type %q is outside the portable subset", valueType)
	}
}
