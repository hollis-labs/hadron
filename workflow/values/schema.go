package values

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/workflow/graph"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

var (
	// ErrInvalidSchema identifies a declaration that cannot compile as local,
	// daemon-independent JSON Schema.
	ErrInvalidSchema = errors.New("invalid workflow value schema")
	// ErrSchemaMismatch identifies a valid schema rejected by a typed value.
	ErrSchemaMismatch = errors.New("workflow value does not satisfy schema")
)

const valueSchemaResource = "urn:hadron:workflow:values:schema"

// ValidateValueSchema validates one typed Value against an inline JSON Schema.
// Local JSON Pointer and $defs references are supported. Network, file, and
// other external resource loading is always rejected. The graph-native
// "artifact" type validates the immutable ArtifactRef projection and
// "secret_ref" validates an opaque canonical reference string. Plain string
// schemas do not accept typed secret references. An empty schema accepts every
// valid Value envelope.
func ValidateValueSchema(schema graph.Schema, value Value) error {
	if err := value.Validate(); err != nil {
		return fmt.Errorf("%w: invalid value envelope: %w", ErrSchemaMismatch, err)
	}
	document, err := valueSchemaDocument(schema)
	if err != nil {
		return err
	}
	declarationDocument, err := rewriteValueTypes(document, true, true)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSchema, err)
	}
	declarationSchema, err := compileValueSchema(declarationDocument)
	if err != nil {
		return err
	}

	var instance any
	compiled := declarationSchema
	switch value.Type {
	case TypeArtifact:
		if len(document) != 0 && !schemaPermitsArtifact(document, document, make(map[string]bool)) {
			return fmt.Errorf("%w: schema does not explicitly permit artifact", ErrSchemaMismatch)
		}
		instance, err = plainJSONValue(value.Artifact)
	case TypeSecretRef:
		if len(document) != 0 && !schemaPermitsSecretRef(document, document, make(map[string]bool)) {
			return fmt.Errorf("%w: schema does not explicitly permit secret_ref", ErrSchemaMismatch)
		}
		secretDocument, rewriteErr := rewriteValueTypes(document, false, true)
		if rewriteErr != nil {
			return fmt.Errorf("%w: %w", ErrInvalidSchema, rewriteErr)
		}
		compiled, err = compileValueSchema(secretDocument)
		if err == nil {
			instance = string(*value.SecretRef)
		}
	default:
		inlineDocument, rewriteErr := rewriteValueTypes(document, false, false)
		if rewriteErr != nil {
			return fmt.Errorf("%w: %w", ErrInvalidSchema, rewriteErr)
		}
		compiled, err = compileValueSchema(inlineDocument)
		if err == nil {
			instance, err = plainJSONValue(value.Inline)
		}
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSchemaMismatch, err)
	}
	if err := compiled.Validate(instance); err != nil {
		return fmt.Errorf("%w: %w", ErrSchemaMismatch, err)
	}
	return nil
}

// ValidateSchema compiles an inline workflow value schema without requiring an
// instance. It is the declaration-validation path for optional values that are
// absent at a particular binding. Local references are supported and external
// resource loading is denied exactly as in ValidateValueSchema.
func ValidateSchema(schema graph.Schema) error {
	document, err := valueSchemaDocument(schema)
	if err != nil {
		return err
	}
	rewritten, err := rewriteValueTypes(document, true, true)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSchema, err)
	}
	_, err = compileValueSchema(rewritten)
	return err
}

func compileValueSchema(document any) (*jsonschema.Schema, error) {
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(localOnlySchemaLoader{})
	if err := compiler.AddResource(valueSchemaResource, document); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSchema, err)
	}
	compiled, err := compiler.Compile(valueSchemaResource)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSchema, err)
	}
	return compiled, nil
}

func valueSchemaDocument(schema graph.Schema) (map[string]any, error) {
	if schema == nil {
		return map[string]any{}, nil
	}
	document, err := plainJSONValue(schema)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSchema, err)
	}
	object, ok := document.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: schema must be an object", ErrInvalidSchema)
	}
	return object, nil
}

func rewriteValueTypes(value any, allowArtifact, allowSecretRef bool) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, child := range typed {
			switch key {
			case "$defs", "definitions", "properties", "patternProperties", "dependentSchemas":
				rewritten, err := rewriteSchemaMap(child, allowArtifact, allowSecretRef)
				if err != nil {
					return nil, err
				}
				result[key] = rewritten
			case "items", "additionalItems", "additionalProperties", "unevaluatedItems", "unevaluatedProperties",
				"propertyNames", "contains", "not", "if", "then", "else", "contentSchema":
				rewritten, err := rewriteValueTypes(child, allowArtifact, allowSecretRef)
				if err != nil {
					return nil, err
				}
				result[key] = rewritten
			case "prefixItems", "allOf", "anyOf", "oneOf":
				rewritten, err := rewriteSchemaList(child, allowArtifact, allowSecretRef)
				if err != nil {
					return nil, err
				}
				result[key] = rewritten
			case "dependencies":
				rewritten, err := rewriteLegacyDependencies(child, allowArtifact, allowSecretRef)
				if err != nil {
					return nil, err
				}
				result[key] = rewritten
			default:
				// const, enum, default, examples, and unknown annotation or
				// extension values are literal instance data, not subschemas.
				result[key] = child
			}
		}
		declared, exists := typed["type"]
		if !exists {
			return result, nil
		}
		switch schemaType := declared.(type) {
		case string:
			if schemaType != "artifact" && schemaType != "secret_ref" {
				return result, nil
			}
			if schemaType == "artifact" && allowArtifact {
				result["type"] = "object"
				return result, nil
			}
			if schemaType == "secret_ref" && allowSecretRef {
				result["type"] = "string"
				return result, nil
			}
			return map[string]any{"not": map[string]any{}}, nil
		case []any:
			types := make([]any, 0, len(schemaType))
			seen := make(map[string]bool, len(schemaType))
			for _, item := range schemaType {
				name, ok := item.(string)
				if !ok {
					return nil, fmt.Errorf("schema type array entries must be strings")
				}
				if name == "artifact" {
					if !allowArtifact {
						continue
					}
					name = "object"
				} else if name == "secret_ref" {
					if !allowSecretRef {
						continue
					}
					name = "string"
				}
				if !seen[name] {
					types = append(types, name)
					seen[name] = true
				}
			}
			if len(types) == 0 {
				return map[string]any{"not": map[string]any{}}, nil
			}
			result["type"] = types
			return result, nil
		default:
			return nil, fmt.Errorf("schema type must be a string or array of strings")
		}
	case bool:
		return typed, nil
	default:
		return nil, fmt.Errorf("subschema must be an object or boolean")
	}
}

func rewriteSchemaMap(value any, allowArtifact, allowSecretRef bool) (map[string]any, error) {
	container, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("schema map keyword must contain an object")
	}
	result := make(map[string]any, len(container))
	for name, schema := range container {
		rewritten, err := rewriteValueTypes(schema, allowArtifact, allowSecretRef)
		if err != nil {
			return nil, fmt.Errorf("subschema %q: %w", name, err)
		}
		result[name] = rewritten
	}
	return result, nil
}

func rewriteSchemaList(value any, allowArtifact, allowSecretRef bool) ([]any, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("schema list keyword must contain an array")
	}
	result := make([]any, len(items))
	for index, schema := range items {
		rewritten, err := rewriteValueTypes(schema, allowArtifact, allowSecretRef)
		if err != nil {
			return nil, fmt.Errorf("subschema[%d]: %w", index, err)
		}
		result[index] = rewritten
	}
	return result, nil
}

func rewriteLegacyDependencies(value any, allowArtifact, allowSecretRef bool) (map[string]any, error) {
	container, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("dependencies must contain an object")
	}
	result := make(map[string]any, len(container))
	for name, dependency := range container {
		switch dependency.(type) {
		case map[string]any, bool:
			rewritten, err := rewriteValueTypes(dependency, allowArtifact, allowSecretRef)
			if err != nil {
				return nil, fmt.Errorf("dependency %q: %w", name, err)
			}
			result[name] = rewritten
		default:
			result[name] = dependency
		}
	}
	return result, nil
}

func schemaPermitsArtifact(root, schema any, resolving map[string]bool) bool {
	return schemaPermitsType(root, schema, "artifact", resolving)
}

func schemaPermitsSecretRef(root, schema any, resolving map[string]bool) bool {
	return schemaPermitsType(root, schema, "secret_ref", resolving)
}

func schemaPermitsType(root, schema any, valueType string, resolving map[string]bool) bool {
	switch typed := schema.(type) {
	case bool:
		return typed
	case map[string]any:
		if len(typed) == 0 {
			return true
		}
		if declared, ok := typed["type"]; ok {
			switch schemaType := declared.(type) {
			case string:
				return schemaType == valueType
			case []any:
				for _, candidate := range schemaType {
					if candidate == valueType {
						return true
					}
				}
			}
			return false
		}
		if ref, ok := typed["$ref"].(string); ok && strings.HasPrefix(ref, "#") && !resolving[ref] {
			if target, resolved := resolveLocalSchemaRef(root, ref); resolved {
				resolving[ref] = true
				allowed := schemaPermitsType(root, target, valueType, resolving)
				delete(resolving, ref)
				return allowed
			}
		}
		for _, keyword := range []string{"allOf", "anyOf", "oneOf"} {
			branches, ok := typed[keyword].([]any)
			if !ok {
				continue
			}
			for _, branch := range branches {
				if schemaPermitsType(root, branch, valueType, resolving) {
					return true
				}
			}
		}
	}
	return false
}

func resolveLocalSchemaRef(root any, ref string) (any, bool) {
	if ref == "#" {
		return root, true
	}
	if !strings.HasPrefix(ref, "#/") {
		return nil, false
	}
	pointer, err := url.PathUnescape(strings.TrimPrefix(ref, "#"))
	if err != nil {
		return nil, false
	}
	current := root
	for _, raw := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		part := strings.ReplaceAll(strings.ReplaceAll(raw, "~1", "/"), "~0", "~")
		switch container := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = container[part]
			if !ok {
				return nil, false
			}
		case []any:
			index, parseErr := strconv.Atoi(part)
			if parseErr != nil || index < 0 || index >= len(container) {
				return nil, false
			}
			current = container[index]
		default:
			return nil, false
		}
	}
	return current, true
}

type localOnlySchemaLoader struct{}

func (localOnlySchemaLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("external schema resource %q is unavailable to workflow value validation", url)
}

func plainJSONValue(value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return decoded, nil
}
