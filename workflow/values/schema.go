package values

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
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

// ValidateValueSetSchema validates the logical object formed by a named set of
// typed Values. Inline values contribute their JSON payload, artifacts
// contribute the immutable ArtifactRef projection, and secret references
// contribute only their canonical opaque URI. The graph-native artifact and
// secret_ref schema types remain identity-sensitive: an inline object or
// string cannot satisfy them merely because its JSON projection has the same
// shape. Local references and combinators retain that identity at each named
// value boundary. Identity-bearing entries must use exact named properties;
// schemas that route them through patternProperties or a schema-valued
// additional/unevaluatedProperties keyword are rejected explicitly, as are
// conditional schemas over identity-bearing entries.
func ValidateValueSetSchema(schema graph.Schema, set ValueSet) error {
	if err := set.Validate(); err != nil {
		return fmt.Errorf("%w: invalid value set: %w", ErrSchemaMismatch, err)
	}
	if err := ValidateSchema(schema); err != nil {
		return err
	}
	document, err := valueSchemaDocument(schema)
	if err != nil {
		return err
	}
	instance, err := valueSetSchemaInstance(set)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSchemaMismatch, err)
	}
	specializer := valueSetSchemaSpecializer{
		root: document, set: set,
		variants: make(map[valueSetSchemaVariant]string),
		defs:     make(map[string]any),
	}
	rewritten, err := specializer.rewrite(document, valueSchemaInline, true, false, nil)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSchema, err)
	}
	rewrittenDocument, ok := rewritten.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: value-set schema root must be an object", ErrInvalidSchema)
	}
	if len(specializer.defs) != 0 {
		rewrittenDocument["$defs"] = specializer.defs
	}
	compiled, err := compileValueSchema(rewrittenDocument)
	if err != nil {
		return err
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

type valueSchemaMode uint8

const (
	valueSchemaInline valueSchemaMode = iota
	valueSchemaArtifact
	valueSchemaSecretRef
)

type valueSetSchemaVariant struct {
	ref       string
	mode      valueSchemaMode
	atSetRoot bool
	relaxed   string
}

type valueSetSchemaSpecializer struct {
	root     map[string]any
	set      ValueSet
	variants map[valueSetSchemaVariant]string
	defs     map[string]any
}

func valueSetSchemaInstance(set ValueSet) (map[string]any, error) {
	instance := make(map[string]any, len(set))
	for name, value := range set {
		var payload any
		var err error
		switch value.Type {
		case TypeArtifact:
			payload, err = plainJSONValue(value.Artifact)
		case TypeSecretRef:
			payload = string(*value.SecretRef)
		default:
			payload, err = plainJSONValue(value.Inline)
		}
		if err != nil {
			return nil, fmt.Errorf("value-set[%q]: %w", name, err)
		}
		instance[name] = payload
	}
	return instance, nil
}

func valueSetMode(value Value) valueSchemaMode {
	switch value.Type {
	case TypeArtifact:
		return valueSchemaArtifact
	case TypeSecretRef:
		return valueSchemaSecretRef
	default:
		return valueSchemaInline
	}
}

func (s *valueSetSchemaSpecializer) rewrite(
	schema any,
	mode valueSchemaMode,
	atSetRoot bool,
	enforceIdentity bool,
	relaxed map[string]bool,
) (any, error) {
	switch typed := schema.(type) {
	case bool:
		return typed, nil
	case map[string]any:
		if atSetRoot {
			relaxed = s.conjunctiveIdentityDeclarations(typed, relaxed)
		}
		if enforceIdentity && len(typed) != 0 {
			valueType := "artifact"
			if mode == valueSchemaSecretRef {
				valueType = "secret_ref"
			}
			if !schemaPermitsType(s.root, typed, valueType, make(map[string]bool)) {
				return false, nil
			}
		}

		result := make(map[string]any, len(typed))
		for key, child := range typed {
			switch key {
			case "$defs", "definitions":
				// Local references are materialized as context-specific variants
				// below. Unused declarations were already checked by ValidateSchema.
				continue
			case "$ref":
				ref, ok := child.(string)
				if !ok || !strings.HasPrefix(ref, "#/") {
					result[key] = child
					continue
				}
				variant, err := s.reference(ref, mode, atSetRoot, relaxed)
				if err != nil {
					return nil, err
				}
				result[key] = variant
			case "properties":
				properties, ok := child.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("schema map keyword must contain an object")
				}
				rewritten := make(map[string]any, len(properties))
				for name, propertySchema := range properties {
					propertyMode := valueSchemaInline
					propertyBoundary := false
					if atSetRoot {
						if value, exists := s.set[name]; exists {
							propertyMode = valueSetMode(value)
							propertyBoundary = propertyMode != valueSchemaInline && !relaxed[name]
						}
					}
					value, err := s.rewrite(propertySchema, propertyMode, false, propertyBoundary, nil)
					if err != nil {
						return nil, fmt.Errorf("subschema %q: %w", name, err)
					}
					rewritten[name] = value
				}
				result[key] = rewritten
			case "patternProperties":
				if atSetRoot && valueSetHasIdentityValues(s.set) {
					return nil, fmt.Errorf("identity-bearing value-set entries require exact properties; patternProperties is unsupported")
				}
				rewritten, err := s.rewriteSchemaMap(child, valueSchemaInline, false, nil)
				if err != nil {
					return nil, err
				}
				result[key] = rewritten
			case "dependentSchemas":
				rewritten, err := s.rewriteSchemaMap(child, mode, atSetRoot, relaxed)
				if err != nil {
					return nil, err
				}
				result[key] = rewritten
			case "additionalProperties", "unevaluatedProperties":
				if atSetRoot && valueSetHasIdentityValues(s.set) {
					_, schemaValued := child.(map[string]any)
					unsupported := schemaValued && (key == "unevaluatedProperties" || valueSetIdentityOutsideProperties(s.set, typed["properties"]))
					if unsupported {
						return nil, fmt.Errorf("identity-bearing value-set entries require exact properties; schema-valued %s is unsupported", key)
					}
				}
				rewritten, err := s.rewrite(child, mode, false, false, nil)
				if err != nil {
					return nil, err
				}
				result[key] = rewritten
			case "if", "then", "else":
				if atSetRoot && valueSetHasIdentityValues(s.set) {
					return nil, fmt.Errorf("identity-bearing value-set entries do not support conditional schemas")
				}
				rewritten, err := s.rewrite(child, mode, atSetRoot, false, relaxed)
				if err != nil {
					return nil, err
				}
				result[key] = rewritten
			case "not":
				rewritten, err := s.rewrite(child, mode, atSetRoot, false, relaxed)
				if err != nil {
					return nil, err
				}
				result[key] = rewritten
			case "items", "additionalItems", "unevaluatedItems", "propertyNames", "contains", "contentSchema":
				rewritten, err := s.rewrite(child, mode, false, false, nil)
				if err != nil {
					return nil, err
				}
				result[key] = rewritten
			case "prefixItems", "allOf", "anyOf", "oneOf":
				rewritten, err := s.rewriteSchemaList(child, mode, atSetRoot, relaxed)
				if err != nil {
					return nil, err
				}
				result[key] = rewritten
			case "dependencies":
				rewritten, err := s.rewriteDependencies(child, mode, atSetRoot, relaxed)
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
		if declared, exists := typed["type"]; exists {
			rewritten, ok, err := specializeValueSetSchemaType(declared, mode)
			if err != nil {
				return nil, err
			}
			if !ok {
				return false, nil
			}
			result["type"] = rewritten
		}
		return result, nil
	default:
		return nil, fmt.Errorf("subschema must be an object or boolean")
	}
}

func valueSetHasIdentityValues(set ValueSet) bool {
	for _, value := range set {
		if value.Type == TypeArtifact || value.Type == TypeSecretRef {
			return true
		}
	}
	return false
}

func valueSetIdentityOutsideProperties(set ValueSet, declaration any) bool {
	properties, _ := declaration.(map[string]any)
	for name, value := range set {
		if value.Type != TypeArtifact && value.Type != TypeSecretRef {
			continue
		}
		if _, exists := properties[name]; !exists {
			return true
		}
	}
	return false
}

func (s *valueSetSchemaSpecializer) conjunctiveIdentityDeclarations(
	schema map[string]any,
	inherited map[string]bool,
) map[string]bool {
	result := cloneSchemaNameSet(inherited)
	names := make([]string, 0, len(s.set))
	for name := range s.set {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := s.set[name]
		if value.Type != TypeArtifact && value.Type != TypeSecretRef {
			continue
		}
		valueType := "artifact"
		if value.Type == TypeSecretRef {
			valueType = "secret_ref"
		}
		if schemaConjunctivelyPermitsPropertyType(s.root, schema, name, valueType, make(map[string]bool)) {
			result[name] = true
		}
	}
	return result
}

func schemaConjunctivelyPermitsPropertyType(
	root map[string]any,
	schema any,
	property string,
	valueType string,
	resolving map[string]bool,
) bool {
	object, ok := schema.(map[string]any)
	if !ok {
		return false
	}
	if properties, ok := object["properties"].(map[string]any); ok {
		if propertySchema, exists := properties[property]; exists &&
			schemaPermitsType(root, propertySchema, valueType, make(map[string]bool)) {
			return true
		}
	}
	if ref, ok := object["$ref"].(string); ok && strings.HasPrefix(ref, "#") && !resolving[ref] {
		if target, resolved := resolveLocalSchemaRef(root, ref); resolved {
			resolving[ref] = true
			permitted := schemaConjunctivelyPermitsPropertyType(root, target, property, valueType, resolving)
			delete(resolving, ref)
			if permitted {
				return true
			}
		}
	}
	if branches, ok := object["allOf"].([]any); ok {
		for _, branch := range branches {
			if schemaConjunctivelyPermitsPropertyType(root, branch, property, valueType, resolving) {
				return true
			}
		}
	}
	return false
}

func cloneSchemaNameSet(input map[string]bool) map[string]bool {
	result := make(map[string]bool, len(input))
	for name, enabled := range input {
		if enabled {
			result[name] = true
		}
	}
	return result
}

func schemaNameSetSignature(input map[string]bool) string {
	names := make([]string, 0, len(input))
	for name, enabled := range input {
		if enabled {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return strings.Join(names, "\x00")
}

func (s *valueSetSchemaSpecializer) reference(
	ref string,
	mode valueSchemaMode,
	atSetRoot bool,
	relaxed map[string]bool,
) (string, error) {
	variant := valueSetSchemaVariant{ref: ref, mode: mode, atSetRoot: atSetRoot, relaxed: schemaNameSetSignature(relaxed)}
	if name, exists := s.variants[variant]; exists {
		return "#/$defs/" + name, nil
	}
	target, ok := resolveLocalSchemaRef(s.root, ref)
	if !ok {
		return "", fmt.Errorf("cannot resolve local schema reference %q", ref)
	}
	name := fmt.Sprintf("__hadron_value_set_ref_%d", len(s.variants))
	s.variants[variant] = name
	s.defs[name] = true
	rewritten, err := s.rewrite(target, mode, atSetRoot, false, relaxed)
	if err != nil {
		return "", err
	}
	s.defs[name] = rewritten
	return "#/$defs/" + name, nil
}

func (s *valueSetSchemaSpecializer) rewriteSchemaMap(
	value any,
	mode valueSchemaMode,
	atSetRoot bool,
	relaxed map[string]bool,
) (map[string]any, error) {
	container, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("schema map keyword must contain an object")
	}
	result := make(map[string]any, len(container))
	for name, schema := range container {
		rewritten, err := s.rewrite(schema, mode, atSetRoot, false, relaxed)
		if err != nil {
			return nil, fmt.Errorf("subschema %q: %w", name, err)
		}
		result[name] = rewritten
	}
	return result, nil
}

func (s *valueSetSchemaSpecializer) rewriteSchemaList(
	value any,
	mode valueSchemaMode,
	atSetRoot bool,
	relaxed map[string]bool,
) ([]any, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("schema list keyword must contain an array")
	}
	result := make([]any, len(items))
	for index, schema := range items {
		rewritten, err := s.rewrite(schema, mode, atSetRoot, false, relaxed)
		if err != nil {
			return nil, fmt.Errorf("subschema[%d]: %w", index, err)
		}
		result[index] = rewritten
	}
	return result, nil
}

func (s *valueSetSchemaSpecializer) rewriteDependencies(
	value any,
	mode valueSchemaMode,
	atSetRoot bool,
	relaxed map[string]bool,
) (map[string]any, error) {
	container, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("dependencies must contain an object")
	}
	result := make(map[string]any, len(container))
	for name, dependency := range container {
		switch dependency.(type) {
		case map[string]any, bool:
			rewritten, err := s.rewrite(dependency, mode, atSetRoot, false, relaxed)
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

func specializeValueSetSchemaType(declared any, mode valueSchemaMode) (any, bool, error) {
	var names []string
	switch typed := declared.(type) {
	case string:
		names = []string{typed}
	case []any:
		names = make([]string, len(typed))
		for index, candidate := range typed {
			name, ok := candidate.(string)
			if !ok {
				return nil, false, fmt.Errorf("schema type array entries must be strings")
			}
			names[index] = name
		}
	default:
		return nil, false, fmt.Errorf("schema type must be a string or array of strings")
	}

	selected := make([]any, 0, len(names))
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		switch mode {
		case valueSchemaArtifact:
			if name == "secret_ref" {
				continue
			}
			if name == "artifact" {
				name = "object"
			}
		case valueSchemaSecretRef:
			if name == "artifact" {
				continue
			}
			if name == "secret_ref" {
				name = "string"
			}
		default:
			if name == "artifact" || name == "secret_ref" {
				continue
			}
		}
		if !seen[name] {
			selected = append(selected, name)
			seen[name] = true
		}
	}
	if len(selected) == 0 {
		return nil, false, nil
	}
	if _, scalar := declared.(string); scalar && len(selected) == 1 {
		return selected[0], true, nil
	}
	return selected, true, nil
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
