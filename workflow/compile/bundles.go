package compile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

var (
	// ErrBundledDefinitionNotFound reports that a plan does not contain the
	// exact immutable definition requested by a call node.
	ErrBundledDefinitionNotFound = errors.New("bundled workflow definition not found")
	// ErrBundledDefinitionConflict reports duplicate or changed immutable
	// definition identity in one plan bundle.
	ErrBundledDefinitionConflict = errors.New("bundled workflow definition conflict")
	// ErrInvalidBundledDefinition reports malformed serialized definition
	// material.
	ErrInvalidBundledDefinition = errors.New("invalid bundled workflow definition")
)

// NewBundledDefinitionResolver constructs an immutable resolver solely from a
// serialized execution plan. It does not retain plan-owned maps or slices and
// therefore remains safe after the caller mutates or discards the plan.
func NewBundledDefinitionResolver(plan *ExecutionPlan) (DefinitionResolver, error) {
	if plan == nil {
		return nil, fmt.Errorf("%w: execution plan is required", ErrInvalidBundledDefinition)
	}
	if err := bundledDefinitionConflicts(plan.BundledDefinitions); err != nil {
		return nil, err
	}
	definitions, findings := normalizeBundledDefinitions(plan.BundledDefinitions, graphSource(plan.Graph))
	if len(findings) != 0 {
		return nil, fmt.Errorf("%w: %s", ErrInvalidBundledDefinition, findings[0].Message)
	}
	indexed := make(map[string]ResolvedDefinition, len(definitions))
	for _, definition := range definitions {
		indexed[bundledDefinitionKey(definition.Definition)] = definition
	}
	return &bundledDefinitionResolver{definitions: indexed}, nil
}

func bundledDefinitionConflicts(input []ResolvedDefinition) error {
	seen := make(map[string]ResolvedDefinition, len(input))
	for _, definition := range input {
		key := bundledDefinitionKey(definition.Definition)
		if prior, exists := seen[key]; exists && !reflect.DeepEqual(prior, definition) {
			return fmt.Errorf("%w: immutable definition tuple is repeated with different content", ErrBundledDefinitionConflict)
		}
		seen[key] = definition
	}
	return nil
}

type bundledDefinitionResolver struct{ definitions map[string]ResolvedDefinition }

func (r *bundledDefinitionResolver) ResolveDefinition(ctx context.Context, ref graph.DefinitionRef) (ResolvedDefinition, error) {
	if ctx == nil {
		return ResolvedDefinition{}, fmt.Errorf("%w: context is required", ErrBundledDefinitionNotFound)
	}
	if err := ctx.Err(); err != nil {
		return ResolvedDefinition{}, err
	}
	if r == nil || r.definitions == nil {
		return ResolvedDefinition{}, fmt.Errorf("%w: resolver is not initialized", ErrBundledDefinitionNotFound)
	}
	if err := validateRequestedBundleRef(ref); err != nil {
		return ResolvedDefinition{}, fmt.Errorf("%w: request is invalid: %w", ErrBundledDefinitionNotFound, err)
	}
	resolved, found := r.definitions[bundledDefinitionKey(ref)]
	if !found {
		return ResolvedDefinition{}, fmt.Errorf("%w: immutable definition tuple is absent", ErrBundledDefinitionNotFound)
	}
	if ref.Provenance != nil && !reflect.DeepEqual(ref.Provenance, resolved.Definition.Provenance) {
		return ResolvedDefinition{}, fmt.Errorf("%w: requested provenance differs from bundled definition", ErrBundledDefinitionNotFound)
	}
	var cloned ResolvedDefinition
	if err := cloneExpansionJSON(resolved, &cloned); err != nil {
		return ResolvedDefinition{}, fmt.Errorf("%w: copy resolved definition", ErrInvalidBundledDefinition)
	}
	return cloned, nil
}

func normalizeBundledDefinitions(input []ResolvedDefinition, source *graph.SourceRef) ([]ResolvedDefinition, []diagnostic.Diagnostic) {
	if len(input) == 0 {
		return nil, nil
	}
	definitions := make([]ResolvedDefinition, 0, len(input))
	for _, candidate := range input {
		var cloned ResolvedDefinition
		if err := cloneExpansionJSON(candidate, &cloned); err != nil {
			return nil, []diagnostic.Diagnostic{expansionDiagnostic(source, "bundled definition is not JSON-compatible")}
		}
		if err := validateBundledDefinition(cloned); err != nil {
			return nil, []diagnostic.Diagnostic{expansionDiagnostic(source, fmt.Sprintf("bundled definition is invalid: %v", err))}
		}
		definitions = append(definitions, cloned)
	}
	sort.SliceStable(definitions, func(i, j int) bool {
		return bundledDefinitionKey(definitions[i].Definition) < bundledDefinitionKey(definitions[j].Definition)
	})
	result := definitions[:0]
	for _, definition := range definitions {
		if len(result) == 0 || bundledDefinitionKey(result[len(result)-1].Definition) != bundledDefinitionKey(definition.Definition) {
			result = append(result, definition)
			continue
		}
		if !reflect.DeepEqual(result[len(result)-1], definition) {
			return nil, []diagnostic.Diagnostic{expansionDiagnostic(source, "bundled definitions reuse one immutable identity with different content")}
		}
	}
	return result, nil
}

func validateBundledDefinition(input ResolvedDefinition) error {
	if err := validateRequestedBundleRef(input.Definition); err != nil {
		return err
	}
	if err := graph.ValidateID(input.Graph.ID); err != nil {
		return fmt.Errorf("graph id: %w", err)
	}
	if err := input.Graph.ValidateEnums(); err != nil {
		return fmt.Errorf("graph enums: %w", err)
	}
	if err := values.ValidateDigest(input.Graph.Digest); err != nil {
		return fmt.Errorf("graph digest: %w", err)
	}
	calculated, err := digestGraph(input.Graph)
	if err != nil || calculated != input.Graph.Digest {
		return fmt.Errorf("graph digest does not match graph semantics")
	}
	if input.Definition.Kind != "workflow" || input.Definition.ID != input.Graph.ID || input.Definition.Version != input.Graph.Version || input.Definition.Digest != input.Graph.Digest {
		return fmt.Errorf("definition identity does not match graph identity")
	}
	if input.Definition.Provenance == nil || !reflect.DeepEqual(*input.Definition.Provenance, input.Graph.Provenance) || input.Graph.Provenance.Digest != input.Graph.Digest {
		return fmt.Errorf("definition and graph provenance must match the immutable digest")
	}
	for name := range input.InputBindings {
		if err := graph.ValidateID(name); err != nil {
			return fmt.Errorf("input binding %q: %w", name, err)
		}
	}
	return nil
}

func validateRequestedBundleRef(ref graph.DefinitionRef) error {
	if ref.Kind != "workflow" || !stableBundleText(ref.Authority) || !stableBundleText(ref.Locator) || !stableBundleText(ref.Version) {
		return fmt.Errorf("exact authority, kind, locator, and version are required")
	}
	if err := graph.ValidateID(ref.ID); err != nil {
		return fmt.Errorf("definition id: %w", err)
	}
	if err := values.ValidateDigest(ref.Digest); err != nil {
		return fmt.Errorf("definition digest: %w", err)
	}
	return nil
}

func bundledDefinitionKey(ref graph.DefinitionRef) string {
	encoded, _ := json.Marshal([6]string{ref.Authority, ref.Kind, ref.ID, ref.Locator, ref.Version, ref.Digest})
	return string(encoded)
}

func stableBundleText(value string) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
		return false
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return false
		}
	}
	return true
}
