// Package testgen generates test input fixtures from blueprint input definitions.
package testgen

import (
	"fmt"
	"strings"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

// FixtureSet holds generated test fixtures for a blueprint.
type FixtureSet struct {
	Valid    map[string]any   // All inputs with valid values
	Boundary []map[string]any // Boundary cases (min, max, edge values)
	Invalid  []InvalidCase    // Invalid inputs that should fail validation
}

// InvalidCase represents an input set that should fail validation, along with a reason.
type InvalidCase struct {
	Inputs map[string]any `json:"inputs" yaml:"inputs"`
	Reason string         `json:"reason" yaml:"reason"`
}

// GenerateFixtures produces a FixtureSet from a blueprint's input definitions.
func GenerateFixtures(bp *blueprint.Blueprint) (*FixtureSet, error) {
	if bp == nil {
		return nil, fmt.Errorf("blueprint is nil")
	}

	if len(bp.Inputs) == 0 {
		return &FixtureSet{
			Valid:    map[string]any{},
			Boundary: nil,
			Invalid:  nil,
		}, nil
	}

	fs := &FixtureSet{
		Valid: make(map[string]any, len(bp.Inputs)),
	}

	for _, inp := range bp.Inputs {
		fs.Valid[inp.Name] = validValue(inp)
	}

	// Generate boundary cases — one fixture per boundary value per input.
	for _, inp := range bp.Inputs {
		boundaries := boundaryValues(inp)
		for _, bv := range boundaries {
			fixture := copyValid(fs.Valid)
			fixture[inp.Name] = bv
			fs.Boundary = append(fs.Boundary, fixture)
		}
	}

	// Generate invalid cases — one fixture per invalid scenario per input.
	for _, inp := range bp.Inputs {
		invalids := invalidCases(inp, fs.Valid)
		fs.Invalid = append(fs.Invalid, invalids...)
	}

	return fs, nil
}

// validValue returns a single valid value for the given input.
func validValue(inp blueprint.Input) any {
	switch inp.Type {
	case "string":
		if inp.Default != nil {
			return inp.Default
		}
		if len(inp.Enum) > 0 {
			return fmt.Sprintf("%v", inp.Enum[0])
		}
		if inp.Pattern != "" {
			return patternPlaceholder(inp)
		}
		// Respect min_length if set.
		if inp.MinLength != nil && *inp.MinLength > len("test-value") {
			return strings.Repeat("a", *inp.MinLength)
		}
		return "test-value"

	case "number":
		if inp.Default != nil {
			return inp.Default
		}
		if len(inp.Enum) > 0 {
			return inp.Enum[0]
		}
		// If min is set and 42 < min, use min.
		if inp.Min != nil && 42 < *inp.Min {
			return *inp.Min
		}
		// If max is set and 42 > max, use max.
		if inp.Max != nil && 42 > *inp.Max {
			return *inp.Max
		}
		return 42

	case "boolean":
		if inp.Default != nil {
			return inp.Default
		}
		return true

	case "array":
		if inp.Default != nil {
			return inp.Default
		}
		placeholder := itemPlaceholder(inp.ItemsType)
		return []any{placeholder}

	default:
		return nil
	}
}

// patternPlaceholder returns a basic placeholder for simple patterns.
func patternPlaceholder(inp blueprint.Input) string {
	// For patterns starting with ^[a-zA-Z], produce an alphabetic string.
	s := "test-value"
	if inp.MinLength != nil && len(s) < *inp.MinLength {
		s = strings.Repeat("a", *inp.MinLength)
	}
	if inp.MaxLength != nil && len(s) > *inp.MaxLength {
		s = s[:*inp.MaxLength]
	}
	return s
}

// itemPlaceholder returns a placeholder value for the given items_type.
func itemPlaceholder(itemsType string) any {
	switch itemsType {
	case "string":
		return "item-1"
	case "number":
		return 1
	case "boolean":
		return true
	default:
		return "item-1"
	}
}

// boundaryValues returns boundary-case values for the input (valid boundary values only).
func boundaryValues(inp blueprint.Input) []any {
	var out []any

	switch inp.Type {
	case "string":
		if inp.MinLength != nil {
			out = append(out, strings.Repeat("a", *inp.MinLength))
		}
		if inp.MaxLength != nil {
			out = append(out, strings.Repeat("a", *inp.MaxLength))
		}

	case "number":
		if inp.Min != nil {
			out = append(out, *inp.Min)
		}
		if inp.Max != nil {
			out = append(out, *inp.Max)
		}

	case "boolean":
		out = append(out, true, false)

	case "array":
		// Empty array and single-item array.
		out = append(out, []any{})
		out = append(out, []any{itemPlaceholder(inp.ItemsType)})
	}

	return out
}

// invalidCases returns invalid input fixtures for the given input.
func invalidCases(inp blueprint.Input, validBase map[string]any) []InvalidCase {
	var out []InvalidCase

	add := func(val any, reason string) {
		fixture := copyValid(validBase)
		fixture[inp.Name] = val
		out = append(out, InvalidCase{
			Inputs: fixture,
			Reason: fmt.Sprintf("%s: %s", inp.Name, reason),
		})
	}

	addMissing := func(reason string) {
		fixture := copyValid(validBase)
		delete(fixture, inp.Name)
		out = append(out, InvalidCase{
			Inputs: fixture,
			Reason: fmt.Sprintf("%s: %s", inp.Name, reason),
		})
	}

	switch inp.Type {
	case "string":
		if inp.Required && inp.Default == nil {
			addMissing("required input missing")
		}
		if inp.MinLength != nil && *inp.MinLength > 0 {
			add(strings.Repeat("a", *inp.MinLength-1), fmt.Sprintf("below min_length (%d)", *inp.MinLength))
		}
		if inp.MaxLength != nil {
			add(strings.Repeat("a", *inp.MaxLength+1), fmt.Sprintf("above max_length (%d)", *inp.MaxLength))
		}
		// Wrong type: number instead of string.
		add(12345, "wrong type (number instead of string)")

		// If enum is set, add a non-enum value.
		if len(inp.Enum) > 0 {
			add("__invalid_enum_value__", "value not in enum")
		}

	case "number":
		if inp.Required && inp.Default == nil {
			addMissing("required input missing")
		}
		if inp.Min != nil {
			add(*inp.Min-1, fmt.Sprintf("below min (%v)", *inp.Min))
		}
		if inp.Max != nil {
			add(*inp.Max+1, fmt.Sprintf("above max (%v)", *inp.Max))
		}
		// Wrong type: string instead of number.
		add("not-a-number", "wrong type (string instead of number)")

		if len(inp.Enum) > 0 {
			add(999999, "value not in enum")
		}

	case "boolean":
		// Wrong types.
		add("true", "wrong type (string instead of boolean)")
		add(1, "wrong type (number instead of boolean)")

	case "array":
		// Wrong type: string instead of array.
		add("not-an-array", "wrong type (string instead of array)")
		// Array with wrong items_type.
		if inp.ItemsType == "string" {
			add([]any{12345}, "array with wrong items_type (number instead of string)")
		} else if inp.ItemsType == "number" {
			add([]any{"wrong"}, "array with wrong items_type (string instead of number)")
		}
	}

	return out
}

// copyValid makes a shallow copy of a map.
func copyValid(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
