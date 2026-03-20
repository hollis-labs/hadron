package testgen

import (
	"testing"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

// helper to build a minimal valid blueprint with given inputs.
func minimalBP(inputs []blueprint.Input) *blueprint.Blueprint {
	return &blueprint.Blueprint{
		Version: "0.4",
		Spec:    blueprint.BlueprintInfo{Name: "test-bp"},
		Inputs:  inputs,
		Steps: []blueprint.Section{
			{
				Section: "test",
				Steps:   []blueprint.Step{{Name: "noop", Cmd: "echo ok"}},
			},
		},
	}
}

func intPtr(n int) *int           { return &n }
func floatPtr(f float64) *float64 { return &f }

func TestStringInput(t *testing.T) {
	bp := minimalBP([]blueprint.Input{
		{
			Name:      "app_name",
			Type:      "string",
			Required:  true,
			MinLength: intPtr(3),
			MaxLength: intPtr(20),
			Pattern:   "^[a-zA-Z][a-zA-Z0-9-]*$",
		},
	})

	fs, err := GenerateFixtures(bp)
	if err != nil {
		t.Fatalf("GenerateFixtures: %v", err)
	}

	// Valid value should be a string.
	v, ok := fs.Valid["app_name"].(string)
	if !ok {
		t.Fatalf("expected string, got %T", fs.Valid["app_name"])
	}
	if len(v) < 3 || len(v) > 20 {
		t.Errorf("valid value length %d not in [3,20]", len(v))
	}

	// Should have boundary cases: min_length string, max_length string.
	if len(fs.Boundary) < 2 {
		t.Fatalf("expected at least 2 boundary cases, got %d", len(fs.Boundary))
	}
	b0 := fs.Boundary[0]["app_name"].(string)
	if len(b0) != 3 {
		t.Errorf("boundary[0] length: want 3, got %d", len(b0))
	}
	b1 := fs.Boundary[1]["app_name"].(string)
	if len(b1) != 20 {
		t.Errorf("boundary[1] length: want 20, got %d", len(b1))
	}

	// Should have invalid cases: missing required, below min_length, above max_length, wrong type.
	if len(fs.Invalid) < 4 {
		t.Fatalf("expected at least 4 invalid cases, got %d", len(fs.Invalid))
	}

	// Valid fixture must pass NormalizeInputs.
	_, normErr := blueprint.NormalizeInputs(bp, fs.Valid)
	if normErr != nil {
		t.Errorf("valid fixture failed NormalizeInputs: %v", normErr)
	}
}

func TestNumberInput(t *testing.T) {
	bp := minimalBP([]blueprint.Input{
		{
			Name: "count",
			Type: "number",
			Min:  floatPtr(1),
			Max:  floatPtr(100),
		},
	})

	fs, err := GenerateFixtures(bp)
	if err != nil {
		t.Fatalf("GenerateFixtures: %v", err)
	}

	// Valid should be 42 (within range).
	if fs.Valid["count"] != 42 {
		t.Errorf("expected 42, got %v", fs.Valid["count"])
	}

	// Boundary: min=1, max=100.
	if len(fs.Boundary) < 2 {
		t.Fatalf("expected at least 2 boundary cases, got %d", len(fs.Boundary))
	}
	if fs.Boundary[0]["count"] != float64(1) {
		t.Errorf("boundary[0]: want 1, got %v", fs.Boundary[0]["count"])
	}
	if fs.Boundary[1]["count"] != float64(100) {
		t.Errorf("boundary[1]: want 100, got %v", fs.Boundary[1]["count"])
	}

	// Invalid: below min (0), above max (101), wrong type.
	if len(fs.Invalid) < 3 {
		t.Fatalf("expected at least 3 invalid cases, got %d", len(fs.Invalid))
	}

	// Valid fixture must pass NormalizeInputs.
	_, normErr := blueprint.NormalizeInputs(bp, fs.Valid)
	if normErr != nil {
		t.Errorf("valid fixture failed NormalizeInputs: %v", normErr)
	}
}

func TestEnumInput(t *testing.T) {
	bp := minimalBP([]blueprint.Input{
		{
			Name:    "env",
			Type:    "string",
			Default: "dev",
			Enum:    []any{"dev", "staging", "prod"},
		},
	})

	fs, err := GenerateFixtures(bp)
	if err != nil {
		t.Fatalf("GenerateFixtures: %v", err)
	}

	// Valid should use default "dev".
	if fs.Valid["env"] != "dev" {
		t.Errorf("expected 'dev', got %v", fs.Valid["env"])
	}

	// Should have an invalid case for non-enum value.
	found := false
	for _, ic := range fs.Invalid {
		if ic.Inputs["env"] == "__invalid_enum_value__" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected an invalid case with non-enum value")
	}

	// Valid fixture must pass NormalizeInputs.
	_, normErr := blueprint.NormalizeInputs(bp, fs.Valid)
	if normErr != nil {
		t.Errorf("valid fixture failed NormalizeInputs: %v", normErr)
	}
}

func TestRequiredInputNoDefault(t *testing.T) {
	bp := minimalBP([]blueprint.Input{
		{
			Name:     "name",
			Type:     "string",
			Required: true,
		},
	})

	fs, err := GenerateFixtures(bp)
	if err != nil {
		t.Fatalf("GenerateFixtures: %v", err)
	}

	// Valid should have a placeholder.
	v, ok := fs.Valid["name"].(string)
	if !ok || v == "" {
		t.Errorf("expected non-empty placeholder, got %v", fs.Valid["name"])
	}

	// Should have an invalid case for missing required.
	found := false
	for _, ic := range fs.Invalid {
		if _, exists := ic.Inputs["name"]; !exists {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected an invalid case with missing required input")
	}

	// Valid fixture must pass NormalizeInputs.
	_, normErr := blueprint.NormalizeInputs(bp, fs.Valid)
	if normErr != nil {
		t.Errorf("valid fixture failed NormalizeInputs: %v", normErr)
	}
}

func TestBooleanInput(t *testing.T) {
	bp := minimalBP([]blueprint.Input{
		{
			Name:    "debug",
			Type:    "boolean",
			Default: false,
		},
	})

	fs, err := GenerateFixtures(bp)
	if err != nil {
		t.Fatalf("GenerateFixtures: %v", err)
	}

	// Valid should use default false.
	if fs.Valid["debug"] != false {
		t.Errorf("expected false, got %v", fs.Valid["debug"])
	}

	// Boundary: true and false.
	if len(fs.Boundary) < 2 {
		t.Fatalf("expected at least 2 boundary cases, got %d", len(fs.Boundary))
	}

	// Invalid: string "true", number 1.
	if len(fs.Invalid) < 2 {
		t.Fatalf("expected at least 2 invalid cases, got %d", len(fs.Invalid))
	}

	// Valid fixture must pass NormalizeInputs.
	_, normErr := blueprint.NormalizeInputs(bp, fs.Valid)
	if normErr != nil {
		t.Errorf("valid fixture failed NormalizeInputs: %v", normErr)
	}
}

func TestArrayInput(t *testing.T) {
	bp := minimalBP([]blueprint.Input{
		{
			Name:      "tags",
			Type:      "array",
			ItemsType: "string",
		},
	})

	fs, err := GenerateFixtures(bp)
	if err != nil {
		t.Fatalf("GenerateFixtures: %v", err)
	}

	// Valid should be a slice with one string item.
	arr, ok := fs.Valid["tags"].([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", fs.Valid["tags"])
	}
	if len(arr) != 1 {
		t.Errorf("expected 1 item, got %d", len(arr))
	}

	// Boundary: empty array, single-item array.
	if len(fs.Boundary) < 2 {
		t.Fatalf("expected at least 2 boundary cases, got %d", len(fs.Boundary))
	}

	// Invalid: string instead of array, wrong items_type.
	if len(fs.Invalid) < 2 {
		t.Fatalf("expected at least 2 invalid cases, got %d", len(fs.Invalid))
	}

	// Valid fixture must pass NormalizeInputs.
	_, normErr := blueprint.NormalizeInputs(bp, fs.Valid)
	if normErr != nil {
		t.Errorf("valid fixture failed NormalizeInputs: %v", normErr)
	}
}

func TestNoInputs(t *testing.T) {
	bp := minimalBP(nil)

	fs, err := GenerateFixtures(bp)
	if err != nil {
		t.Fatalf("GenerateFixtures: %v", err)
	}

	if len(fs.Valid) != 0 {
		t.Errorf("expected empty valid map, got %d entries", len(fs.Valid))
	}
	if len(fs.Boundary) != 0 {
		t.Errorf("expected no boundary cases, got %d", len(fs.Boundary))
	}
	if len(fs.Invalid) != 0 {
		t.Errorf("expected no invalid cases, got %d", len(fs.Invalid))
	}
}

func TestNilBlueprint(t *testing.T) {
	_, err := GenerateFixtures(nil)
	if err == nil {
		t.Error("expected error for nil blueprint")
	}
}

// TestParameterizedBlueprint tests against the actual parameterized.yaml example.
func TestParameterizedBlueprint(t *testing.T) {
	bp, err := blueprint.ParseFile("../../examples/parameterized.yaml")
	if err != nil {
		t.Skipf("could not load parameterized.yaml: %v", err)
	}

	fs, err := GenerateFixtures(bp)
	if err != nil {
		t.Fatalf("GenerateFixtures: %v", err)
	}

	// Valid fixtures must pass NormalizeInputs.
	_, normErr := blueprint.NormalizeInputs(bp, fs.Valid)
	if normErr != nil {
		t.Errorf("valid fixture failed NormalizeInputs: %v", normErr)
	}

	// All boundary fixtures should also pass NormalizeInputs (they are valid boundary values).
	for i, bfix := range fs.Boundary {
		_, bErr := blueprint.NormalizeInputs(bp, bfix)
		if bErr != nil {
			t.Errorf("boundary[%d] failed NormalizeInputs: %v", i, bErr)
		}
	}

	// All invalid fixtures should fail NormalizeInputs.
	for i, ic := range fs.Invalid {
		_, iErr := blueprint.NormalizeInputs(bp, ic.Inputs)
		if iErr == nil {
			t.Errorf("invalid[%d] (%s) passed NormalizeInputs but should have failed", i, ic.Reason)
		}
	}
}
