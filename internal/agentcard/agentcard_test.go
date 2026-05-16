package agentcard

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func intPtr(v int) *int           { return &v }
func floatPtr(v float64) *float64 { return &v }

func minimalBlueprint(name string) *blueprint.Blueprint {
	return &blueprint.Blueprint{
		Version: "0.4",
		Spec:    blueprint.BlueprintInfo{Name: name},
		Steps: []blueprint.Section{{
			Section: "Init",
			Steps:   []blueprint.Step{{Name: "echo", Cmd: "echo ok"}},
		}},
	}
}

// ── SkillFromBlueprint tests ──────────────────────────────────────────────────

func TestSkillFromBlueprint_StringInput(t *testing.T) {
	bp := minimalBlueprint("test-bp")
	bp.Inputs = []blueprint.Input{
		{
			Name:        "app_name",
			Description: "App name",
			Type:        "string",
			Required:    true,
			Pattern:     "^[a-z]+$",
			MinLength:   intPtr(2),
			MaxLength:   intPtr(50),
		},
	}

	skill := SkillFromBlueprint(bp, "test.yaml")

	if skill.ID != "test-bp" {
		t.Fatalf("expected id test-bp, got %q", skill.ID)
	}
	if skill.InputSchema.Type != "object" {
		t.Fatalf("expected schema type object, got %q", skill.InputSchema.Type)
	}
	prop, ok := skill.InputSchema.Properties["app_name"]
	if !ok {
		t.Fatal("missing app_name property")
	}
	if prop.Type != "string" {
		t.Fatalf("expected type string, got %q", prop.Type)
	}
	if prop.Pattern != "^[a-z]+$" {
		t.Fatalf("expected pattern ^[a-z]+$, got %q", prop.Pattern)
	}
	if prop.MinLength == nil || *prop.MinLength != 2 {
		t.Fatal("expected minLength 2")
	}
	if prop.MaxLength == nil || *prop.MaxLength != 50 {
		t.Fatal("expected maxLength 50")
	}
}

func TestSkillFromBlueprint_RequiredInputs(t *testing.T) {
	bp := minimalBlueprint("req-test")
	bp.Inputs = []blueprint.Input{
		{Name: "required_one", Type: "string", Required: true},
		{Name: "optional_one", Type: "string", Required: false},
		{Name: "required_two", Type: "number", Required: true},
	}

	skill := SkillFromBlueprint(bp, "test.yaml")

	if len(skill.InputSchema.Required) != 2 {
		t.Fatalf("expected 2 required inputs, got %d", len(skill.InputSchema.Required))
	}
	reqMap := map[string]bool{}
	for _, r := range skill.InputSchema.Required {
		reqMap[r] = true
	}
	if !reqMap["required_one"] || !reqMap["required_two"] {
		t.Fatalf("wrong required list: %v", skill.InputSchema.Required)
	}
	if reqMap["optional_one"] {
		t.Fatal("optional_one should not be in required list")
	}
}

func TestSkillFromBlueprint_EnumInput(t *testing.T) {
	bp := minimalBlueprint("enum-test")
	bp.Inputs = []blueprint.Input{
		{
			Name: "env",
			Type: "string",
			Enum: []any{"dev", "staging", "prod"},
		},
	}

	skill := SkillFromBlueprint(bp, "test.yaml")

	prop := skill.InputSchema.Properties["env"]
	if len(prop.Enum) != 3 {
		t.Fatalf("expected 3 enum values, got %d", len(prop.Enum))
	}
}

func TestSkillFromBlueprint_NumberConstraints(t *testing.T) {
	bp := minimalBlueprint("number-test")
	bp.Inputs = []blueprint.Input{
		{
			Name:        "workers",
			Description: "Worker count",
			Type:        "number",
			Min:         floatPtr(1),
			Max:         floatPtr(32),
		},
	}

	skill := SkillFromBlueprint(bp, "test.yaml")

	prop := skill.InputSchema.Properties["workers"]
	if prop.Type != "number" {
		t.Fatalf("expected type number, got %q", prop.Type)
	}
	if prop.Minimum == nil || *prop.Minimum != 1 {
		t.Fatal("expected minimum 1")
	}
	if prop.Maximum == nil || *prop.Maximum != 32 {
		t.Fatal("expected maximum 32")
	}
}

func TestSkillFromBlueprint_BooleanInput(t *testing.T) {
	bp := minimalBlueprint("bool-test")
	bp.Inputs = []blueprint.Input{
		{Name: "debug", Type: "boolean", Description: "Enable debug"},
	}

	skill := SkillFromBlueprint(bp, "test.yaml")

	prop := skill.InputSchema.Properties["debug"]
	if prop.Type != "boolean" {
		t.Fatalf("expected type boolean, got %q", prop.Type)
	}
}

func TestSkillFromBlueprint_ArrayInput(t *testing.T) {
	bp := minimalBlueprint("array-test")
	bp.Inputs = []blueprint.Input{
		{Name: "tags", Type: "array", ItemsType: "string"},
	}

	skill := SkillFromBlueprint(bp, "test.yaml")

	prop := skill.InputSchema.Properties["tags"]
	if prop.Type != "array" {
		t.Fatalf("expected type array, got %q", prop.Type)
	}
	if prop.Items == nil {
		t.Fatal("expected items schema")
	}
	if prop.Items.Type != "string" {
		t.Fatalf("expected items type string, got %q", prop.Items.Type)
	}
}

func TestSkillFromBlueprint_UsesSlugForID(t *testing.T) {
	bp := minimalBlueprint("full-name")
	bp.Spec.Slug = "my-slug"

	skill := SkillFromBlueprint(bp, "test.yaml")
	if skill.ID != "my-slug" {
		t.Fatalf("expected id my-slug, got %q", skill.ID)
	}
}

func TestSkillFromBlueprint_UsesTitleForName(t *testing.T) {
	bp := minimalBlueprint("name-only")
	bp.Spec.Title = "My Cool Blueprint"

	skill := SkillFromBlueprint(bp, "test.yaml")
	if skill.Name != "My Cool Blueprint" {
		t.Fatalf("expected name 'My Cool Blueprint', got %q", skill.Name)
	}
}

func TestSkillFromBlueprint_FallbackIDFromPath(t *testing.T) {
	bp := &blueprint.Blueprint{
		Version: "0.4",
		Steps: []blueprint.Section{{
			Section: "Init",
			Steps:   []blueprint.Step{{Name: "echo", Cmd: "echo ok"}},
		}},
	}
	// Spec.Name and Spec.Slug are both empty

	skill := SkillFromBlueprint(bp, "/some/dir/my-cool-bp.yaml")
	if skill.ID != "my-cool-bp" {
		t.Fatalf("expected id my-cool-bp, got %q", skill.ID)
	}
}

func TestSkillFromBlueprint_NoInputs(t *testing.T) {
	bp := minimalBlueprint("no-inputs")

	skill := SkillFromBlueprint(bp, "test.yaml")
	if len(skill.InputSchema.Properties) != 0 {
		t.Fatalf("expected 0 properties, got %d", len(skill.InputSchema.Properties))
	}
	if len(skill.InputSchema.Required) != 0 {
		t.Fatalf("expected 0 required, got %d", len(skill.InputSchema.Required))
	}
}

// ── FromDirectory tests ───────────────────────────────────────────────────────

func TestFromDirectory_WithExamples(t *testing.T) {
	// Use the project's examples/ directory which contains real blueprints
	examplesDir := filepath.Join("..", "..", "examples")
	if _, err := os.Stat(examplesDir); os.IsNotExist(err) {
		t.Skip("examples/ directory not found")
	}

	card, err := FromDirectory(examplesDir, "http://localhost:9999")
	if err != nil {
		t.Fatalf("FromDirectory: %v", err)
	}
	if card.Name != "Hadron Automation" {
		t.Fatalf("expected name 'Hadron Automation', got %q", card.Name)
	}
	if card.URL != "http://localhost:9999" {
		t.Fatalf("expected URL http://localhost:9999, got %q", card.URL)
	}
	if len(card.Skills) == 0 {
		t.Fatal("expected at least one skill from examples/")
	}

	// Verify it produces valid JSON
	data, err := card.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
	var roundtrip AgentCard
	if err := json.Unmarshal(data, &roundtrip); err != nil {
		t.Fatalf("roundtrip unmarshal: %v", err)
	}
	if len(roundtrip.Skills) != len(card.Skills) {
		t.Fatalf("roundtrip skill count mismatch: %d vs %d", len(roundtrip.Skills), len(card.Skills))
	}
}

func TestFromDirectory_Empty(t *testing.T) {
	dir := t.TempDir()

	card, err := FromDirectory(dir, "")
	if err != nil {
		t.Fatalf("FromDirectory empty: %v", err)
	}
	if len(card.Skills) != 0 {
		t.Fatalf("expected 0 skills, got %d", len(card.Skills))
	}
	if card.URL != "http://localhost:8095" {
		t.Fatalf("expected default URL, got %q", card.URL)
	}
}

func TestFromDirectory_DefaultURL(t *testing.T) {
	dir := t.TempDir()

	card, err := FromDirectory(dir, "")
	if err != nil {
		t.Fatalf("FromDirectory: %v", err)
	}
	if card.URL != "http://localhost:8095" {
		t.Fatalf("expected default URL http://localhost:8095, got %q", card.URL)
	}
}

func TestAgentCard_JSONOutput(t *testing.T) {
	card := &AgentCard{
		Name:        "Test Agent",
		Description: "Test",
		URL:         "http://localhost:8095",
		Provider:    Provider{Organization: "Test"},
		Version:     "0.4.0",
		Capabilities: Capabilities{
			Streaming:         false,
			PushNotifications: false,
		},
		DefaultInputModes:  []string{"application/json"},
		DefaultOutputModes: []string{"application/json"},
		Skills:             []Skill{},
	}

	data, err := card.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed["name"] != "Test Agent" {
		t.Fatalf("expected name 'Test Agent', got %v", parsed["name"])
	}
	caps, _ := parsed["capabilities"].(map[string]any)
	if caps["streaming"] != false {
		t.Fatal("expected streaming false")
	}
}

func TestFromDirectory_BestEffortOnUnreadableSubdir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: directory permission bits are not enforced")
	}
	tmpDir := t.TempDir()

	// A valid blueprint at the root.
	good := `version: "0.4"
blueprint:
  name: good-blueprint
  slug: good-bp
  title: Good Blueprint
  description: A valid peer
  author: Test
  tags: [test]
steps:
  - section: Test
    tasks:
      - name: hello
        cmd: echo hello
`
	if err := os.WriteFile(filepath.Join(tmpDir, "good.yaml"), []byte(good), 0o644); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}

	// An unreadable subdirectory: WalkDir reports a permission error for it.
	// Card generation must skip it, not abort and drop the valid peer.
	locked := filepath.Join(tmpDir, "locked")
	if err := os.Mkdir(locked, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	// Restore perms so t.TempDir cleanup can remove the tree.
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })

	card, err := FromDirectory(tmpDir, "")
	if err != nil {
		t.Fatalf("FromDirectory aborted on an unreadable subdir: %v", err)
	}
	if len(card.Skills) != 1 {
		t.Fatalf("expected the valid blueprint as a skill despite the bad subdir, got %d", len(card.Skills))
	}
}
