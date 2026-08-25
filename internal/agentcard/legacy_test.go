package agentcard

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

func TestLegacySkillPreservesInputConstraintsAndRequiredFields(t *testing.T) {
	minimum, maximum := 2, 40
	numberMinimum, numberMaximum := 1.0, 8.0
	bp := &blueprint.Blueprint{
		Version: "0.4",
		Spec:    blueprint.BlueprintInfo{Name: "legacy", Slug: "legacy-skill", Title: "Legacy Skill"},
		Inputs: []blueprint.Input{
			{Name: "name", Type: "string", Required: true, Pattern: "^[a-z]+$", MinLength: &minimum, MaxLength: &maximum, Enum: []any{"alpha", "beta"}},
			{Name: "workers", Type: "number", Min: &numberMinimum, Max: &numberMaximum},
			{Name: "tags", Type: "array", ItemsType: "string"},
		},
	}
	skill := SkillFromBlueprint(bp, "ignored.yaml")
	properties, ok := skill.InputSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("legacy properties = %#v", skill.InputSchema["properties"])
	}
	name, ok := properties["name"].(map[string]any)
	if !ok || name["type"] != "string" || name["pattern"] != "^[a-z]+$" || name["minLength"] != minimum || name["maxLength"] != maximum || !reflect.DeepEqual(name["enum"], []any{"alpha", "beta"}) {
		t.Fatalf("legacy string schema = %#v", name)
	}
	workers, ok := properties["workers"].(map[string]any)
	if !ok || workers["minimum"] != numberMinimum || workers["maximum"] != numberMaximum {
		t.Fatalf("legacy number schema = %#v", workers)
	}
	tags, ok := properties["tags"].(map[string]any)
	items, itemsOK := tags["items"].(map[string]any)
	if !ok || !itemsOK || items["type"] != "string" {
		t.Fatalf("legacy array schema = %#v", tags)
	}
	if required, ok := skill.InputSchema["required"].([]string); !ok || !reflect.DeepEqual(required, []string{"name"}) {
		t.Fatalf("legacy required fields = %#v", skill.InputSchema["required"])
	}
}

func TestLegacyDirectoryUsesDefaultURLAndProducesJSON(t *testing.T) {
	dir := t.TempDir()
	fixture := []byte(`version: "0.4"
blueprint:
  name: legacy-directory
  slug: legacy-directory
  title: Legacy Directory
steps:
  - section: Test
    tasks:
      - name: hello
        cmd: echo hello
`)
	if err := os.WriteFile(filepath.Join(dir, "legacy.yaml"), fixture, 0o600); err != nil {
		t.Fatal(err)
	}
	card, err := FromDirectory(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if card.URL != "http://localhost:8095" || len(card.Skills) != 1 || card.Skills[0].ID != "legacy-directory" {
		t.Fatalf("legacy directory card = %#v", card)
	}
	encoded, err := card.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip AgentCard
	if err := json.Unmarshal(encoded, &roundTrip); err != nil || len(roundTrip.Skills) != 1 {
		t.Fatalf("legacy card JSON = %#v, %v", roundTrip, err)
	}
}
