package agentcard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/workflow/graph"
)

// SkillFromBlueprint remains only for the legacy CLI/MCP blueprint discovery
// commands pending W06-T06 quarantine. The daemon A2A surface never calls it.
func SkillFromBlueprint(bp *blueprint.Blueprint, path string) Skill {
	id := bp.Spec.Slug
	if id == "" {
		id = bp.Spec.Name
	}
	if id == "" {
		id = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	name := bp.Spec.Title
	if name == "" {
		name = bp.Spec.Name
	}
	if name == "" {
		name = id
	}
	properties := make(map[string]any, len(bp.Inputs))
	required := make([]string, 0, len(bp.Inputs))
	for _, input := range bp.Inputs {
		property := graph.Schema{"type": legacyInputType(input.Type)}
		if input.Description != "" {
			property["description"] = input.Description
		}
		if input.Pattern != "" {
			property["pattern"] = input.Pattern
		}
		if input.MinLength != nil {
			property["minLength"] = *input.MinLength
		}
		if input.MaxLength != nil {
			property["maxLength"] = *input.MaxLength
		}
		if input.Min != nil {
			property["minimum"] = *input.Min
		}
		if input.Max != nil {
			property["maximum"] = *input.Max
		}
		if len(input.Enum) > 0 {
			property["enum"] = append([]any(nil), input.Enum...)
		}
		if input.Type == "array" && input.ItemsType != "" {
			property["items"] = map[string]any{"type": legacyInputType(input.ItemsType)}
		}
		properties[input.Name] = map[string]any(property)
		if input.Required {
			required = append(required, input.Name)
		}
	}
	inputSchema := graph.Schema{"type": "object", "properties": properties}
	if len(required) != 0 {
		inputSchema["required"] = required
	}
	tags := append([]string(nil), bp.Spec.Tags...)
	if tags == nil {
		tags = []string{}
	}
	return Skill{ID: id, Name: name, Description: bp.Spec.Description, Tags: tags, InputSchema: inputSchema, OutputSchema: graph.Schema{"type": "object"}, Effects: graph.EffectSet{}}
}

// FromDirectory remains a legacy authoring helper. The daemon agent card is
// always built from PublishedWorkflows instead.
func FromDirectory(dir, baseURL string) (*AgentCard, error) {
	if baseURL == "" {
		baseURL = "http://localhost:8095"
	}
	skills := []Skill{}
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if path == dir {
				return walkErr
			}
			return nil
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yaml" && filepath.Ext(entry.Name()) != ".yml" {
			return nil
		}
		bp, ok := parseLegacyBlueprint(path)
		if !ok {
			return nil
		}
		skills = append(skills, SkillFromBlueprint(bp, path))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk blueprint directory: %w", err)
	}
	return &AgentCard{
		Name: "Hadron Automation", Description: "Local-first blueprint automation runner", URL: baseURL,
		Provider: Provider{Organization: "Hadron"}, Version: "0.4.0",
		Capabilities: Capabilities{}, DefaultInputModes: []string{"application/json"},
		DefaultOutputModes: []string{"application/json"}, Skills: skills,
	}, nil
}

func parseLegacyBlueprint(path string) (*blueprint.Blueprint, bool) {
	bp, err := blueprint.ParseFile(path)
	return bp, err == nil
}

func legacyInputType(input string) string {
	switch input {
	case "string", "number", "boolean", "array":
		return input
	default:
		return "string"
	}
}
