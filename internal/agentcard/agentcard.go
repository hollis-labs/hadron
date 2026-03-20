package agentcard

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

// ── A2A Agent Card Types ──────────────────────────────────────────────────────

// AgentCard represents an A2A-compatible agent card served at /.well-known/agent.json.
type AgentCard struct {
	Name               string       `json:"name"`
	Description        string       `json:"description"`
	URL                string       `json:"url"`
	Provider           Provider     `json:"provider"`
	Version            string       `json:"version"`
	Capabilities       Capabilities `json:"capabilities"`
	DefaultInputModes  []string     `json:"defaultInputModes"`
	DefaultOutputModes []string     `json:"defaultOutputModes"`
	Skills             []Skill      `json:"skills"`
}

// Provider identifies the organization providing the agent.
type Provider struct {
	Organization string `json:"organization"`
}

// Capabilities describes what the agent supports.
type Capabilities struct {
	Streaming         bool `json:"streaming"`
	PushNotifications bool `json:"pushNotifications"`
}

// Skill represents a single capability derived from a blueprint.
type Skill struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Tags        []string    `json:"tags"`
	InputSchema InputSchema `json:"inputSchema"`
}

// InputSchema is a JSON Schema object describing inputs.
type InputSchema struct {
	Type       string                    `json:"type"`
	Properties map[string]SchemaProperty `json:"properties"`
	Required   []string                  `json:"required,omitempty"`
}

// SchemaProperty describes one JSON Schema property.
type SchemaProperty struct {
	Type        string       `json:"type"`
	Description string       `json:"description,omitempty"`
	Pattern     string       `json:"pattern,omitempty"`
	MinLength   *int         `json:"minLength,omitempty"`
	MaxLength   *int         `json:"maxLength,omitempty"`
	Minimum     *float64     `json:"minimum,omitempty"`
	Maximum     *float64     `json:"maximum,omitempty"`
	Enum        []any        `json:"enum,omitempty"`
	Items       *SchemaItems `json:"items,omitempty"`
}

// SchemaItems describes the items constraint for array types.
type SchemaItems struct {
	Type string `json:"type"`
}

// ── Construction ──────────────────────────────────────────────────────────────

// SkillFromBlueprint generates a Skill from a single parsed blueprint.
// The path argument is used only for fallback ID generation when the blueprint
// has no slug or name.
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

	desc := bp.Spec.Description

	tags := bp.Spec.Tags
	if tags == nil {
		tags = []string{}
	}

	props := make(map[string]SchemaProperty, len(bp.Inputs))
	var required []string

	for _, inp := range bp.Inputs {
		prop := inputToSchemaProperty(inp)
		props[inp.Name] = prop
		if inp.Required {
			required = append(required, inp.Name)
		}
	}

	return Skill{
		ID:          id,
		Name:        name,
		Description: desc,
		Tags:        tags,
		InputSchema: InputSchema{
			Type:       "object",
			Properties: props,
			Required:   required,
		},
	}
}

// FromDirectory scans a directory for blueprint YAML files and builds a
// composite AgentCard with all discovered blueprints as skills.
func FromDirectory(dir string, baseURL string) (*AgentCard, error) {
	if baseURL == "" {
		baseURL = "http://localhost:8095"
	}

	var skills []Skill

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		ext := filepath.Ext(d.Name())
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}
		bp, parseErr := blueprint.ParseFile(path)
		if parseErr != nil {
			return nil // skip invalid files
		}
		skills = append(skills, SkillFromBlueprint(bp, path))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk blueprint directory: %w", err)
	}

	if skills == nil {
		skills = []Skill{}
	}

	return &AgentCard{
		Name:        "Hadron Automation",
		Description: "Local-first blueprint automation runner",
		URL:         baseURL,
		Provider:    Provider{Organization: "Hadron"},
		Version:     "0.4.0",
		Capabilities: Capabilities{
			Streaming:         false,
			PushNotifications: false,
		},
		DefaultInputModes:  []string{"application/json"},
		DefaultOutputModes: []string{"application/json"},
		Skills:             skills,
	}, nil
}

// JSON returns the agent card as pretty-printed JSON bytes.
func (c *AgentCard) JSON() ([]byte, error) {
	return json.MarshalIndent(c, "", "  ")
}

// ── Input → JSON Schema mapping ───────────────────────────────────────────────

func inputToSchemaProperty(inp blueprint.Input) SchemaProperty {
	prop := SchemaProperty{
		Type:        mapInputType(inp.Type),
		Description: inp.Description,
	}

	if inp.Pattern != "" {
		prop.Pattern = inp.Pattern
	}
	if inp.MinLength != nil {
		prop.MinLength = inp.MinLength
	}
	if inp.MaxLength != nil {
		prop.MaxLength = inp.MaxLength
	}
	if inp.Min != nil {
		prop.Minimum = inp.Min
	}
	if inp.Max != nil {
		prop.Maximum = inp.Max
	}
	if len(inp.Enum) > 0 {
		prop.Enum = inp.Enum
	}
	if inp.Type == "array" && inp.ItemsType != "" {
		prop.Items = &SchemaItems{Type: mapInputType(inp.ItemsType)}
	}

	return prop
}

func mapInputType(t string) string {
	switch t {
	case "string":
		return "string"
	case "number":
		return "number"
	case "boolean":
		return "boolean"
	case "array":
		return "array"
	default:
		return "string"
	}
}
