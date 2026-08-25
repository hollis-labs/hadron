package agentcard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/workflow/graph"
)

const MaximumAgentCardBytes = 1 << 20

// AgentCard represents Hadron's bounded A2A discovery document.
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

type Provider struct {
	Organization string `json:"organization"`
}

type Capabilities struct {
	Streaming         bool `json:"streaming"`
	PushNotifications bool `json:"pushNotifications"`
}

// Skill carries the exact published registry identity and canonical workflow
// contract. Raw source locators and arbitrary provenance metadata are omitted.
type Skill struct {
	ID           string                                 `json:"id"`
	Name         string                                 `json:"name"`
	Description  string                                 `json:"description"`
	Tags         []string                               `json:"tags"`
	Definition   graph.DefinitionRef                    `json:"definition"`
	Provenance   appworkflow.WorkflowExposureProvenance `json:"provenance"`
	Effects      graph.EffectSet                        `json:"effects"`
	InputSchema  graph.Schema                           `json:"inputSchema"`
	OutputSchema graph.Schema                           `json:"outputSchema"`
}

type PublishedWorkflowSource interface {
	PublishedWorkflows(context.Context) ([]appworkflow.WorkflowExposureDescriptor, error)
}

type Builder struct {
	source PublishedWorkflowSource
}

func NewBuilder(source PublishedWorkflowSource) (*Builder, error) {
	if nilInterface(source) {
		return nil, errors.New("published workflow source is required")
	}
	return &Builder{source: source}, nil
}

// Card derives one immutable snapshot from published graph registry records.
func (b *Builder) Card(ctx context.Context, baseURL string) (*AgentCard, error) {
	if ctx == nil || b == nil || nilInterface(b.source) {
		return nil, errors.New("agent-card builder is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if baseURL == "" {
		baseURL = "http://localhost:8095"
	}
	descriptors, err := b.source.PublishedWorkflows(ctx)
	if err != nil {
		return nil, err
	}
	skills := make([]Skill, len(descriptors))
	for index, descriptor := range descriptors {
		input, cloneErr := cloneSchema(descriptor.InputSchema)
		if cloneErr != nil {
			return nil, fmt.Errorf("clone workflow input schema: %w", cloneErr)
		}
		output, cloneErr := cloneSchema(descriptor.OutputSchema)
		if cloneErr != nil {
			return nil, fmt.Errorf("clone workflow output schema: %w", cloneErr)
		}
		description := descriptor.Description
		if description == "" {
			description = "Start the published durable workflow " + descriptor.Name + "."
		}
		skills[index] = Skill{
			ID:   descriptor.Name + "@" + descriptor.Version + "@" + descriptor.Digest,
			Name: descriptor.Name, Description: description,
			Tags: append([]string(nil), descriptor.Tags...), Definition: descriptor.Definition,
			Provenance: descriptor.Provenance, Effects: append(graph.EffectSet(nil), descriptor.Effects...),
			InputSchema: input, OutputSchema: output,
		}
	}
	card := &AgentCard{
		Name: "Hadron Workflows", Description: "Published graph-native durable workflows", URL: baseURL,
		Provider: Provider{Organization: "Hadron"}, Version: "1.0.0",
		Capabilities:      Capabilities{Streaming: false, PushNotifications: false},
		DefaultInputModes: []string{"application/json"}, DefaultOutputModes: []string{"application/json"},
		Skills: skills,
	}
	if encoded, marshalErr := json.Marshal(card); marshalErr != nil || len(encoded) > MaximumAgentCardBytes {
		return nil, errors.New("agent card exceeds the supported response bound")
	}
	return card, nil
}

func (c *AgentCard) JSON() ([]byte, error) {
	encoded, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaximumAgentCardBytes {
		return nil, errors.New("agent card exceeds the supported response bound")
	}
	return encoded, nil
}

func cloneSchema(input graph.Schema) (graph.Schema, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result graph.Schema
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("workflow schema has trailing data")
	}
	return result, nil
}

func nilInterface(input any) bool {
	if input == nil {
		return true
	}
	value := reflect.ValueOf(input)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
