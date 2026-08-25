package runtime

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const ContinueAsNewExtensionVersion = "reactor/v1"

// ContinueAsNewPolicy is the typed runtime interpretation of the graph's
// versioned durability extension. Carry names exact same-named output/input
// values handed to the next generation.
type ContinueAsNewPolicy struct {
	MaxEvents int      `json:"max_events"`
	Carry     []string `json:"carry,omitempty"`
}

// EffectiveDurability preserves the durable default for existing plans.
func EffectiveDurability(workflow graph.Graph) graph.DurabilityMode {
	if workflow.Durability == nil || workflow.Durability.Mode == "" {
		return graph.DurabilitySteps
	}
	return workflow.Durability.Mode
}

// ParseContinueAsNew returns the typed extension without accepting unknown or
// partially canonical config.
func ParseContinueAsNew(workflow graph.Graph) (ContinueAsNewPolicy, bool, error) {
	if workflow.Durability == nil || workflow.Durability.Extension.Version == "" {
		return ContinueAsNewPolicy{}, false, nil
	}
	extension := workflow.Durability.Extension
	if extension.Version != ContinueAsNewExtensionVersion || EffectiveDurability(workflow) != graph.DurabilitySteps {
		return ContinueAsNewPolicy{}, true, fmt.Errorf("%w: unsupported continue-as-new durability extension", ErrInvalidRecord)
	}
	maxEvents, ok := runtimeDurabilityInteger(extension.Config["max_events"])
	carry, carryOK := runtimeDurabilityStrings(extension.Config["carry"])
	if !ok || maxEvents < 1 || maxEvents > 1_000_000 || !carryOK || len(carry) > 128 || !sort.StringsAreSorted(carry) {
		return ContinueAsNewPolicy{}, true, fmt.Errorf("%w: malformed continue-as-new policy", ErrInvalidRecord)
	}
	for index, name := range carry {
		if graph.ValidateID(name) != nil || index > 0 && carry[index-1] == name {
			return ContinueAsNewPolicy{}, true, fmt.Errorf("%w: malformed continue-as-new carry identity", ErrInvalidRecord)
		}
	}
	carried := make(map[string]struct{}, len(carry))
	for _, name := range carry {
		carried[name] = struct{}{}
	}
	inputs := make(map[string]graph.Schema, len(workflow.Inputs))
	outputs := make(map[string]graph.Schema, len(workflow.Outputs))
	for _, input := range workflow.Inputs {
		inputs[input.Name] = input.Schema
		if _, ok := carried[input.Name]; ok {
			continue
		}
		if input.Default != nil && !continuationLiteralDefault(input) {
			return ContinueAsNewPolicy{}, true, fmt.Errorf("%w: workflow input %q has a continuation-unsafe default", ErrInvalidRecord, input.Name)
		}
		if input.Required && input.Default == nil {
			return ContinueAsNewPolicy{}, true, fmt.Errorf("%w: required workflow input %q is not carried across continue-as-new", ErrInvalidRecord, input.Name)
		}
	}
	for _, output := range workflow.Outputs {
		outputs[output.Name] = output.Schema
	}
	for _, name := range carry {
		inputSchema, inputOK := inputs[name]
		outputSchema, outputOK := outputs[name]
		if !inputOK || !outputOK || !reflect.DeepEqual(inputSchema, outputSchema) || schemaMayCarryReference(inputSchema) {
			return ContinueAsNewPolicy{}, true, fmt.Errorf("%w: continue-as-new carry is not an exact inline-safe input/output", ErrInvalidRecord)
		}
	}
	capacity, capacityErr := reactorEventCapacity(workflow)
	if capacityErr != nil || maxEvents < capacity {
		return ContinueAsNewPolicy{}, true, fmt.Errorf("%w: continue-as-new max_events is below the plan's bounded delivery capacity", ErrInvalidRecord)
	}
	return ContinueAsNewPolicy{MaxEvents: maxEvents, Carry: carry}, true, nil
}

func continuationLiteralDefault(input graph.InputSpec) bool {
	return values.ValidateLiteralBindingSchema(input.Default, input.Schema) == nil
}

// reactorEventCapacity is deliberately conservative. Generation one consumes
// its source activation at start; every authored named signal/event wait may
// consume one later delivery. Dynamic/retried named waits cannot establish a
// finite per-generation maximum and are rejected.
func reactorEventCapacity(workflow graph.Graph) (int, error) {
	capacity := 1
	waits := 0
	for _, node := range workflow.Nodes {
		if node.Kind != "wait_for" || !namedReactorWait(node.Config) {
			continue
		}
		waits++
		if node.ForEach != nil || node.Retry != nil && node.Retry.Attempts > 1 {
			return 0, fmt.Errorf("named reactor wait %q has dynamic or retried cardinality", node.ID)
		}
		capacity++
	}
	if waits == 0 {
		return 0, fmt.Errorf("reactor has no bounded named signal/event wait")
	}
	return capacity, nil
}

func namedReactorWait(config graph.Config) bool {
	_, signal := config["signal"]
	_, event := config["event"]
	return signal || event
}

func schemaMayCarryReference(schema graph.Schema) bool {
	return schemaNodeMayCarryReference(map[string]any(schema))
}

func schemaNodeMayCarryReference(node any) bool {
	var object map[string]any
	switch typed := node.(type) {
	case graph.Schema:
		object = map[string]any(typed)
	case map[string]any:
		object = typed
	default:
		return false
	}
	switch declared := object["type"].(type) {
	case string:
		if declared == "artifact" || declared == "secret_ref" {
			return true
		}
	case []any:
		for _, item := range declared {
			if name, ok := item.(string); ok && (name == "artifact" || name == "secret_ref") {
				return true
			}
		}
	}
	for _, key := range []string{"items", "additionalItems", "additionalProperties", "unevaluatedItems", "unevaluatedProperties", "propertyNames", "contains", "not", "if", "then", "else", "contentSchema"} {
		if schemaNodeMayCarryReference(object[key]) {
			return true
		}
	}
	for _, key := range []string{"$defs", "definitions", "properties", "patternProperties", "dependentSchemas", "dependencies"} {
		children, ok := object[key].(map[string]any)
		if !ok {
			continue
		}
		for _, child := range children {
			if schemaNodeMayCarryReference(child) {
				return true
			}
		}
	}
	for _, key := range []string{"prefixItems", "allOf", "anyOf", "oneOf"} {
		children, ok := object[key].([]any)
		if !ok {
			continue
		}
		for _, child := range children {
			if schemaNodeMayCarryReference(child) {
				return true
			}
		}
	}
	return false
}

func runtimeDurabilityInteger(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), int64(int(typed)) == typed
	case json.Number:
		parsed, err := typed.Int64()
		return int(parsed), err == nil && int64(int(parsed)) == parsed
	case float64:
		return int(typed), typed == float64(int(typed))
	default:
		return 0, false
	}
}

func runtimeDurabilityStrings(value any) ([]string, bool) {
	if value == nil {
		return nil, true
	}
	switch typed := value.(type) {
	case []string:
		return append([]string(nil), typed...), true
	case []any:
		result := make([]string, len(typed))
		for index, item := range typed {
			var ok bool
			result[index], ok = item.(string)
			if !ok {
				return nil, false
			}
		}
		return result, true
	default:
		return nil, false
	}
}
