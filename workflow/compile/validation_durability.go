package compile

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func (v *validator) validateDurability() {
	spec := v.graph.Durability
	if spec == nil {
		return
	}
	if !spec.Mode.Valid() {
		v.add(CodeInvalidDurability, graphSource(v.graph), fmt.Sprintf("workflow durability mode %q is unsupported", spec.Mode), "Use durability none or steps.")
		return
	}
	if spec.Extension.Version != "" {
		v.validateContinueAsNew(spec)
	}
	if spec.Mode != graph.DurabilityNone {
		return
	}
	if spec.Extension.Version != "" {
		v.add(CodeInvalidDurability, firstSource(spec.Extension.Source, graphSource(v.graph)), "durability none cannot continue as new", "Use durability steps for reactors and continue-as-new.")
	}
	if targetRequired(v.graph.Target) {
		v.add(CodeInvalidDurability, graphSource(v.graph), "durability none cannot bind workflow execution-target requirements", "Remove the target requirements or use durability steps with a host-selected target.")
	}
	for _, node := range v.graph.Nodes {
		if node.Durability != nil && (node.Durability.Mode != graph.DurabilityNone || node.Durability.Extension.Version != "") {
			v.add(CodeInvalidDurability, v.nodeSource(node), fmt.Sprintf("node %q durability override is incompatible with workflow durability none", node.ID), "Remove the override or keep the node durability mode none without an extension.")
		}
		if targetRequired(node.Target) {
			v.add(CodeInvalidDurability, v.nodeSource(node), fmt.Sprintf("node %q requires a host-selected execution target", node.ID), "Remove the target requirements or use durability steps.")
		}
		if retryRequiresDurableTimer(node.Retry) {
			v.add(CodeInvalidDurability, v.nodeSource(node), fmt.Sprintf("node %q retry policy requires durable backoff scheduling", node.ID), "Use immediate retries without backoff or durability steps.")
		}
		_, registered, reason := v.kinds.resolve(node.Kind, node.KindVersion)
		if registered == nil {
			if reason == "" {
				reason = "the exact executor is unavailable"
			}
			v.add(CodeInvalidDurability, v.nodeSource(node), fmt.Sprintf("durability none cannot validate node %q: %s", node.ID, reason), "Pin and register an executor that truthfully supports non-durable execution.")
			continue
		}
		if !nonDurableKindSupported(node, *registered) {
			v.add(CodeInvalidDurability, v.nodeSource(node), fmt.Sprintf("node %q (%s@%s) requires durable or host-dependent execution", node.ID, registered.Name, registered.Version), "Use durability steps or choose an embedded read/compute executor without waits, external lifecycle, services, calls, or host capabilities.")
		}
	}
}

func targetRequired(target graph.ExecutionTargetRequirements) bool {
	return len(target.Kinds) != 0 || len(target.Capabilities) != 0 || len(target.Labels) != 0 || len(target.Constraints) != 0
}

func retryRequiresDurableTimer(retry *graph.RetryPolicy) bool {
	if retry == nil || retry.Attempts <= 1 {
		return false
	}
	return retry.Backoff.Strategy != "" && retry.Backoff.Strategy != graph.BackoffNone ||
		retry.Backoff.InitialDelay != "" || retry.Backoff.MaxDelay != "" || retry.Backoff.Multiplier != 0
}

func nonDurableKindSupported(node graph.Node, spec stepkind.StepKindSpec) bool {
	if !spec.EmbeddedModeSupported || spec.CanSuspend || spec.Observation.Mode != stepkind.ObservationNone ||
		spec.Lifecycle.Service || node.Service != nil || node.Call != nil || len(spec.RequiredCapabilities) != 0 {
		return false
	}
	for _, effect := range append(append(graph.EffectSet(nil), spec.Effects...), node.Effects...) {
		if effect != graph.EffectRead && effect != graph.EffectCompute {
			return false
		}
	}
	return true
}

func (v *validator) validateContinueAsNew(spec *graph.DurabilitySpec) {
	source := firstSource(spec.Extension.Source, graphSource(v.graph))
	if spec.Extension.Version != reactorDurabilityExtensionVersion {
		v.add(CodeInvalidDurability, source, fmt.Sprintf("continue-as-new extension version %q is unsupported", spec.Extension.Version), "Compile the canonical reactor/v1 continue_as_new policy.")
		return
	}
	if spec.Mode != graph.DurabilitySteps {
		v.add(CodeInvalidDurability, source, "continue-as-new requires durability steps", "Use durability mode steps for a durable reactor.")
	}
	maxEvents, ok := durabilityInteger(spec.Extension.Config["max_events"])
	if !ok || maxEvents < 1 || maxEvents > 1_000_000 {
		v.add(CodeInvalidDurability, source, "continue_as_new.max_events must be between 1 and 1000000", "Choose a positive bounded event-history threshold.")
	}
	capacity := 1
	namedWaits := 0
	boundedCapacity := true
	for _, node := range v.graph.Nodes {
		if node.Kind != "wait_for" || !namedReactorWait(node.Config) {
			continue
		}
		namedWaits++
		capacity++
		if node.ForEach != nil || node.Retry != nil && node.Retry.Attempts > 1 {
			boundedCapacity = false
			v.add(CodeInvalidDurability, v.nodeSource(node), fmt.Sprintf("reactor signal/event wait %q has dynamic or retried delivery cardinality", node.ID), "Use one non-fan-out, single-attempt named wait so max_events remains a truthful upper bound.")
		}
	}
	if namedWaits == 0 {
		v.add(CodeInvalidDurability, source, "continue-as-new requires at least one bounded named signal/event wait", "Add a non-fan-out, single-attempt wait_for.signal or wait_for.event node so carried generations cannot roll without another delivery.")
	}
	if ok && boundedCapacity && maxEvents < capacity {
		v.add(CodeInvalidDurability, source, fmt.Sprintf("continue_as_new.max_events %d is below the plan's maximum %d source delivery consumptions", maxEvents, capacity), "Include the generation-one activation and every named signal/event wait in max_events.")
	}
	carry, ok := durabilityStrings(spec.Extension.Config["carry"])
	if !ok || len(carry) > 128 || !sort.StringsAreSorted(carry) {
		v.add(CodeInvalidDurability, source, "continue_as_new.carry must be a sorted bounded list", "Use at most 128 unique workflow output/input names.")
		return
	}
	carried := make(map[string]struct{}, len(carry))
	for _, name := range carry {
		carried[name] = struct{}{}
	}
	inputs := make(map[string]graph.Schema, len(v.graph.Inputs))
	outputs := make(map[string]graph.Schema, len(v.graph.Outputs))
	for _, input := range v.graph.Inputs {
		inputs[input.Name] = input.Schema
		if _, ok := carried[input.Name]; ok {
			continue
		}
		if input.Default != nil && !continuationLiteralDefault(input) {
			v.add(CodeInvalidDurability, firstSource(input.Source, source), fmt.Sprintf("workflow input %q has a continuation-unsafe default", input.Name), "Use an exact literal default that satisfies the input schema, or carry the input across continue-as-new.")
			continue
		}
		if input.Required && input.Default == nil {
			v.add(CodeInvalidDurability, firstSource(input.Source, source), fmt.Sprintf("required workflow input %q must be carried across continue-as-new", input.Name), "Add the required input to continue_as_new.carry or declare a literal default for later generations.")
		}
	}
	for _, output := range v.graph.Outputs {
		outputs[output.Name] = output.Schema
	}
	for index, name := range carry {
		if index > 0 && name == carry[index-1] {
			v.add(CodeInvalidDurability, source, fmt.Sprintf("continue_as_new carry name %q is duplicated", name), "Use each carry name once.")
			continue
		}
		inputSchema, inputOK := inputs[name]
		outputSchema, outputOK := outputs[name]
		if !inputOK || !outputOK {
			v.add(CodeInvalidDurability, source, fmt.Sprintf("continue_as_new carry name %q must be both a workflow output and input", name), "Declare a same-named typed output and next-generation input.")
			continue
		}
		left, _ := json.Marshal(inputSchema)
		right, _ := json.Marshal(outputSchema)
		if string(left) != string(right) {
			v.add(CodeInvalidDurability, source, fmt.Sprintf("continue_as_new carry name %q has different input and output schemas", name), "Use one exact schema for carried state on both sides of the generation boundary.")
			continue
		}
		if schemaMayCarryReference(inputSchema) {
			v.add(CodeInvalidDurability, source, fmt.Sprintf("continue_as_new carry name %q can contain an artifact or secret reference", name), "Carry only inline-safe typed state across the next-generation start boundary.")
		}
	}
	hasActivation := false
	for _, activation := range v.graph.Activations {
		if activation.Kind == "message" || activation.Kind == "event" {
			hasActivation = true
			break
		}
	}
	if !hasActivation {
		v.add(CodeInvalidDurability, source, "continue-as-new requires an on.message or on.event activation", "Declare the source-owned activation that establishes reactor identity.")
	}
}

func continuationLiteralDefault(input graph.InputSpec) bool {
	return values.ValidateLiteralBindingSchema(input.Default, input.Schema) == nil
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

func namedReactorWait(config graph.Config) bool {
	_, signal := config["signal"]
	_, event := config["event"]
	return signal || event
}

func durabilityInteger(value any) (int, bool) {
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

func durabilityStrings(value any) ([]string, bool) {
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
