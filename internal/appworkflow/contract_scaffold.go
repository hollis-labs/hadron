package appworkflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func snapshotStepKinds(input stepkind.Registry) (*stepkind.MemoryRegistry, error) {
	result := stepkind.NewRegistry()
	for _, spec := range input.List() {
		kind, ok := input.Lookup(spec.Name, spec.Version)
		if !ok || nilInterface(kind) {
			return nil, fmt.Errorf("step-kind registry lookup/list mismatch for %s@%s", spec.Name, spec.Version)
		}
		if err := result.Register(kind); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func scaffoldFor(plan *compile.ExecutionPlan, kinds stepkind.Registry) (WorkflowContractSuite, error) {
	if plan == nil {
		return WorkflowContractSuite{}, fmt.Errorf("%w: execution plan is required", ErrInvalidContractService)
	}
	inputs := make(values.ValueSet, len(plan.Graph.Inputs))
	for _, declaration := range plan.Graph.Inputs {
		literal := scaffoldLiteral(declaration.Schema)
		value, err := values.NewInline(literal, scaffoldMetadata("workflow-input", plan.ID, declaration.Name))
		if err != nil || values.ValidateValueSchema(declaration.Schema, value) != nil {
			// Null is deliberately visible as an editable placeholder when no
			// safe schema-conforming example can be inferred.
			value, err = values.NewInline(nil, scaffoldMetadata("workflow-input", plan.ID, declaration.Name))
		}
		if err != nil {
			return WorkflowContractSuite{}, err
		}
		inputs[declaration.Name] = value
	}
	outputs := make(values.ValueSet, len(plan.Graph.Outputs))
	for _, declaration := range plan.Graph.Outputs {
		value, err := values.NewInline(scaffoldLiteral(declaration.Schema), scaffoldMetadata("workflow-output", plan.ID, declaration.Name))
		if err != nil {
			return WorkflowContractSuite{}, err
		}
		outputs[declaration.Name] = value
	}

	nodes := append([]graph.Node(nil), plan.Graph.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	mocks := make([]ContractExecutorMock, 0, len(nodes))
	var effects graph.EffectSet
	for _, node := range nodes {
		_, spec, err := stepkind.Resolve(kinds, node.Kind, node.KindVersion)
		if err != nil {
			return WorkflowContractSuite{}, err
		}
		effects = append(effects, spec.Effects...)
		effects = append(effects, node.Effects...)
		mockOutputs := make(values.ValueSet, len(node.Outputs))
		for _, declaration := range node.Outputs {
			value, valueErr := values.NewInline(scaffoldLiteral(declaration.Schema), scaffoldMetadata("executor-mock", node.ID, declaration.Name))
			if valueErr != nil {
				return WorkflowContractSuite{}, valueErr
			}
			mockOutputs[declaration.Name] = value
		}
		config, cloneErr := cloneContractConfig(node.Config)
		if cloneErr != nil {
			return WorkflowContractSuite{}, cloneErr
		}
		configSchema, cloneErr := cloneContractSchema(spec.ConfigSchema)
		if cloneErr != nil {
			return WorkflowContractSuite{}, cloneErr
		}
		inputSchema, cloneErr := cloneContractSchema(spec.InputSchema)
		if cloneErr != nil {
			return WorkflowContractSuite{}, cloneErr
		}
		outputSchema, cloneErr := cloneContractSchema(spec.OutputSchema)
		if cloneErr != nil {
			return WorkflowContractSuite{}, cloneErr
		}
		expectedInputs := values.ValueSet{}
		inputsEditable := hasDependencies(plan.Graph, node.ID)
		if !inputsEditable {
			expectedInputs, cloneErr = bindNodeInputs(node, inputs, runtime.RunID("contract-scaffold"))
			if cloneErr != nil {
				// A binding that requires invocation-only scope remains an explicit
				// editable placeholder and cannot qualify unchanged.
				expectedInputs, inputsEditable = values.ValueSet{}, true
			}
		}
		mocks = append(mocks, ContractExecutorMock{
			NodeID: node.ID, Kind: spec.Name, KindVersion: spec.Version,
			ConfigSchema: configSchema, InputSchema: inputSchema,
			OutputSchema: outputSchema, ExpectedConfig: config,
			ExpectedInputs: expectedInputs, ExpectedInputsEditable: inputsEditable,
			Results: []ContractMockResult{{Outputs: mockOutputs}},
		})
	}
	return WorkflowContractSuite{SchemaVersion: ContractSuiteSchemaVersion, Cases: []WorkflowContractCase{{
		Name: "edit-me", Editable: true, Inputs: inputs, ExpectedOutputs: outputs,
		ExpectedEffects: sortedEffects(effects), Mocks: mocks,
	}}}, nil
}

func cloneContractSchema(input graph.Schema) (graph.Schema, error) {
	encoded, err := canonicalJSON(input)
	if err != nil {
		return nil, err
	}
	var result graph.Schema
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func scaffoldLiteral(schema graph.Schema) any {
	typeName, _ := schema["type"].(string)
	switch typeName {
	case "boolean":
		return false
	case "integer", "number":
		return json.Number("0")
	case "array":
		return []any{}
	case "object":
		return map[string]any{}
	case "null":
		return nil
	default:
		return "EDIT_ME"
	}
}

func scaffoldMetadata(kind, reference, output string) values.Metadata {
	return values.Metadata{
		Producer:  values.Producer{Kind: kind, Reference: reference, Output: output},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}
}

func cloneContractConfig(input graph.Config) (graph.Config, error) {
	if input == nil {
		return graph.Config{}, nil
	}
	encoded, err := canonicalJSON(input)
	if err != nil {
		return nil, err
	}
	var result graph.Config
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}
