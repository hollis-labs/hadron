package compile

import (
	"strconv"

	"github.com/hollis-labs/hadron/workflow/graph"
	"gopkg.in/yaml.v3"
)

// Compile lowers one loaded graph-native workflow source into an immutable
// execution plan. It does not evaluate expressions, resolve definitions,
// infer dependencies, validate registered kinds, or perform graph validation.
func Compile(source *Source) CompileResult {
	return CompileWithOptions(source, CompileOptions{})
}

// CompileWithOptions lowers source and then applies registered pure node
// expanders before graph and plan digests are computed. The same source and
// expander set must always produce byte-identical plan semantics.
func CompileWithOptions(source *Source, options CompileOptions) CompileResult {
	l := &lowerer{source: source}
	if source == nil || source.Document == nil || source.Document.Kind != yaml.DocumentNode || len(source.Document.Content) != 1 {
		l.invalidShape(nil, nil, "a loaded single-document Source is required")
		return CompileResult{Diagnostics: l.diagnostics}
	}

	root := source.Document.Content[0]
	rootFields := l.mapping(root, nil, "workflow", "on", "inputs", "outputs", "steps", "finally")
	headerField, ok := rootFields["workflow"]
	if !ok {
		l.invalidShape(root, nil, "required graph-native workflow marker is missing")
		return CompileResult{Diagnostics: l.diagnostics}
	}

	rawDigest := sourceDigest(source.Bytes())
	header := l.lowerHeader(headerField.value, headerField.path, rawDigest)
	sourceMap := graph.SourceMap{
		Inputs:      make(map[string]graph.SourceRef),
		Outputs:     make(map[string]graph.SourceRef),
		Nodes:       make(map[string]graph.SourceRef),
		Edges:       make(map[string]graph.SourceRef),
		Activations: make(map[string]graph.SourceRef),
	}
	graphRef := l.location(headerField.value, headerField.path)
	sourceMap.Graph = &graphRef

	compiledGraph := graph.Graph{
		ID:         header.id,
		Namespace:  header.namespace,
		Version:    header.version,
		Provenance: header.provenance,
		Source:     &graphRef,
		Metadata:   header.metadata,
	}
	if field, exists := rootFields["inputs"]; exists {
		compiledGraph.Inputs = l.lowerInputs(field.value, field.path, sourceMap.Inputs)
	}
	if field, exists := rootFields["outputs"]; exists {
		compiledGraph.Outputs = l.lowerOutputs(field.value, field.path, sourceMap.Outputs, true)
	}
	if field, exists := rootFields["on"]; exists {
		compiledGraph.Activations = l.lowerActivations(field.value, field.path, sourceMap.Activations, header.provenance)
	}
	if field, exists := rootFields["steps"]; exists {
		compiledGraph.Nodes, compiledGraph.Edges = l.lowerNodes(field.value, field.path, &sourceMap, false)
	} else {
		l.invalidShape(root, nil, "required steps sequence is missing")
	}
	if field, exists := rootFields["finally"]; exists {
		finallyNodes, finallyEdges := l.lowerNodes(field.value, field.path, &sourceMap, true)
		compiledGraph.Nodes = append(compiledGraph.Nodes, finallyNodes...)
		compiledGraph.Edges = append(compiledGraph.Edges, finallyEdges...)
	}
	compiledGraph.SourceMap = sourceMap

	if len(l.diagnostics) != 0 {
		return CompileResult{Diagnostics: l.diagnostics}
	}
	bundled, expansionDiagnostics := expandGraph(compiledGraph, options)
	if len(expansionDiagnostics) != 0 {
		return CompileResult{Diagnostics: expansionDiagnostics}
	}
	compiledGraph = bundled.Graph
	sourceMap = compiledGraph.SourceMap
	graphDigest, err := digestGraph(compiledGraph)
	if err != nil {
		l.invalidShape(root, nil, err.Error())
		return CompileResult{Diagnostics: l.diagnostics}
	}
	compiledGraph.Digest = graphDigest

	definition := graph.DefinitionRef{
		Authority: header.provenance.Authority,
		Kind:      "workflow",
		ID:        header.id,
		Version:   header.version,
		Digest:    rawDigest,
	}
	plan := ExecutionPlan{
		SchemaVersion:      ExecutionPlanSchemaVersion,
		ID:                 header.id,
		Definition:         definition,
		Provenance:         header.provenance,
		SourceDigests:      []SourceDigest{{Format: graph.SourceWorkflow, Digest: rawDigest}},
		Graph:              compiledGraph,
		SourceMap:          sourceMap,
		BundledDefinitions: bundled.Definitions,
	}
	planDigest, err := digestPlan(plan)
	if err != nil {
		l.invalidShape(root, nil, err.Error())
		return CompileResult{Diagnostics: l.diagnostics}
	}
	plan.Digest = planDigest
	return CompileResult{Plan: &plan}
}

type loweredHeader struct {
	id         string
	namespace  string
	version    string
	provenance graph.Provenance
	metadata   graph.Metadata
}

func (l *lowerer) lowerHeader(node *yaml.Node, path []string, digest string) loweredHeader {
	fields := l.mapping(node, path, "id", "name", "namespace", "version", "provenance", "metadata")
	identity, ok := fields["id"]
	if !ok {
		identity, ok = fields["name"]
	}
	if !ok {
		l.invalidShape(node, path, "workflow.id or workflow.name is required")
	}
	header := loweredHeader{version: "1.0.0"}
	if ok {
		header.id = l.normalizeID(identity.value, identity.path)
	}
	if field, exists := fields["namespace"]; exists {
		header.namespace = l.normalizeID(field.value, field.path)
	}
	if field, exists := fields["version"]; exists {
		header.version = l.string(field.value, field.path)
		if header.version == "" {
			l.invalidShape(field.value, field.path, "version must not be empty")
		}
	}
	if field, exists := fields["metadata"]; exists {
		header.metadata = l.metadata(field.value, field.path)
	}
	header.provenance = graph.Provenance{
		Origin:  "workflow-source",
		Locator: l.source.Locator,
		Digest:  digest,
	}
	if field, exists := fields["provenance"]; exists {
		header.provenance = l.lowerProvenance(field.value, field.path, header.provenance)
	}
	return header
}

func (l *lowerer) lowerProvenance(node *yaml.Node, path []string, base graph.Provenance) graph.Provenance {
	fields := l.mapping(node, path, "authority", "origin", "revision", "parents", "metadata")
	if field, ok := fields["authority"]; ok {
		base.Authority = l.string(field.value, field.path)
	}
	if field, ok := fields["origin"]; ok {
		base.Origin = l.string(field.value, field.path)
	}
	if field, ok := fields["revision"]; ok {
		base.Revision = l.string(field.value, field.path)
	}
	if field, ok := fields["metadata"]; ok {
		base.Metadata = l.metadata(field.value, field.path)
	}
	if field, ok := fields["parents"]; ok {
		items := l.sequence(field.value, field.path)
		base.Parents = make([]graph.ProvenanceRef, 0, len(items))
		for i, item := range items {
			itemPath := appendPath(field.path, strconv.Itoa(i))
			parentFields := l.mapping(item, itemPath, "authority", "locator", "digest")
			var parent graph.ProvenanceRef
			if value, exists := parentFields["authority"]; exists {
				parent.Authority = l.string(value.value, value.path)
			}
			if value, exists := parentFields["locator"]; exists {
				parent.Locator = l.string(value.value, value.path)
			}
			if value, exists := parentFields["digest"]; exists {
				parent.Digest = l.string(value.value, value.path)
			}
			base.Parents = append(base.Parents, parent)
		}
	}
	return base
}

func (l *lowerer) lowerInputs(node *yaml.Node, path []string, sourceMap map[string]graph.SourceRef) []graph.InputSpec {
	items := l.sequence(node, path)
	inputs := make([]graph.InputSpec, 0, len(items))
	for i, item := range items {
		itemPath := appendPath(path, strconv.Itoa(i))
		fields := l.mapping(item, itemPath, "name", "description", "schema", "type", "items_type", "required", "default", "metadata")
		nameField, ok := fields["name"]
		if !ok {
			l.invalidShape(item, itemPath, "input.name is required")
			continue
		}
		input := graph.InputSpec{
			Name:   l.normalizeID(nameField.value, nameField.path),
			Source: pointer(l.location(item, itemPath)),
		}
		if field, exists := fields["description"]; exists {
			input.Description = l.string(field.value, field.path)
		}
		input.Schema = l.lowerSchema(fields)
		if field, exists := fields["required"]; exists {
			input.Required = l.boolean(field.value, field.path)
		}
		if field, exists := fields["default"]; exists {
			binding := l.lowerBinding(field.value, field.path)
			input.Default = &binding
		}
		if field, exists := fields["metadata"]; exists {
			input.Metadata = l.metadata(field.value, field.path)
		}
		inputs = append(inputs, input)
		if input.Name != "" {
			sourceMap[input.Name] = l.location(item, itemPath)
		}
	}
	return inputs
}

func (l *lowerer) lowerOutputs(node *yaml.Node, path []string, sourceMap map[string]graph.SourceRef, valuesRequired bool) []graph.OutputSpec {
	if node.Kind == yaml.MappingNode {
		outputs := make([]graph.OutputSpec, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			key, value := node.Content[i], node.Content[i+1]
			itemPath := appendPath(path, key.Value)
			name := l.normalizeID(key, itemPath)
			output := l.lowerNamedOutput(name, value, itemPath, valuesRequired)
			output.Source = pointer(l.location(value, itemPath))
			outputs = append(outputs, output)
			if sourceMap != nil && name != "" {
				sourceMap[name] = l.location(value, itemPath)
			}
		}
		return outputs
	}

	items := l.sequence(node, path)
	outputs := make([]graph.OutputSpec, 0, len(items))
	for i, item := range items {
		itemPath := appendPath(path, strconv.Itoa(i))
		fields := l.mapping(item, itemPath, "name", "description", "schema", "type", "items_type", "value", "metadata")
		nameField, ok := fields["name"]
		if !ok {
			l.invalidShape(item, itemPath, "output.name is required")
			continue
		}
		name := l.normalizeID(nameField.value, nameField.path)
		output := l.outputFromFields(name, fields, item, itemPath, valuesRequired)
		output.Source = pointer(l.location(item, itemPath))
		outputs = append(outputs, output)
		if sourceMap != nil && name != "" {
			sourceMap[name] = l.location(item, itemPath)
		}
	}
	return outputs
}

func (l *lowerer) lowerNamedOutput(name string, node *yaml.Node, path []string, valuesRequired bool) graph.OutputSpec {
	if node.Kind != yaml.MappingNode || !hasAnyMappingKey(node, "description", "schema", "type", "items_type", "value", "metadata") {
		output := graph.OutputSpec{Name: name, Schema: graph.Schema{}}
		binding := l.lowerBinding(node, path)
		output.Value = &binding
		return output
	}
	fields := l.mapping(node, path, "description", "schema", "type", "items_type", "value", "metadata")
	return l.outputFromFields(name, fields, node, path, valuesRequired)
}

func (l *lowerer) outputFromFields(name string, fields map[string]sourceField, node *yaml.Node, path []string, valuesRequired bool) graph.OutputSpec {
	output := graph.OutputSpec{Name: name, Schema: l.lowerSchema(fields)}
	if field, ok := fields["description"]; ok {
		output.Description = l.string(field.value, field.path)
	}
	if field, ok := fields["value"]; ok {
		binding := l.lowerBinding(field.value, field.path)
		output.Value = &binding
	} else if valuesRequired {
		l.invalidShape(node, path, "workflow output.value is required")
	}
	if field, ok := fields["metadata"]; ok {
		output.Metadata = l.metadata(field.value, field.path)
	}
	return output
}

func (l *lowerer) lowerSchema(fields map[string]sourceField) graph.Schema {
	if field, ok := fields["schema"]; ok {
		value := l.jsonValue(field.value, field.path)
		object, ok := value.(map[string]any)
		if !ok {
			l.invalidShape(field.value, field.path, "schema must be a JSON object")
			return graph.Schema{}
		}
		return graph.Schema(object)
	}
	typeField, hasType := fields["type"]
	itemsField, hasItemsType := fields["items_type"]
	if !hasType {
		if hasItemsType {
			l.invalidShape(itemsField.value, itemsField.path, "items_type requires type: array")
		}
		return graph.Schema{}
	}
	typeName := l.string(typeField.value, typeField.path)
	schema := graph.Schema{"type": typeName}
	if hasItemsType {
		if typeName != "array" {
			l.invalidShape(itemsField.value, itemsField.path, "items_type requires type: array")
		} else {
			schema["items"] = map[string]any{"type": l.string(itemsField.value, itemsField.path)}
		}
	}
	return schema
}

func (l *lowerer) metadata(node *yaml.Node, path []string) graph.Metadata {
	value := l.jsonValue(node, path)
	object, ok := value.(map[string]any)
	if !ok {
		l.invalidShape(node, path, "metadata must be a JSON object")
		return nil
	}
	return graph.Metadata(object)
}

func (l *lowerer) location(node *yaml.Node, path []string) graph.SourceRef {
	if ref, ok := l.source.Location(path...); ok {
		return ref
	}
	return sourceRef(l.source.Locator, graph.SourceWorkflow, node, path)
}

func hasAnyMappingKey(node *yaml.Node, names ...string) bool {
	if node.Kind != yaml.MappingNode {
		return false
	}
	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[name] = struct{}{}
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if _, ok := wanted[node.Content[i].Value]; ok {
			return true
		}
	}
	return false
}

func pointer[T any](value T) *T { return &value }
