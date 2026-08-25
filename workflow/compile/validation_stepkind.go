package compile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

type kindCatalog struct {
	lookup StepKindLookup
	specs  map[string][]stepkind.StepKindSpec
}

func newKindCatalog(lookup StepKindLookup) kindCatalog {
	catalog := kindCatalog{specs: make(map[string][]stepkind.StepKindSpec)}
	if isNilInterface(lookup) {
		return catalog
	}
	catalog.lookup = lookup
	for _, spec := range lookup.List() {
		catalog.specs[spec.Name] = append(catalog.specs[spec.Name], spec)
	}
	for name := range catalog.specs {
		sort.SliceStable(catalog.specs[name], func(i, j int) bool {
			return catalog.specs[name][i].Version < catalog.specs[name][j].Version
		})
	}
	return catalog
}

func (c kindCatalog) resolve(name, version string) (stepkind.StepKind, *stepkind.StepKindSpec, string) {
	if c.lookup == nil {
		return nil, nil, ""
	}
	requestedVersion := version
	if requestedVersion == "" {
		matches := c.specs[name]
		switch len(matches) {
		case 0:
			return nil, nil, ""
		case 1:
			requestedVersion = matches[0].Version
		default:
			versions := make([]string, len(matches))
			for i, spec := range matches {
				versions[i] = spec.Version
			}
			return nil, nil, fmt.Sprintf("step kind %q has multiple registered versions (%s) and kind_version is not pinned", name, strings.Join(versions, ", "))
		}
	}
	kind, ok := c.lookup.Lookup(name, requestedVersion)
	if !ok || isNilInterface(kind) {
		return nil, nil, ""
	}
	for _, candidate := range c.specs[name] {
		if candidate.Version == requestedVersion {
			spec := candidate
			return kind, &spec, ""
		}
	}
	spec := kind.Spec()
	return kind, &spec, ""
}

func (v *validator) validateKindConfig(node graph.Node, kind stepkind.StepKind, spec *stepkind.StepKindSpec) {
	if spec != nil {
		v.validateConfigSchema(node, *spec)
		v.validateMemoizationSafety(node, *spec)
	}
	config := node.Config
	if config == nil {
		config = graph.Config{}
	}
	validationConfig, err := cloneValidationConfig(config)
	if err != nil {
		v.add(CodeInvalidStepConfig, v.nodeSource(node), fmt.Sprintf("node %q config could not be isolated for step-kind validation", node.ID), "Keep executor config JSON-compatible.")
		return
	}
	for _, finding := range kind.ValidateConfig(v.ctx, validationConfig) {
		v.diagnostics = append(v.diagnostics, normalizeFinding(
			finding,
			v.nodeSource(node),
			CodeInvalidStepConfig,
			fmt.Sprintf("step kind %q rejected config for node %q", node.Kind, node.ID),
			"Update the node config to satisfy the registered step-kind contract.",
		))
	}
}

func cloneValidationConfig(input graph.Config) (graph.Config, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result graph.Config
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func (v *validator) validateMemoizationSafety(node graph.Node, spec stepkind.StepKindSpec) {
	if node.Memoization == nil {
		return
	}
	if spec.Memoization == stepkind.MemoizationDisabled {
		v.add(CodeInvalidMemoization, v.nodeSource(node), fmt.Sprintf("node %q requests memoization but step kind %q disables it", node.ID, spec.Name), "Remove memoize or select an executor that truthfully supports result reuse.")
		return
	}
	effects := make(map[graph.Effect]struct{}, len(node.Effects)+len(spec.Effects))
	for _, effect := range spec.Effects {
		effects[effect] = struct{}{}
	}
	for _, effect := range node.Effects {
		effects[effect] = struct{}{}
	}
	if _, mutate := effects[graph.EffectMutate]; mutate {
		v.add(CodeInvalidMemoization, v.nodeSource(node), fmt.Sprintf("node %q cannot memoize mutate effects", node.ID), "Remove memoize from mutating work.")
		return
	}
	if _, destructive := effects[graph.EffectDestructive]; destructive {
		v.add(CodeInvalidMemoization, v.nodeSource(node), fmt.Sprintf("node %q cannot memoize destructive effects", node.ID), "Remove memoize from destructive work.")
		return
	}
	if _, materialize := effects[graph.EffectMaterialize]; materialize && spec.Memoization != stepkind.MemoizationApproved {
		v.add(CodeInvalidMemoization, v.nodeSource(node), fmt.Sprintf("node %q materialize memoization lacks executor approval", node.ID), "Use a kind that explicitly approves materialized-result reuse; host policy must also approve at runtime.")
	}
}

func (v *validator) validateConfigSchema(node graph.Node, spec stepkind.StepKindSpec) {
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(rejectExternalSchemaLoader{})
	const resource = "urn:hadron:workflow:validation:step-kind-config"
	document, err := plainJSON(spec.ConfigSchema)
	if err != nil {
		v.invalidRegisteredSchema(node, spec, err)
		return
	}
	if addErr := compiler.AddResource(resource, document); addErr != nil {
		v.invalidRegisteredSchema(node, spec, addErr)
		return
	}
	compiled, err := compiler.Compile(resource)
	if err != nil {
		v.invalidRegisteredSchema(node, spec, err)
		return
	}
	config := node.Config
	if config == nil {
		config = graph.Config{}
	}
	instance, err := plainJSON(config)
	if err != nil {
		v.add(
			CodeInvalidStepConfig,
			v.nodeSource(node),
			fmt.Sprintf("node %q config is not JSON-compatible: %v", node.ID, err),
			"Use JSON-compatible values in node config.",
		)
		return
	}
	if err := compiled.Validate(instance); err != nil {
		var validationError *jsonschema.ValidationError
		if !errors.As(err, &validationError) {
			v.add(
				CodeInvalidStepConfig,
				v.nodeSource(node),
				fmt.Sprintf("node %q config does not satisfy step kind %q schema", node.ID, spec.Name),
				"Update the node config to satisfy the registered step-kind schema.",
			)
			return
		}
		leaves := validationLeaves(validationError)
		sort.SliceStable(leaves, func(i, j int) bool {
			return validationErrorKey(leaves[i]) < validationErrorKey(leaves[j])
		})
		for _, finding := range leaves {
			instance := jsonPointer(finding.InstanceLocation)
			keyword := "schema"
			if finding.ErrorKind != nil && len(finding.ErrorKind.KeywordPath()) != 0 {
				keyword = jsonPointer(finding.ErrorKind.KeywordPath())
			}
			v.add(
				CodeInvalidStepConfig,
				v.nodeSource(node),
				fmt.Sprintf("node %q config at %s violates step kind %q keyword %s", node.ID, instance, spec.Name, keyword),
				"Update the node config to satisfy the registered step-kind schema.",
			)
		}
	}
}

type rejectExternalSchemaLoader struct{}

func (rejectExternalSchemaLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("external schema resource %q is unavailable to daemon-independent validation", url)
}

func plainJSON(value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if err := requireJSONEnd(decoder); err != nil {
		return nil, err
	}
	return document, nil
}

func (v *validator) invalidRegisteredSchema(node graph.Node, spec stepkind.StepKindSpec, err error) {
	v.add(
		CodeInvalidKindSchema,
		v.nodeSource(node),
		fmt.Sprintf("registered step kind %q version %q has an invalid config schema: %v", spec.Name, spec.Version, err),
		"Fix the registered step-kind config schema before validating this workflow.",
	)
}

func validationLeaves(root *jsonschema.ValidationError) []*jsonschema.ValidationError {
	if root == nil {
		return nil
	}
	if len(root.Causes) == 0 {
		return []*jsonschema.ValidationError{root}
	}
	var leaves []*jsonschema.ValidationError
	for _, cause := range root.Causes {
		leaves = append(leaves, validationLeaves(cause)...)
	}
	return leaves
}

func validationErrorKey(finding *jsonschema.ValidationError) string {
	if finding == nil {
		return ""
	}
	keyword := []string(nil)
	if finding.ErrorKind != nil {
		keyword = finding.ErrorKind.KeywordPath()
	}
	return jsonPointer(finding.InstanceLocation) + "\x00" + jsonPointer(keyword)
}

func jsonPointer(parts []string) string {
	if len(parts) == 0 {
		return "/"
	}
	escaped := make([]string, len(parts))
	for i, part := range parts {
		escaped[i] = strings.ReplaceAll(strings.ReplaceAll(part, "~", "~0"), "/", "~1")
	}
	return "/" + strings.Join(escaped, "/")
}
