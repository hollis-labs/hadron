package generatedchild

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

// ProcessorOptions freezes the same compiler expanders and validator catalogs
// used by an application host. Callers must treat supplied registry/policy
// collaborators as immutable for the processor's lifetime.
type ProcessorOptions struct {
	Compile    workflowcompile.CompileOptions
	Dependency workflowcompile.DependencyOptions
	Validation workflowcompile.ValidationOptions
}

// CoreProcessor is the repository-native implementation of Processor.
type CoreProcessor struct{ options ProcessorOptions }

// NewProcessor defensively copies collection options. Interface-backed
// catalogs and hooks remain host-owned immutable collaborators.
func NewProcessor(options ProcessorOptions) *CoreProcessor {
	options.Compile.NodeExpanders = append([]workflowcompile.NodeExpander(nil), options.Compile.NodeExpanders...)
	options.Validation.PolicyHooks = append([]workflowcompile.PolicyHook(nil), options.Validation.PolicyHooks...)
	if options.Dependency.VerificationExtractors != nil {
		extractors := make(map[string]workflowcompile.VerificationExpressionExtractor, len(options.Dependency.VerificationExtractors))
		for kind, extractor := range options.Dependency.VerificationExtractors {
			extractors[kind] = extractor
		}
		options.Dependency.VerificationExtractors = extractors
	}
	return &CoreProcessor{options: options}
}

func (p *CoreProcessor) ProcessGenerated(ctx context.Context, request ProcessRequest) (ProcessedDefinition, error) {
	if ctx == nil || p == nil || !request.Format.Valid() || !stableText(request.Authority, maxStableBytes) {
		return ProcessedDefinition{}, fmt.Errorf("%w: processor request is invalid", ErrInvalidMaterial)
	}
	if err := ctx.Err(); err != nil {
		return ProcessedDefinition{}, err
	}
	if err := request.Identity.Validate(); err != nil {
		return ProcessedDefinition{}, fmt.Errorf("%w: identity: %w", ErrInvalidMaterial, err)
	}
	if err := request.Value.Validate(); err != nil || request.Value.Redaction == values.RedactionSecret || request.Value.Retention == values.RetentionNone {
		return ProcessedDefinition{}, fmt.Errorf("%w: material value is not safely persistable", ErrInvalidMaterial)
	}

	var value graph.Graph
	switch request.Format {
	case FormatWorkflowSource:
		source, ok := request.Value.Inline.(string)
		if request.Value.Type != values.TypeString || !ok || len(source) == 0 || len(source) > maxMaterialBytes {
			return ProcessedDefinition{}, fmt.Errorf("%w: workflow source must be a bounded inline string", ErrInvalidMaterial)
		}
		if secretShaped(source) {
			return ProcessedDefinition{}, fmt.Errorf("%w: generated source contains forbidden secret-shaped material", ErrInvalidMaterial)
		}
		locator := "generated-" + strings.TrimPrefix(request.Value.Digest, "sha256:") + ".workflow.yaml"
		loaded := workflowcompile.LoadBytes(locator, []byte(source))
		if loaded.Source == nil || len(loaded.Diagnostics) != 0 {
			return ProcessedDefinition{}, fmt.Errorf("%w: generated source could not be loaded", ErrInvalidMaterial)
		}
		compiled := workflowcompile.CompileWithOptions(loaded.Source, p.options.Compile)
		if compiled.Plan == nil || len(compiled.Diagnostics) != 0 {
			return ProcessedDefinition{}, fmt.Errorf("%w: generated source could not be compiled", ErrInvalidMaterial)
		}
		inferred := workflowcompile.InferValueDependencies(compiled.Plan, p.options.Dependency)
		if inferred.Plan == nil || len(inferred.Diagnostics) != 0 {
			return ProcessedDefinition{}, fmt.Errorf("%w: generated source dependency inference failed", ErrInvalidMaterial)
		}
		if findings := workflowcompile.ValidatePlan(ctx, inferred.Plan, p.options.Validation); len(findings) != 0 {
			return ProcessedDefinition{}, fmt.Errorf("%w: generated source failed graph validation", ErrInvalidMaterial)
		}
		value = inferred.Plan.Graph
	case FormatGraphIR:
		if request.Value.Type != values.TypeObject {
			return ProcessedDefinition{}, fmt.Errorf("%w: graph IR must be a bounded inline object", ErrInvalidMaterial)
		}
		encoded, err := json.Marshal(request.Value.Inline)
		if err != nil || len(encoded) == 0 || len(encoded) > maxMaterialBytes || secretShaped(string(encoded)) {
			return ProcessedDefinition{}, fmt.Errorf("%w: graph IR is invalid or contains secret-shaped material", ErrInvalidMaterial)
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.UseNumber()
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&value); err != nil {
			return ProcessedDefinition{}, fmt.Errorf("%w: decode graph IR", ErrInvalidMaterial)
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			return ProcessedDefinition{}, fmt.Errorf("%w: graph IR has trailing data", ErrInvalidMaterial)
		}
		if findings := workflowcompile.ValidateGraph(ctx, value, p.options.Validation); len(findings) != 0 {
			return ProcessedDefinition{}, fmt.Errorf("%w: graph IR failed validation", ErrInvalidMaterial)
		}
	}
	if len(value.Nodes) == 0 || len(value.Nodes) > maxNodes || len(value.Edges) > maxEdges {
		return ProcessedDefinition{}, fmt.Errorf("%w: generated graph exceeds structural bounds", ErrInvalidMaterial)
	}
	if !stableText(value.Version, 256) {
		return ProcessedDefinition{}, fmt.Errorf("%w: generated graph version is invalid", ErrInvalidMaterial)
	}
	digest, err := workflowcompile.GraphDigest(value)
	if err != nil {
		return ProcessedDefinition{}, fmt.Errorf("%w: digest generated graph", ErrInvalidMaterial)
	}
	if value.Digest != "" && value.Digest != digest {
		return ProcessedDefinition{}, fmt.Errorf("%w: generated graph digest does not match its contents", ErrInvalidMaterial)
	}
	originDigest := request.Value.Digest
	value.Digest = digest
	value.Provenance = graph.Provenance{
		Authority: request.Authority, Origin: "generated-child", Locator: "generated:" + strings.TrimPrefix(digest, "sha256:"),
		Revision: value.Version, Digest: digest,
		Parents: []graph.ProvenanceRef{{Authority: "workflow-value", Locator: "value-digest", Digest: originDigest}},
	}
	provenance := value.Provenance
	definition := graph.DefinitionRef{
		Authority: request.Authority, Kind: "workflow", ID: value.ID, Locator: value.Provenance.Locator,
		Version: value.Version, Digest: digest, Provenance: &provenance,
	}
	resolved, err := cloneResolved(workflowcompile.ResolvedDefinition{Definition: definition, Graph: value})
	if err != nil {
		return ProcessedDefinition{}, err
	}
	policy, err := p.policySummary(value)
	if err != nil {
		return ProcessedDefinition{}, err
	}
	return ProcessedDefinition{Definition: resolved, Policy: policy}, nil
}

func (p *CoreProcessor) policySummary(value graph.Graph) (PolicySummary, error) {
	if p.options.Validation.StepKinds == nil {
		return PolicySummary{}, fmt.Errorf("%w: frozen step-kind catalog is required", ErrInvalidMaterial)
	}
	effects := make(map[graph.Effect]struct{})
	capabilities := make(map[string]struct{})
	configDigests := make(map[string]string, len(value.Nodes))
	for _, node := range value.Nodes {
		spec, resolveErr := resolvePolicySpec(p.options.Validation.StepKinds, node.Kind, node.KindVersion)
		if resolveErr != nil {
			return PolicySummary{}, fmt.Errorf("%w: exact registered kind is unavailable", ErrInvalidMaterial)
		}
		for _, effect := range append(append(graph.EffectSet(nil), node.Effects...), spec.Effects...) {
			effects[effect] = struct{}{}
		}
		for _, capability := range spec.RequiredCapabilities {
			capabilities[capability] = struct{}{}
		}
		encoded, err := json.Marshal(node.Config)
		if err != nil {
			return PolicySummary{}, fmt.Errorf("%w: node config digest", ErrInvalidMaterial)
		}
		digest := sha256.Sum256(encoded)
		configDigests[node.ID] = "sha256:" + hex.EncodeToString(digest[:])
	}
	result := PolicySummary{ConfigDigests: configDigests}
	for effect := range effects {
		result.Effects = append(result.Effects, effect)
	}
	for capability := range capabilities {
		result.RequiredCapabilities = append(result.RequiredCapabilities, capability)
	}
	sort.Slice(result.Effects, func(i, j int) bool { return result.Effects[i] < result.Effects[j] })
	sort.Strings(result.RequiredCapabilities)
	return result, nil
}

func resolvePolicySpec(lookup workflowcompile.StepKindLookup, name, version string) (stepkind.StepKindSpec, error) {
	if lookup == nil {
		return stepkind.StepKindSpec{}, ErrInvalidMaterial
	}
	matches := make([]stepkind.StepKindSpec, 0, 1)
	for _, candidate := range lookup.List() {
		if candidate.Name == name && (version == "" || candidate.Version == version) {
			matches = append(matches, candidate)
		}
	}
	if len(matches) != 1 {
		return stepkind.StepKindSpec{}, ErrInvalidMaterial
	}
	kind, ok := lookup.Lookup(name, matches[0].Version)
	if !ok || kind == nil || !reflect.DeepEqual(kind.Spec(), matches[0]) {
		return stepkind.StepKindSpec{}, ErrInvalidMaterial
	}
	return matches[0], nil
}

func secretShaped(value string) bool {
	lower := strings.ToLower(value)
	for _, marker := range []string{"secret://", "bearer ", "authorization:", "password:", "api_key:", "api-key:", "private_key:", "client_secret:"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func cloneResolved(value workflowcompile.ResolvedDefinition) (workflowcompile.ResolvedDefinition, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return workflowcompile.ResolvedDefinition{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var cloned workflowcompile.ResolvedDefinition
	if err := decoder.Decode(&cloned); err != nil {
		return workflowcompile.ResolvedDefinition{}, err
	}
	return cloned, nil
}

func definitionEqual(left, right workflowcompile.ResolvedDefinition) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
