package offline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	ManifestSchemaVersion = "1"
	ModeCLI               = Mode("cli")
	ModeMCPServer         = Mode("mcp-server")

	DefaultMaxManifestBytes = 8 << 20
	MaximumManifestBytes    = 64 << 20
	DefaultMaxBindingBytes  = 256 << 10
	MaximumBindings         = 256

	CodeInvalidBuild        diagnostic.Code = "HADR-HOST-030"
	CodeUnknownKind         diagnostic.Code = "HADR-HOST-031"
	CodeEmbeddedUnsupported diagnostic.Code = "HADR-HOST-032"
	CodeUnsupportedEffect   diagnostic.Code = "HADR-HOST-033"
	CodeBindingRequired     diagnostic.Code = "HADR-HOST-034"
	CodeBindingInvalid      diagnostic.Code = "HADR-HOST-035"
	CodeWaitServiceRequired diagnostic.Code = "HADR-HOST-036"
)

var (
	ErrInvalidBuild     = errors.New("invalid offline workflow build")
	ErrExecutionStalled = errors.New("offline workflow execution made no progress")
)

type Mode string

func (m Mode) Valid() bool { return m == ModeCLI || m == ModeMCPServer }

// ExternalBinding is a non-secret, node-scoped declaration for a capability
// that the generated host must actually provide. Config may contain opaque
// secret:// references, but never resolved credentials. Driver is a stable
// implementation identity and therefore participates in the build digest.
type ExternalBinding struct {
	NodeID       string          `json:"node_id"`
	Kind         string          `json:"kind"`
	Version      string          `json:"version"`
	Driver       string          `json:"driver"`
	Config       graph.Config    `json:"config,omitempty"`
	Effects      graph.EffectSet `json:"effects,omitempty"`
	Capabilities []string        `json:"capabilities,omitempty"`
}

// BindingDescription is the trusted node-scoped execution profile returned by
// the closed versioned bridge. MCP/LLM source specs conservatively advertise
// their maximum effects; ADR 0011 permits only an exact node/kind/version/config
// binding to narrow that maximum to a safe remote operation. Ordinary graph
// effect declarations never narrow either source or effective effects.
type BindingDescription struct {
	EffectiveEffects graph.EffectSet `json:"effective_effects,omitempty"`
	Capabilities     []string        `json:"capabilities,omitempty"`
	RemoteWait       bool            `json:"remote_wait,omitempty"`
}

// BindingCatalog proves that a binding is backed by a functional runtime
// bridge. Merely placing a binding-shaped object in a manifest never makes an
// otherwise unsupported node buildable.
type BindingCatalog interface {
	DescribeBinding(context.Context, ExternalBinding, graph.Node, stepkind.StepKindSpec) (BindingDescription, error)
}

// BindingCatalogFunc adapts one deterministic binding description function.
type BindingCatalogFunc func(context.Context, ExternalBinding, graph.Node, stepkind.StepKindSpec) (BindingDescription, error)

func (f BindingCatalogFunc) DescribeBinding(ctx context.Context, binding ExternalBinding, node graph.Node, spec stepkind.StepKindSpec) (BindingDescription, error) {
	return f(ctx, binding, node, spec)
}

type SchemaField struct {
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Required    bool         `json:"required,omitempty"`
	Schema      graph.Schema `json:"schema"`
}

type ResolvedBinding struct {
	Binding     ExternalBinding    `json:"binding"`
	Description BindingDescription `json:"description"`
	// SourceSpec is the exact daemon kind contract adapted by the versioned
	// remote driver. Manifest StepKinds contains the actual executable proxy
	// contract; recording both makes the adaptation explicit and reviewable.
	SourceSpec stepkind.StepKindSpec `json:"source_spec"`
	// ExecutionProfile labels the binding-backed proxy rather than presenting
	// it as the original public adapter contract. It is sent on every remote
	// invocation so the daemon can enforce the exact build-time narrowing.
	ExecutionProfile RemoteExecutionProfile `json:"execution_profile"`
}

type RemoteExecutionProfile struct {
	Driver              string          `json:"driver"`
	NodeID              string          `json:"node_id"`
	Kind                string          `json:"kind"`
	Version             string          `json:"version"`
	NodeConfigDigest    string          `json:"node_config_digest"`
	SourceSpecDigest    string          `json:"source_spec_digest"`
	ExecutionSpecDigest string          `json:"execution_spec_digest"`
	Effects             graph.EffectSet `json:"effects,omitempty"`
	Capabilities        []string        `json:"capabilities,omitempty"`
}

// Manifest is the canonical, executable offline envelope. PlanDigest is
// repeated beside Plan so inspection does not need to understand plan JSON.
// Input/Output schema projections are also repeated and exact-checked.
type Manifest struct {
	SchemaVersion string                      `json:"schema_version"`
	Mode          Mode                        `json:"mode"`
	BuildDigest   string                      `json:"build_digest"`
	PlanDigest    string                      `json:"plan_digest"`
	Plan          compile.ExecutionPlan       `json:"plan"`
	Visibility    compile.ValueVisibilityPlan `json:"visibility"`
	Inputs        []SchemaField               `json:"inputs,omitempty"`
	Outputs       []SchemaField               `json:"outputs,omitempty"`
	StepKinds     []stepkind.StepKindSpec     `json:"step_kinds"`
	Bindings      []ResolvedBinding           `json:"bindings,omitempty"`
	ToolName      string                      `json:"tool_name,omitempty"`
}

type BuildOptions struct {
	Registry stepkind.Registry
	// SourceRegistry is the authoring/daemon catalog when Registry contains
	// explicit binding-backed executable proxies. Nil means Registry.
	SourceRegistry   stepkind.Registry
	Bindings         []ExternalBinding
	BindingCatalog   BindingCatalog
	Mode             Mode
	ToolName         string
	MaxManifestBytes int
	MaxBindingBytes  int
}

type BuildResult struct {
	Manifest    *Manifest
	Bytes       []byte
	Diagnostics []diagnostic.Diagnostic
}

// Build validates and canonicalizes an already compiled plan. Diagnostics are
// stable and source-mapped; no artifact is returned when any diagnostic exists.
func Build(ctx context.Context, plan *compile.ExecutionPlan, options BuildOptions) (BuildResult, error) {
	if ctx == nil {
		return BuildResult{}, fmt.Errorf("%w: context is required", ErrInvalidBuild)
	}
	if err := ctx.Err(); err != nil {
		return BuildResult{}, err
	}
	maxManifest := options.MaxManifestBytes
	if maxManifest == 0 {
		maxManifest = DefaultMaxManifestBytes
	}
	maxBinding := options.MaxBindingBytes
	if maxBinding == 0 {
		maxBinding = DefaultMaxBindingBytes
	}
	if plan == nil || nilRegistry(options.Registry) || !options.Mode.Valid() || maxManifest < 1 || maxManifest > MaximumManifestBytes || maxBinding < 1 || maxBinding > DefaultMaxManifestBytes {
		return BuildResult{Diagnostics: []diagnostic.Diagnostic{buildDiagnostic(CodeInvalidBuild, nil, "offline build options are incomplete or out of bounds", "Supply a compiled plan, exact registry, valid output mode, and bounded size limits.")}}, nil
	}
	clonedPlan, err := cloneJSON(*plan)
	if err != nil {
		return BuildResult{}, fmt.Errorf("%w: clone plan: %w", ErrInvalidBuild, err)
	}
	sourceRegistry := options.SourceRegistry
	if sourceRegistry == nil {
		sourceRegistry = options.Registry
	} else if nilRegistry(sourceRegistry) {
		return BuildResult{Diagnostics: []diagnostic.Diagnostic{buildDiagnostic(CodeInvalidBuild, nil, "offline source registry is typed nil", "Supply the exact immutable source registry.")}}, nil
	}
	base := compile.ValidatePlan(ctx, &clonedPlan, compile.ValidationOptions{StepKinds: sourceRegistry})
	inferred := compile.InferValueDependencies(&clonedPlan, compile.DependencyOptions{})
	base = append(base, inferred.Diagnostics...)
	if inferred.Plan == nil {
		if len(base) == 0 {
			base = append(base, buildDiagnostic(CodeInvalidBuild, clonedPlan.Graph.Source, "workflow value dependencies could not be inferred", "Fix workflow bindings before building an offline artifact."))
		}
		sortDiagnostics(base)
		return BuildResult{Diagnostics: base}, nil
	}
	clonedPlan = *inferred.Plan

	bindings, bindingFindings, err := normalizeBindings(options.Bindings, maxBinding)
	if err != nil {
		return BuildResult{}, err
	}
	findings := append(base, bindingFindings...)
	resolved, specs, compatibility := validateCompatibility(ctx, clonedPlan.Graph, options.Registry, sourceRegistry, bindings, options.BindingCatalog)
	findings = append(findings, compatibility...)
	findings = append(findings, compile.ValidatePlan(ctx, &clonedPlan, compile.ValidationOptions{StepKinds: options.Registry})...)
	if len(findings) != 0 {
		sortDiagnostics(findings)
		return BuildResult{Diagnostics: findings}, nil
	}
	inputs, err := inputFields(clonedPlan.Graph.Inputs)
	if err != nil {
		return BuildResult{}, err
	}
	outputs, err := outputFields(clonedPlan.Graph.Outputs)
	if err != nil {
		return BuildResult{}, err
	}
	toolName := strings.TrimSpace(options.ToolName)
	if options.Mode == ModeMCPServer {
		if toolName == "" {
			toolName = clonedPlan.ID
		}
		if toolErr := validateIdentifier("tool name", toolName); toolErr != nil {
			return BuildResult{Diagnostics: []diagnostic.Diagnostic{buildDiagnostic(CodeInvalidBuild, clonedPlan.Graph.Source, toolErr.Error(), "Use a stable non-secret tool identifier.")}}, nil
		}
	} else if toolName != "" {
		return BuildResult{Diagnostics: []diagnostic.Diagnostic{buildDiagnostic(CodeInvalidBuild, clonedPlan.Graph.Source, "tool_name is only valid for mcp-server artifacts", "Remove tool_name or select mcp-server mode.")}}, nil
	}
	manifest := Manifest{
		SchemaVersion: ManifestSchemaVersion, Mode: options.Mode, PlanDigest: clonedPlan.Digest,
		Plan: clonedPlan, Visibility: inferred.Visibility, Inputs: inputs, Outputs: outputs,
		StepKinds: specs, Bindings: resolved, ToolName: toolName,
	}
	manifest.BuildDigest, err = digestManifest(manifest)
	if err != nil {
		return BuildResult{}, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return BuildResult{}, fmt.Errorf("%w: encode manifest: %w", ErrInvalidBuild, err)
	}
	if len(encoded) > maxManifest {
		return BuildResult{Diagnostics: []diagnostic.Diagnostic{buildDiagnostic(CodeInvalidBuild, clonedPlan.Graph.Source, "offline manifest exceeds the configured size bound", "Reduce embedded schemas/configuration or raise the explicit bounded limit.")}}, nil
	}
	return BuildResult{Manifest: &manifest, Bytes: bytes.Clone(encoded)}, nil
}

func ParseManifest(data []byte) (Manifest, error) {
	if len(data) == 0 || len(data) > MaximumManifestBytes {
		return Manifest{}, fmt.Errorf("%w: manifest size is invalid", ErrInvalidBuild)
	}
	var manifest Manifest
	if err := decodeStrictJSON(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("%w: decode manifest: %w", ErrInvalidBuild, err)
	}
	canonical, err := json.Marshal(manifest)
	if err != nil || !bytes.Equal(canonical, data) {
		return Manifest{}, fmt.Errorf("%w: manifest must use canonical JSON encoding", ErrInvalidBuild)
	}
	if identityErr := validateManifestIdentity(manifest); identityErr != nil {
		return Manifest{}, identityErr
	}
	want := manifest.BuildDigest
	manifest.BuildDigest = ""
	digest, err := digestManifest(manifest)
	if err != nil {
		return Manifest{}, err
	}
	manifest.BuildDigest = want
	if want != digest {
		return Manifest{}, fmt.Errorf("%w: manifest identity is inconsistent", ErrInvalidBuild)
	}
	return manifest, nil
}

func validateManifestIdentity(manifest Manifest) error {
	if manifest.SchemaVersion != ManifestSchemaVersion || !manifest.Mode.Valid() || (manifest.Mode == ModeMCPServer) != (manifest.ToolName != "") {
		return fmt.Errorf("%w: manifest schema or mode is inconsistent", ErrInvalidBuild)
	}
	if err := values.ValidateDigest(manifest.BuildDigest); err != nil {
		return fmt.Errorf("%w: invalid build digest: %w", ErrInvalidBuild, err)
	}
	if err := values.ValidateDigest(manifest.PlanDigest); err != nil {
		return fmt.Errorf("%w: invalid plan digest: %w", ErrInvalidBuild, err)
	}
	if manifest.PlanDigest != manifest.Plan.Digest {
		return fmt.Errorf("%w: repeated plan digest differs", ErrInvalidBuild)
	}
	planDigest, err := compile.PlanDigest(manifest.Plan)
	if err != nil || planDigest != manifest.Plan.Digest {
		return fmt.Errorf("%w: embedded plan content differs from its digest", ErrInvalidBuild)
	}
	if enumErr := manifest.Plan.Graph.ValidateEnums(); enumErr != nil {
		return fmt.Errorf("%w: invalid graph: %w", ErrInvalidBuild, enumErr)
	}
	graphDigest, err := compile.GraphDigest(manifest.Plan.Graph)
	if err != nil || graphDigest != manifest.Plan.Graph.Digest {
		return fmt.Errorf("%w: embedded graph content differs from its digest", ErrInvalidBuild)
	}
	foundSource := false
	for _, source := range manifest.Plan.SourceDigests {
		if source.Digest == manifest.Plan.Definition.Digest {
			foundSource = true
			break
		}
	}
	if !foundSource {
		return fmt.Errorf("%w: definition digest is absent from authoritative source digests", ErrInvalidBuild)
	}
	inputs, err := inputFields(manifest.Plan.Graph.Inputs)
	wantInputs, _ := json.Marshal(manifest.Inputs)
	actualInputs, _ := json.Marshal(inputs)
	if err != nil || (len(inputs) != 0 || len(manifest.Inputs) != 0) && !bytes.Equal(actualInputs, wantInputs) {
		return fmt.Errorf("%w: input schema projection differs from the plan", ErrInvalidBuild)
	}
	outputs, err := outputFields(manifest.Plan.Graph.Outputs)
	wantOutputs, _ := json.Marshal(manifest.Outputs)
	actualOutputs, _ := json.Marshal(outputs)
	if err != nil || (len(outputs) != 0 || len(manifest.Outputs) != 0) && !bytes.Equal(actualOutputs, wantOutputs) {
		return fmt.Errorf("%w: output schema projection differs from the plan", ErrInvalidBuild)
	}
	inferred := compile.InferValueDependencies(&manifest.Plan, compile.DependencyOptions{})
	wantVisibility, _ := json.Marshal(manifest.Visibility)
	actualVisibility, _ := json.Marshal(inferred.Visibility)
	if inferred.Plan == nil || len(inferred.Diagnostics) != 0 || !bytes.Equal(actualVisibility, wantVisibility) {
		return fmt.Errorf("%w: value dependency or visibility metadata differs from the plan", ErrInvalidBuild)
	}
	if manifest.Mode == ModeMCPServer {
		if err := validateIdentifier("tool name", manifest.ToolName); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidBuild, err)
		}
	}
	return validateManifestCatalog(manifest)
}

func validateManifestCatalog(manifest Manifest) error {
	bySpec := make(map[string]stepkind.StepKindSpec, len(manifest.StepKinds))
	previous := ""
	for _, spec := range manifest.StepKinds {
		key := spec.Name + "\x00" + spec.Version
		if key <= previous || stepkind.ValidateSpec(spec) != nil {
			return fmt.Errorf("%w: step-kind catalog is invalid, duplicated, or unsorted", ErrInvalidBuild)
		}
		previous = key
		bySpec[key] = spec
	}
	usedSpecs := make(map[string]struct{})
	nodes := make(map[string]graph.Node, len(manifest.Plan.Graph.Nodes))
	for _, node := range manifest.Plan.Graph.Nodes {
		key := node.Kind + "\x00" + node.KindVersion
		if _, ok := bySpec[key]; !ok {
			return fmt.Errorf("%w: node %q is absent from the exact step-kind catalog", ErrInvalidBuild, node.ID)
		}
		usedSpecs[key] = struct{}{}
		nodes[node.ID] = node
	}
	if len(usedSpecs) != len(bySpec) {
		return fmt.Errorf("%w: step-kind catalog contains unused registrations", ErrInvalidBuild)
	}
	previous = ""
	seenBindings := make(map[string]struct{}, len(manifest.Bindings))
	for _, resolved := range manifest.Bindings {
		binding := resolved.Binding
		if binding.NodeID <= previous {
			return fmt.Errorf("%w: bindings are duplicated or unsorted", ErrInvalidBuild)
		}
		previous = binding.NodeID
		node, ok := nodes[binding.NodeID]
		if !ok || node.Kind != binding.Kind || node.KindVersion != binding.Version {
			return fmt.Errorf("%w: binding does not match an exact node", ErrInvalidBuild)
		}
		if err := validateBinding(binding); err != nil {
			return fmt.Errorf("%w: invalid embedded binding: %w", ErrInvalidBuild, err)
		}
		encoded, err := json.Marshal(binding)
		if err != nil || len(encoded) > DefaultMaxManifestBytes {
			return fmt.Errorf("%w: embedded binding exceeds its bound", ErrInvalidBuild)
		}
		actual := bySpec[binding.Kind+"\x00"+binding.Version]
		if resolved.SourceSpec.Name != binding.Kind || resolved.SourceSpec.Version != binding.Version || stepkind.ValidateSpec(resolved.SourceSpec) != nil {
			return fmt.Errorf("%w: binding source spec is invalid", ErrInvalidBuild)
		}
		wantDescription := normalizeDescription(BindingDescription{EffectiveEffects: unionEffects(binding.Effects, node.Effects), Capabilities: binding.Capabilities, RemoteWait: resolved.SourceSpec.CanSuspend})
		if !reflect.DeepEqual(wantDescription, resolved.Description) || validateDescription(resolved.Description) != nil || !containsEffects(actual.Effects, wantDescription.EffectiveEffects) || actual.Observation.Mode != stepkind.ObservationPoll || actual.CanSuspend || !actual.EmbeddedModeSupported {
			return fmt.Errorf("%w: binding execution adaptation is inconsistent", ErrInvalidBuild)
		}
		wantProfile, err := buildExecutionProfile(node, binding, resolved.SourceSpec, actual, wantDescription)
		if err != nil || !reflect.DeepEqual(wantProfile, resolved.ExecutionProfile) {
			return fmt.Errorf("%w: remote execution profile differs from its exact node or specs", ErrInvalidBuild)
		}
		seenBindings[binding.NodeID] = struct{}{}
	}
	for _, node := range manifest.Plan.Graph.Nodes {
		_, bound := seenBindings[node.ID]
		spec := bySpec[node.Kind+"\x00"+node.KindVersion]
		if bound != (spec.Observation.Mode == stepkind.ObservationPoll) {
			return fmt.Errorf("%w: binding coverage differs from executable kind metadata", ErrInvalidBuild)
		}
	}
	return nil
}

func digestManifest(manifest Manifest) (string, error) {
	manifest.BuildDigest = ""
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("%w: canonicalize manifest: %w", ErrInvalidBuild, err)
	}
	return values.SHA256Digest(encoded), nil
}

func cloneJSON[T any](input T) (T, error) {
	var output T
	encoded, err := json.Marshal(input)
	if err != nil {
		return output, err
	}
	if err := decodeJSON(encoded, &output); err != nil {
		return output, err
	}
	return output, nil
}

func decodeJSON(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func decodeStrictJSON(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func nilRegistry(registry stepkind.Registry) bool {
	if registry == nil {
		return true
	}
	value := reflect.ValueOf(registry)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func buildDiagnostic(code diagnostic.Code, source *graph.SourceRef, message, remediation string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{Severity: diagnostic.SeverityError, Code: code, Message: message, Source: cloneSource(source), Remediation: &diagnostic.Remediation{Message: remediation}}
}

func cloneSource(source *graph.SourceRef) *graph.SourceRef {
	if source == nil {
		return nil
	}
	cloned := *source
	cloned.Path = append([]string(nil), source.Path...)
	return &cloned
}

func sortDiagnostics(findings []diagnostic.Diagnostic) {
	sort.SliceStable(findings, func(i, j int) bool {
		left, right := findings[i], findings[j]
		leftLocator, rightLocator := "", ""
		leftLine, rightLine := 0, 0
		if left.Source != nil {
			leftLocator, leftLine = left.Source.Locator, left.Source.StartLine
		}
		if right.Source != nil {
			rightLocator, rightLine = right.Source.Locator, right.Source.StartLine
		}
		if leftLocator != rightLocator {
			return leftLocator < rightLocator
		}
		if leftLine != rightLine {
			return leftLine < rightLine
		}
		if left.Code != right.Code {
			return left.Code < right.Code
		}
		return left.Message < right.Message
	})
}

func validateIdentifier(label, value string) error {
	if value == "" || len(value) > 128 || !utf8.ValidString(value) || !offlineIdentifierPattern.MatchString(value) {
		return fmt.Errorf("%s must match %s and be at most 128 bytes", label, offlineIdentifierPattern.String())
	}
	return nil
}
