package offline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

var offlineIdentifierPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`)

func normalizeBindings(input []ExternalBinding, maxBytes int) ([]ExternalBinding, []diagnostic.Diagnostic, error) {
	if len(input) > MaximumBindings {
		return nil, []diagnostic.Diagnostic{buildDiagnostic(CodeBindingInvalid, nil, "offline build contains too many external bindings", "Reduce bindings to the bounded node-scoped set.")}, nil
	}
	bindings := make([]ExternalBinding, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	var findings []diagnostic.Diagnostic
	for _, raw := range input {
		binding, err := cloneJSON(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: clone binding: %w", ErrInvalidBuild, err)
		}
		binding.NodeID = strings.TrimSpace(binding.NodeID)
		binding.Kind = strings.TrimSpace(binding.Kind)
		binding.Version = strings.TrimSpace(binding.Version)
		binding.Driver = strings.TrimSpace(binding.Driver)
		sort.Strings(binding.Capabilities)
		binding.Effects = unionEffects(binding.Effects, nil)
		encoded, err := json.Marshal(binding)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: encode binding: %w", ErrInvalidBuild, err)
		}
		if len(encoded) > maxBytes {
			findings = append(findings, buildDiagnostic(CodeBindingInvalid, nil, fmt.Sprintf("external binding for node %q exceeds the configured size bound", binding.NodeID), "Reduce non-secret binding metadata."))
			continue
		}
		if err := validateBinding(binding); err != nil {
			findings = append(findings, buildDiagnostic(CodeBindingInvalid, nil, fmt.Sprintf("external binding for node %q is invalid: %v", binding.NodeID, err), "Use an exact node/kind/version, stable driver, canonical capabilities, and opaque secret references."))
			continue
		}
		if _, duplicate := seen[binding.NodeID]; duplicate {
			findings = append(findings, buildDiagnostic(CodeBindingInvalid, nil, fmt.Sprintf("external binding for node %q is duplicated", binding.NodeID), "Declare exactly one binding per node."))
			continue
		}
		seen[binding.NodeID] = struct{}{}
		bindings = append(bindings, binding)
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].NodeID < bindings[j].NodeID })
	return bindings, findings, nil
}

func validateBinding(binding ExternalBinding) error {
	for label, value := range map[string]string{"node id": binding.NodeID, "kind": binding.Kind, "version": binding.Version, "driver": binding.Driver} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	for _, effect := range binding.Effects {
		if !effect.Valid() {
			return fmt.Errorf("unsupported effect %q", effect)
		}
	}
	if hasUnsafeEffects(binding.Effects) {
		return fmt.Errorf("remote binding cannot authorize mutate or destructive effects")
	}
	for index, capability := range binding.Capabilities {
		if err := validateIdentifier("capability", capability); err != nil {
			return err
		}
		if index > 0 && binding.Capabilities[index-1] == capability {
			return fmt.Errorf("capability %q is duplicated", capability)
		}
	}
	return validateBindingSecrets(binding.Config, "config", "")
}

func validateBindingSecrets(value any, path, key string) error {
	switch typed := value.(type) {
	case nil, bool, json.Number:
		return nil
	case string:
		lower := strings.ToLower(strings.TrimSpace(typed))
		if strings.HasPrefix(lower, "secret://") {
			if _, err := values.ParseSecretRef(typed); err != nil {
				return fmt.Errorf("%s contains a malformed secret reference", path)
			}
			return nil
		}
		if strings.Contains(lower, "secret://") || strings.Contains(lower, "bearer ") || strings.Contains(lower, "basic ") || credentialAssignment(lower) {
			return fmt.Errorf("%s contains credential-shaped material", path)
		}
		if parsed, err := url.Parse(typed); err == nil && parsed.IsAbs() && (parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "") {
			return fmt.Errorf("%s contains credential-bearing or mutable URI components", path)
		}
		if sensitiveKey(key) && !strings.HasPrefix(typed, "secret://") {
			return fmt.Errorf("%s contains literal credential-shaped data", path)
		}
		return nil
	case []any:
		for index, item := range typed {
			if err := validateBindingSecrets(item, fmt.Sprintf("%s[%d]", path, index), key); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for child := range typed {
			keys = append(keys, child)
		}
		sort.Strings(keys)
		for _, child := range keys {
			if err := validateBindingSecrets(typed[child], path+"."+child, child); err != nil {
				return err
			}
		}
		return nil
	case graph.Config:
		plain := make(map[string]any, len(typed))
		for child, item := range typed {
			plain[child] = item
		}
		return validateBindingSecrets(plain, path, key)
	default:
		return fmt.Errorf("%s contains unsupported value %T", path, value)
	}
}

func sensitiveKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", "_"), ".", "_"))
	for _, marker := range []string{"secret", "token", "password", "credential", "authorization", "auth", "api_key", "private_key", "access_key"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func credentialAssignment(value string) bool {
	normalized := strings.NewReplacer("-", "_", ".", "_", " ", "_").Replace(value)
	for _, marker := range []string{"token=", "token:", "password=", "password:", "secret=", "secret:", "api_key=", "api_key:", "authorization=", "authorization:"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func validateCompatibility(ctx context.Context, workflow graph.Graph, registry, sourceRegistry stepkind.Registry, bindings []ExternalBinding, catalog BindingCatalog) ([]ResolvedBinding, []stepkind.StepKindSpec, []diagnostic.Diagnostic) {
	byNode := make(map[string]ExternalBinding, len(bindings))
	for _, binding := range bindings {
		byNode[binding.NodeID] = binding
	}
	used := make(map[string]struct{}, len(bindings))
	specByKey := make(map[string]stepkind.StepKindSpec)
	var resolved []ResolvedBinding
	var findings []diagnostic.Diagnostic
	nodes := append([]graph.Node(nil), workflow.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	for _, node := range nodes {
		if strings.TrimSpace(node.KindVersion) == "" {
			findings = append(findings, buildDiagnostic(CodeUnknownKind, node.Source, fmt.Sprintf("node %q must pin an exact step-kind version for an offline artifact", node.ID), "Set kind_version to the exact registered immutable version."))
			continue
		}
		_, spec, err := stepkind.Resolve(registry, node.Kind, node.KindVersion)
		if err != nil {
			findings = append(findings, buildDiagnostic(CodeUnknownKind, node.Source, fmt.Sprintf("node %q requires unavailable exact step kind %s@%s", node.ID, node.Kind, node.KindVersion), "Register the exact immutable step-kind version before building."))
			continue
		}
		_, sourceSpec, sourceErr := stepkind.Resolve(sourceRegistry, node.Kind, node.KindVersion)
		if sourceErr != nil {
			findings = append(findings, buildDiagnostic(CodeUnknownKind, node.Source, fmt.Sprintf("node %q requires unavailable source step kind %s@%s", node.ID, node.Kind, node.KindVersion), "Register the exact immutable source step-kind version before building."))
			continue
		}
		binding, hasBinding := byNode[node.ID]
		if !spec.EmbeddedModeSupported {
			findings = append(findings, buildDiagnostic(CodeEmbeddedUnsupported, node.Source, fmt.Sprintf("node %q uses %s@%s which does not support embedded mode", node.ID, spec.Name, spec.Version), "Choose an embedded-capable kind or run the workflow through the daemon."))
			continue
		}
		specByKey[spec.Name+"\x00"+spec.Version] = spec
		needsBinding := sourceSpec.CanSuspend || len(sourceSpec.RequiredCapabilities) != 0 || hasUnsafeEffects(sourceSpec.Effects) || node.Kind == "mcp" || node.Kind == "llm"
		description := BindingDescription{EffectiveEffects: unionEffects(spec.Effects, node.Effects), Capabilities: append([]string(nil), spec.RequiredCapabilities...)}
		if needsBinding {
			code := CodeBindingRequired
			message := fmt.Sprintf("node %q requires an explicit functional external binding", node.ID)
			if sourceSpec.CanSuspend {
				code = CodeWaitServiceRequired
				message = fmt.Sprintf("suspend-capable node %q requires an explicit remote-daemon wait binding", node.ID)
			}
			if !hasBinding {
				findings = append(findings, buildDiagnostic(code, node.Source, message, "Configure a node-scoped bridge or execute through the daemon."))
				continue
			}
			used[node.ID] = struct{}{}
			if binding.Kind != spec.Name || binding.Version != spec.Version {
				findings = append(findings, buildDiagnostic(CodeBindingInvalid, node.Source, fmt.Sprintf("binding for node %q targets %s@%s instead of %s@%s", node.ID, binding.Kind, binding.Version, spec.Name, spec.Version), "Bind the exact registered node kind and version."))
				continue
			}
			description = normalizeDescription(BindingDescription{EffectiveEffects: unionEffects(binding.Effects, node.Effects), Capabilities: binding.Capabilities, RemoteWait: sourceSpec.CanSuspend})
			if hasUnsafeEffects(spec.Effects) {
				findings = append(findings, buildDiagnostic(CodeUnsupportedEffect, node.Source, fmt.Sprintf("node %q has mutate or destructive executable effects outside the offline subset", node.ID), "Use an explicit safe remote execution profile or run through a policy-enforced daemon host."))
				continue
			}
			if hasUnsafeEffects(description.EffectiveEffects) {
				findings = append(findings, buildDiagnostic(CodeUnsupportedEffect, node.Source, fmt.Sprintf("node %q has mutate or destructive effects outside the offline subset", node.ID), "Run consequential work through a policy-enforced daemon host."))
				continue
			}
			if spec.Observation.Mode != stepkind.ObservationPoll || spec.CanSuspend || !spec.EmbeddedModeSupported {
				findings = append(findings, buildDiagnostic(CodeBindingInvalid, node.Source, fmt.Sprintf("binding for node %q is not backed by an explicit polling execution proxy", node.ID), "Adapt the source catalog through the versioned remote driver before building."))
				continue
			}
			if err := validateRemoteDescription(binding, description); err != nil {
				findings = append(findings, buildDiagnostic(CodeBindingInvalid, node.Source, fmt.Sprintf("binding for node %q does not select a functional closed driver: %v", node.ID, err), "Use the supported remote-daemon driver and bounded credential-free endpoint configuration."))
				continue
			}
			if catalog != nil {
				catalogDescription, describeErr := catalog.DescribeBinding(ctx, binding, node, sourceSpec)
				catalogDescription = normalizeDescription(catalogDescription)
				if describeErr != nil || validateDescription(catalogDescription) != nil || !equalEffects(catalogDescription.EffectiveEffects, description.EffectiveEffects) || !equalStrings(catalogDescription.Capabilities, description.Capabilities) || catalogDescription.RemoteWait != description.RemoteWait {
					findings = append(findings, buildDiagnostic(CodeBindingInvalid, node.Source, fmt.Sprintf("binding for node %q was rejected or changed by its runtime catalog", node.ID), "Keep the catalog description identical to the canonical closed driver contract."))
					continue
				}
			}
			if sourceSpec.CanSuspend && !description.RemoteWait {
				findings = append(findings, buildDiagnostic(CodeWaitServiceRequired, node.Source, fmt.Sprintf("binding for node %q does not provide a remote wait service", node.ID), "Use a functional remote-daemon wait bridge."))
				continue
			}
			profile, profileErr := buildExecutionProfile(node, binding, sourceSpec, spec, description)
			if profileErr != nil {
				findings = append(findings, buildDiagnostic(CodeBindingInvalid, node.Source, fmt.Sprintf("binding for node %q could not be pinned to an execution profile", node.ID), "Use canonical non-secret node and executor metadata."))
				continue
			}
			resolved = append(resolved, ResolvedBinding{Binding: binding, Description: description, SourceSpec: sourceSpec, ExecutionProfile: profile})
		}
		effective := unionEffects(description.EffectiveEffects, node.Effects)
		if hasUnsafeEffects(effective) {
			findings = append(findings, buildDiagnostic(CodeUnsupportedEffect, node.Source, fmt.Sprintf("node %q has mutate or destructive effects outside the offline subset", node.ID), "Run consequential work through a policy-enforced daemon host."))
		}
		if !containsAll(description.Capabilities, spec.RequiredCapabilities) {
			findings = append(findings, buildDiagnostic(CodeBindingInvalid, node.Source, fmt.Sprintf("binding for node %q does not provide every required capability", node.ID), "Declare and install every exact capability required by the registered kind."))
		}
	}
	for _, binding := range bindings {
		if _, ok := used[binding.NodeID]; !ok {
			if _, exists := graphNode(workflow, binding.NodeID); !exists {
				findings = append(findings, buildDiagnostic(CodeBindingInvalid, nil, fmt.Sprintf("binding targets unknown node %q", binding.NodeID), "Remove stale bindings or target an exact graph node."))
			} else {
				findings = append(findings, buildDiagnostic(CodeBindingInvalid, nil, fmt.Sprintf("binding targets pure node %q which requires no external bridge", binding.NodeID), "Remove unused bindings so every build input participates truthfully."))
			}
		}
	}
	specs := make([]stepkind.StepKindSpec, 0, len(specByKey))
	for _, spec := range specByKey {
		cloned, err := cloneJSON(spec)
		if err != nil {
			findings = append(findings, buildDiagnostic(CodeInvalidBuild, workflow.Source, "step-kind metadata could not be canonicalized", "Register JSON-compatible immutable metadata."))
			continue
		}
		specs = append(specs, cloned)
	}
	sort.Slice(specs, func(i, j int) bool {
		if specs[i].Name == specs[j].Name {
			return specs[i].Version < specs[j].Version
		}
		return specs[i].Name < specs[j].Name
	})
	sort.Slice(resolved, func(i, j int) bool { return resolved[i].Binding.NodeID < resolved[j].Binding.NodeID })
	return resolved, specs, findings
}

func buildExecutionProfile(node graph.Node, binding ExternalBinding, source, execution stepkind.StepKindSpec, description BindingDescription) (RemoteExecutionProfile, error) {
	configDigest, err := values.DigestInline(node.Config)
	if err != nil {
		return RemoteExecutionProfile{}, err
	}
	sourceDigest, err := digestCanonical(source)
	if err != nil {
		return RemoteExecutionProfile{}, err
	}
	executionDigest, err := digestCanonical(execution)
	if err != nil {
		return RemoteExecutionProfile{}, err
	}
	return RemoteExecutionProfile{
		Driver: binding.Driver, NodeID: node.ID, Kind: node.Kind, Version: node.KindVersion,
		NodeConfigDigest: configDigest, SourceSpecDigest: sourceDigest, ExecutionSpecDigest: executionDigest,
		Effects: append(graph.EffectSet(nil), description.EffectiveEffects...), Capabilities: append([]string(nil), description.Capabilities...),
	}, nil
}

func digestCanonical(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

func normalizeDescription(input BindingDescription) BindingDescription {
	input.EffectiveEffects = unionEffects(input.EffectiveEffects, nil)
	input.Capabilities = append([]string(nil), input.Capabilities...)
	sort.Strings(input.Capabilities)
	return input
}

func validateDescription(input BindingDescription) error {
	for _, effect := range input.EffectiveEffects {
		if !effect.Valid() || effect == graph.EffectMutate || effect == graph.EffectDestructive {
			return fmt.Errorf("binding description contains unsupported effect %q", effect)
		}
	}
	if len(input.Capabilities) > 64 {
		return fmt.Errorf("binding description contains too many capabilities")
	}
	for index, capability := range input.Capabilities {
		if err := validateIdentifier("capability", capability); err != nil {
			return err
		}
		if index > 0 && input.Capabilities[index-1] == capability {
			return fmt.Errorf("binding capability %q is duplicated", capability)
		}
	}
	return nil
}

func unionEffects(first, second graph.EffectSet) graph.EffectSet {
	seen := make(map[graph.Effect]struct{}, len(first)+len(second))
	for _, effect := range append(append(graph.EffectSet(nil), first...), second...) {
		seen[effect] = struct{}{}
	}
	result := make(graph.EffectSet, 0, len(seen))
	for effect := range seen {
		result = append(result, effect)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func hasUnsafeEffects(effects graph.EffectSet) bool {
	for _, effect := range effects {
		if effect == graph.EffectMutate || effect == graph.EffectDestructive {
			return true
		}
	}
	return false
}

func containsAll(actual, required []string) bool {
	set := make(map[string]struct{}, len(actual))
	for _, item := range actual {
		set[item] = struct{}{}
	}
	for _, item := range required {
		if _, ok := set[item]; !ok {
			return false
		}
	}
	return true
}

func containsEffects(actual, required graph.EffectSet) bool {
	set := make(map[graph.Effect]struct{}, len(actual))
	for _, effect := range actual {
		set[effect] = struct{}{}
	}
	for _, effect := range required {
		if _, ok := set[effect]; !ok {
			return false
		}
	}
	return true
}

func graphNode(workflow graph.Graph, id string) (graph.Node, bool) {
	for _, node := range workflow.Nodes {
		if node.ID == id {
			return node, true
		}
	}
	return graph.Node{}, false
}

func inputFields(input []graph.InputSpec) ([]SchemaField, error) {
	result := make([]SchemaField, len(input))
	for index, declaration := range input {
		schema, err := cloneJSON(declaration.Schema)
		if err != nil {
			return nil, err
		}
		result[index] = SchemaField{Name: declaration.Name, Description: declaration.Description, Required: declaration.Required, Schema: schema}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func outputFields(input []graph.OutputSpec) ([]SchemaField, error) {
	result := make([]SchemaField, len(input))
	for index, declaration := range input {
		schema, err := cloneJSON(declaration.Schema)
		if err != nil {
			return nil, err
		}
		result[index] = SchemaField{Name: declaration.Name, Description: declaration.Description, Required: true, Schema: schema}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}
