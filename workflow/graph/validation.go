package graph

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// MaxIDLength is the maximum normalized workflow, node, input, output, or
// activation identifier length in bytes.
const MaxIDLength = 128

// IDValidationError explains why an identifier is not canonical.
type IDValidationError struct {
	ID         string
	Reason     string
	Normalized string
}

// Error implements error.
func (e *IDValidationError) Error() string {
	if e.Normalized != "" && e.Normalized != e.ID {
		return fmt.Sprintf("invalid id %q: %s (normalized: %q)", e.ID, e.Reason, e.Normalized)
	}
	return fmt.Sprintf("invalid id %q: %s", e.ID, e.Reason)
}

// NormalizeID converts an identifier into lower-case ASCII kebab form. Runs of
// punctuation, underscores, whitespace, or unsupported characters become one
// hyphen; leading and trailing separators are removed.
func NormalizeID(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var normalized strings.Builder
	separatorPending := false
	for _, r := range value {
		if isASCIIAlphaNumeric(r) {
			if separatorPending && normalized.Len() > 0 {
				normalized.WriteByte('-')
			}
			normalized.WriteRune(r)
			separatorPending = false
			continue
		}
		separatorPending = normalized.Len() > 0
	}
	return normalized.String()
}

// ValidateID rejects empty, overlong, or non-normalized identifiers.
func ValidateID(value string) error {
	normalized := NormalizeID(value)
	if normalized == "" {
		return &IDValidationError{ID: value, Reason: "must contain an ASCII letter or digit"}
	}
	if len(value) > MaxIDLength {
		return &IDValidationError{ID: value, Reason: fmt.Sprintf("must be at most %d bytes", MaxIDLength), Normalized: normalized}
	}
	if value != normalized {
		return &IDValidationError{ID: value, Reason: "must use normalized lower-case kebab form", Normalized: normalized}
	}
	return nil
}

func isASCIIAlphaNumeric(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= '0' && r <= '9'
}

// EnumValidationError identifies an unsupported enum value and its graph path.
type EnumValidationError struct {
	Path  string
	Value string
}

// Error implements error.
func (e *EnumValidationError) Error() string {
	return fmt.Sprintf("%s has unsupported enum value %q", e.Path, e.Value)
}

// ValidateEnums validates only closed enum fields. It intentionally does not
// perform graph topology, reference, schema, or policy validation, which belong
// to compiler validation passes.
func (g Graph) ValidateEnums() error {
	var errs []error
	add := func(path, value string, valid bool) {
		if !valid {
			errs = append(errs, &EnumValidationError{Path: path, Value: value})
		}
	}

	if g.Completion != nil {
		add("completion.mode", string(g.Completion.Mode), g.Completion.Mode.Valid())
		validateExtension("completion.extension", g.Completion.Extension, add)
	}
	if g.Durability != nil {
		add("durability.mode", string(g.Durability.Mode), g.Durability.Mode.Valid())
		validateExtension("durability.extension", g.Durability.Extension, add)
	}
	if g.Compensation != nil {
		add("compensation.mode", string(g.Compensation.Mode), g.Compensation.Mode.Valid())
		for i, trigger := range g.Compensation.Triggers {
			add(fmt.Sprintf("compensation.triggers[%d]", i), string(trigger), trigger.Valid())
		}
	}
	validateExtension("concurrency.extension", g.Concurrency.Extension, add)
	validateSourceRef("source", g.Source, add)
	validateSourceRef("source_map.graph", g.SourceMap.Graph, add)
	validateSourceMap(g.SourceMap, add)
	validateExtensionMap("extensions", g.Extensions, add)

	for i, input := range g.Inputs {
		path := fmt.Sprintf("inputs[%d]", i)
		validateOptionalBinding(path+".default", input.Default, add)
		validateSourceRef(path+".source", input.Source, add)
	}
	for i, output := range g.Outputs {
		validateOutput(fmt.Sprintf("outputs[%d]", i), output, add)
	}
	for i, edge := range g.Edges {
		path := fmt.Sprintf("edges[%d]", i)
		add(path+".kind", string(edge.Kind), edge.Kind.Valid())
		validateSourceRef(path+".source", edge.Source, add)
	}
	for i, activation := range g.Activations {
		path := fmt.Sprintf("activations[%d]", i)
		if activation.Policy.Overlap != "" {
			add(path+".policy.overlap", string(activation.Policy.Overlap), activation.Policy.Overlap.Valid())
		}
		if activation.Policy.RunIDReuse != "" {
			add(path+".policy.run_id_reuse", string(activation.Policy.RunIDReuse), activation.Policy.RunIDReuse.Valid())
		}
		for _, name := range sortedKeys(activation.Inputs) {
			validateBinding(fmt.Sprintf("%s.inputs[%q]", path, name), activation.Inputs[name], add)
		}
		validateExpression(path+".policy.deduplication_key", activation.Policy.DeduplicationKey, add)
		validateSourceRef(path+".source", activation.Source, add)
	}
	for i, node := range g.Nodes {
		path := fmt.Sprintf("nodes[%d]", i)
		if node.ReadyWhen != "" {
			add(path+".ready_when", string(node.ReadyWhen), node.ReadyWhen.Valid())
		}
		for j, need := range node.Needs {
			if need.Kind != "" {
				add(fmt.Sprintf("%s.needs[%d].kind", path, j), string(need.Kind), need.Kind.Valid())
			}
			validateSourceRef(fmt.Sprintf("%s.needs[%d].source", path, j), need.Source, add)
		}
		validateExpression(path+".if", node.If, add)
		if node.ForEach != nil {
			validateExpression(path+".for_each.items", &node.ForEach.Items, add)
		}
		for _, name := range sortedKeys(node.InputBindings) {
			validateBinding(fmt.Sprintf("%s.with[%q]", path, name), node.InputBindings[name], add)
		}
		for j, output := range node.Outputs {
			validateOutput(fmt.Sprintf("%s.outputs[%d]", path, j), output, add)
		}
		for _, effect := range node.Effects {
			add(path+".effects", string(effect), effect.Valid())
		}
		if node.Retry != nil && node.Retry.Backoff.Strategy != "" {
			add(path+".retry.backoff.strategy", string(node.Retry.Backoff.Strategy), node.Retry.Backoff.Strategy.Valid())
		}
		if node.Idempotency != nil {
			add(path+".idempotency.mode", string(node.Idempotency.Mode), node.Idempotency.Mode.Valid())
			validateExpression(path+".idempotency.key", node.Idempotency.Key, add)
		}
		for j, catch := range node.Catch {
			catchPath := fmt.Sprintf("%s.catch[%d]", path, j)
			validateExpression(catchPath+".when", catch.When, add)
			validateSourceRef(catchPath+".source", catch.Source, add)
		}
		if node.Switch != nil {
			for j, arm := range node.Switch.Arms {
				armPath := fmt.Sprintf("%s.switch.arms[%d]", path, j)
				validateExpression(armPath+".when", &arm.When, add)
				validateSourceRef(armPath+".source", arm.Source, add)
			}
		}
		if node.Call != nil {
			add(path+".call.mode", string(node.Call.Mode), node.Call.Mode.Valid())
			if node.Call.OnParentClose != "" {
				add(path+".call.on_parent_close", string(node.Call.OnParentClose), node.Call.OnParentClose.Valid())
			}
		}
		if node.Verification != nil {
			validateVerification(path+".verify", node.Verification, add)
		}
		if node.Memoization != nil {
			validateExpression(path+".memoize.key", &node.Memoization.Key, add)
			maxAge, err := time.ParseDuration(string(node.Memoization.MaxAge))
			add(path+".memoize.max_age", string(node.Memoization.MaxAge), err == nil && maxAge > 0)
			if node.Memoization.OutputDigest != "" {
				add(path+".memoize.output_digest", node.Memoization.OutputDigest, validSHA256Digest(node.Memoization.OutputDigest))
			}
			validateExtension(path+".memoize.extension", node.Memoization.Extension, add)
		}
		if node.Durability != nil {
			add(path+".durability.mode", string(node.Durability.Mode), node.Durability.Mode.Valid())
			validateExtension(path+".durability.extension", node.Durability.Extension, add)
		}
		if node.Service != nil {
			validateVerification(path+".service.ready_check", node.Service.ReadyCheck, add)
			if node.Service.HeartbeatTimeout != "" {
				duration, err := time.ParseDuration(string(node.Service.HeartbeatTimeout))
				add(path+".service.heartbeat_timeout", string(node.Service.HeartbeatTimeout), err == nil && duration > 0)
			}
			for j, target := range node.Service.TeardownNodes {
				add(fmt.Sprintf("%s.service.teardown_nodes[%d]", path, j), target, ValidateID(target) == nil)
			}
			if node.Service.TeardownOf != "" {
				add(path+".service.teardown_of", node.Service.TeardownOf, ValidateID(node.Service.TeardownOf) == nil)
			}
			validateExtension(path+".service.extension", node.Service.Extension, add)
		}
		validateExtensionMap(path+".extensions", node.Extensions, add)
		validateSourceRef(path+".source", node.Source, add)
	}
	return errors.Join(errs...)
}

func validSHA256Digest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func validateOptionalBinding(path string, binding *Binding, add func(string, string, bool)) {
	if binding != nil {
		validateBinding(path, *binding, add)
	}
}

func validateBinding(path string, binding Binding, add func(string, string, bool)) {
	add(path+".kind", string(binding.Kind), binding.Kind.Valid())
	validateSourceRef(path+".source", binding.Source, add)
	validateExpression(path+".expression", binding.Expression, add)
}

func validateOutput(path string, output OutputSpec, add func(string, string, bool)) {
	validateOptionalBinding(path+".value", output.Value, add)
	validateSourceRef(path+".source", output.Source, add)
}

func validateExpression(path string, expression *Expression, add func(string, string, bool)) {
	if expression != nil {
		validateSourceRef(path+".source", expression.Source, add)
	}
}

func validateVerification(path string, verification *VerificationSpec, add func(string, string, bool)) {
	if verification == nil {
		return
	}
	for i, check := range verification.Checks {
		validateSourceRef(fmt.Sprintf("%s.checks[%d].source", path, i), check.Source, add)
	}
	validateExtension(path+".extension", verification.Extension, add)
}

func validateExtension(path string, extension Extension, add func(string, string, bool)) {
	validateSourceRef(path+".source", extension.Source, add)
}

func validateExtensionMap(path string, extensions map[string]Extension, add func(string, string, bool)) {
	for _, name := range sortedKeys(extensions) {
		validateExtension(fmt.Sprintf("%s[%q]", path, name), extensions[name], add)
	}
}

func validateSourceRef(path string, ref *SourceRef, add func(string, string, bool)) {
	if ref != nil {
		add(path+".format", string(ref.Format), ref.Format.Valid())
	}
}

func validateSourceMap(sourceMap SourceMap, add func(string, string, bool)) {
	groups := []struct {
		name string
		refs map[string]SourceRef
	}{
		{name: "inputs", refs: sourceMap.Inputs},
		{name: "outputs", refs: sourceMap.Outputs},
		{name: "nodes", refs: sourceMap.Nodes},
		{name: "edges", refs: sourceMap.Edges},
		{name: "activations", refs: sourceMap.Activations},
	}
	for _, group := range groups {
		for _, id := range sortedKeys(group.refs) {
			ref := group.refs[id]
			add(fmt.Sprintf("source_map.%s[%q].format", group.name, id), string(ref.Format), ref.Format.Valid())
		}
	}
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
