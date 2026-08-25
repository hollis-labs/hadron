package hoststate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

const (
	PlanSnapshotSchemaVersion      = "1"
	SourceSnapshotSchemaVersion    = "1"
	CompileDescriptorSchemaVersion = "1"
	maximumPersistedSourceBytes    = 64 << 20
)

// CompileDescriptor is an inspectable, deterministic description of the
// immutable compiler inputs used to produce a durable plan. It records stable
// registrations and semantic versions, never concrete implementations.
type CompileDescriptor struct {
	SchemaVersion          string                      `json:"schema_version"`
	SemanticRevision       string                      `json:"semantic_revision,omitempty"`
	SemanticKey            string                      `json:"semantic_key,omitempty"`
	PlanSchemaVersion      string                      `json:"plan_schema_version,omitempty"`
	MaximumCallDepth       int                         `json:"maximum_call_depth,omitempty"`
	StepKinds              []stepkind.StepKindSpec     `json:"step_kinds,omitempty"`
	Verifiers              []verification.VerifierSpec `json:"verifiers,omitempty"`
	PolicyHookCount        int                         `json:"policy_hook_count,omitempty"`
	VerificationExtractors []string                    `json:"verification_extractors,omitempty"`
	NodeExpanders          []string                    `json:"node_expanders,omitempty"`
	Available              bool                        `json:"available"`
	UnavailableReason      string                      `json:"unavailable_reason,omitempty"`
}

// NewCompileDescriptor freezes canonical compiler metadata and binds it to a
// digest. Callers must change SemanticRevision whenever implementation
// behavior changes without changing inspectable registrations.
func NewCompileDescriptor(revision string, maximumCallDepth int, kinds []stepkind.StepKindSpec, verifiers []verification.VerifierSpec, policyHookCount int, extractors, expanders []string) (CompileDescriptor, error) {
	descriptor := CompileDescriptor{
		SchemaVersion: CompileDescriptorSchemaVersion, SemanticRevision: revision,
		PlanSchemaVersion: compile.ExecutionPlanSchemaVersion,
		MaximumCallDepth:  maximumCallDepth, StepKinds: kinds, Verifiers: verifiers,
		PolicyHookCount: policyHookCount, VerificationExtractors: extractors,
		NodeExpanders: expanders, Available: true,
	}
	canonical, err := canonicalCompileDescriptor(descriptor)
	if err != nil {
		return CompileDescriptor{}, err
	}
	key, err := compileDescriptorKey(canonical)
	if err != nil {
		return CompileDescriptor{}, err
	}
	canonical.SemanticKey = key
	return canonical, nil
}

// UnavailableCompileDescriptor makes unsupported provider metadata explicit.
func UnavailableCompileDescriptor(reason string) CompileDescriptor {
	return CompileDescriptor{SchemaVersion: CompileDescriptorSchemaVersion, Available: false, UnavailableReason: strings.TrimSpace(reason)}
}

func (d CompileDescriptor) Validate() error {
	if d.SchemaVersion != CompileDescriptorSchemaVersion {
		return fmt.Errorf("unsupported compile descriptor schema %q", d.SchemaVersion)
	}
	if !d.Available {
		if strings.TrimSpace(d.UnavailableReason) == "" || d.SemanticKey != "" || d.SemanticRevision != "" || d.PlanSchemaVersion != "" || d.MaximumCallDepth != 0 || len(d.StepKinds) != 0 || len(d.Verifiers) != 0 || d.PolicyHookCount != 0 || len(d.VerificationExtractors) != 0 || len(d.NodeExpanders) != 0 {
			return errors.New("unavailable compile descriptor must contain only its reason")
		}
		return nil
	}
	canonical, err := canonicalCompileDescriptor(d)
	if err != nil {
		return err
	}
	encoded, encodeErr := json.Marshal(d)
	canonicalJSON, canonicalErr := json.Marshal(canonical)
	if encodeErr != nil || canonicalErr != nil || !bytes.Equal(encoded, canonicalJSON) {
		return errors.New("compile descriptor registrations are not canonical")
	}
	key, err := compileDescriptorKey(d)
	if err != nil {
		return err
	}
	if d.SemanticKey != key {
		return errors.New("compile descriptor semantic key does not match metadata")
	}
	return nil
}

func canonicalCompileDescriptor(input CompileDescriptor) (CompileDescriptor, error) {
	cloned, err := cloneJSON(input)
	if err != nil {
		return CompileDescriptor{}, fmt.Errorf("clone compile descriptor: %w", err)
	}
	if strings.TrimSpace(cloned.SemanticRevision) == "" || !utf8.ValidString(cloned.SemanticRevision) || cloned.PlanSchemaVersion != compile.ExecutionPlanSchemaVersion {
		return CompileDescriptor{}, errors.New("compile descriptor semantic revision is required")
	}
	if cloned.MaximumCallDepth < 1 || cloned.PolicyHookCount < 0 || len(cloned.StepKinds) == 0 {
		return CompileDescriptor{}, errors.New("compile descriptor requires positive call depth, step kinds, and non-negative hook count")
	}
	for index, spec := range cloned.StepKinds {
		if err := stepkind.ValidateSpec(spec); err != nil {
			return CompileDescriptor{}, fmt.Errorf("compile step kind[%d]: %w", index, err)
		}
	}
	for index, spec := range cloned.Verifiers {
		if err := spec.Validate(); err != nil {
			return CompileDescriptor{}, fmt.Errorf("compile verifier[%d]: %w", index, err)
		}
	}
	sort.Slice(cloned.StepKinds, func(i, j int) bool {
		if cloned.StepKinds[i].Name == cloned.StepKinds[j].Name {
			return cloned.StepKinds[i].Version < cloned.StepKinds[j].Version
		}
		return cloned.StepKinds[i].Name < cloned.StepKinds[j].Name
	})
	sort.Slice(cloned.Verifiers, func(i, j int) bool {
		if cloned.Verifiers[i].Kind == cloned.Verifiers[j].Kind {
			return cloned.Verifiers[i].Version < cloned.Verifiers[j].Version
		}
		return cloned.Verifiers[i].Kind < cloned.Verifiers[j].Kind
	})
	for index := 1; index < len(cloned.StepKinds); index++ {
		if cloned.StepKinds[index-1].Name == cloned.StepKinds[index].Name && cloned.StepKinds[index-1].Version == cloned.StepKinds[index].Version {
			return CompileDescriptor{}, errors.New("compile descriptor contains a duplicate step kind")
		}
	}
	for index := 1; index < len(cloned.Verifiers); index++ {
		if cloned.Verifiers[index-1].Kind == cloned.Verifiers[index].Kind && cloned.Verifiers[index-1].Version == cloned.Verifiers[index].Version {
			return CompileDescriptor{}, errors.New("compile descriptor contains a duplicate verifier")
		}
	}
	for _, names := range [][]string{cloned.VerificationExtractors, cloned.NodeExpanders} {
		sort.Strings(names)
		for index, name := range names {
			if strings.TrimSpace(name) == "" || !utf8.ValidString(name) || (index > 0 && names[index-1] == name) {
				return CompileDescriptor{}, errors.New("compile descriptor names must be non-empty, valid UTF-8, and unique")
			}
		}
	}
	cloned.Available = true
	cloned.UnavailableReason = ""
	return cloned, nil
}

func compileDescriptorKey(input CompileDescriptor) (string, error) {
	input.SemanticKey = ""
	encoded, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

// SourceSnapshot holds exact source material selected by the authorized host
// resolver. Its private/run classifications are part of the persisted
// contract; presentation layers must never expose Content by default.
type SourceSnapshot struct {
	SchemaVersion       string                `json:"schema_version"`
	Definition          graph.DefinitionRef   `json:"definition"`
	Format              graph.SourceFormat    `json:"format"`
	SourceSchemaID      string                `json:"source_schema_id"`
	SourceSchemaVersion string                `json:"source_schema_version"`
	TrustClass          string                `json:"trust_class"`
	Digest              string                `json:"digest"`
	Content             []byte                `json:"content"`
	MovableAtResolution bool                  `json:"movable_at_resolution,omitempty"`
	Redaction           values.RedactionClass `json:"redaction"`
	Retention           values.RetentionClass `json:"retention"`
}

func (s SourceSnapshot) Validate() error {
	if s.SchemaVersion != SourceSnapshotSchemaVersion || !s.Format.Valid() || strings.TrimSpace(s.SourceSchemaID) == "" || strings.TrimSpace(s.SourceSchemaVersion) == "" || strings.TrimSpace(s.TrustClass) == "" {
		return errors.New("source snapshot requires supported schema, format, trust, and version metadata")
	}
	if len(s.Content) == 0 || len(s.Content) > maximumPersistedSourceBytes || !utf8.Valid(s.Content) {
		return errors.New("source snapshot content is empty, oversized, or invalid UTF-8")
	}
	if s.Redaction != values.RedactionPrivate || (s.Retention != values.RetentionRun && s.Retention != values.RetentionProject) {
		return errors.New("source snapshot must be private with run or project retention")
	}
	if err := values.ValidateDigest(s.Digest); err != nil || s.Digest != values.SHA256Digest(s.Content) {
		return errors.New("source snapshot digest does not match exact content")
	}
	if s.Definition.Digest != s.Digest || s.Definition.Provenance == nil || s.Definition.Provenance.Digest != s.Digest || s.Definition.Authority == "" || s.Definition.Provenance.Authority != s.Definition.Authority {
		return errors.New("source snapshot definition, authority, provenance, and digest do not match")
	}
	return nil
}

// PlanSnapshot is the complete durable recovery/explanation envelope. Plan is
// the same authority already embedded in StartRecord; this envelope only
// supplies exact persistence columns, source material, and compiler context.
type PlanSnapshot struct {
	SchemaVersion string                `json:"schema_version"`
	Digest        string                `json:"digest"`
	Plan          compile.ExecutionPlan `json:"plan"`
	SourceMap     graph.SourceMap       `json:"source_map"`
	Source        *SourceSnapshot       `json:"source,omitempty"`
	Compile       CompileDescriptor     `json:"compile"`
}

// PlanSnapshotMetadata is the default-safe inspection projection. It contains
// source locations and compiler registrations, but never exact source bytes or
// arbitrary provenance metadata.
type PlanSnapshotMetadata struct {
	SchemaVersion  string                 `json:"schema_version"`
	SnapshotDigest string                 `json:"snapshot_digest"`
	Plan           runtime.PlanRef        `json:"plan"`
	Definition     DefinitionMetadata     `json:"definition"`
	GraphDigest    string                 `json:"graph_digest"`
	SourceDigests  []compile.SourceDigest `json:"source_digests"`
	SourceMap      graph.SourceMap        `json:"source_map"`
	Source         SourceSnapshotMetadata `json:"source"`
	Compile        CompileDescriptor      `json:"compile"`
}

type DefinitionMetadata struct {
	Authority string                `json:"authority,omitempty"`
	Kind      string                `json:"kind,omitempty"`
	ID        string                `json:"id"`
	Locator   string                `json:"locator,omitempty"`
	Version   string                `json:"version"`
	Digest    string                `json:"digest"`
	Origin    string                `json:"origin,omitempty"`
	Revision  string                `json:"revision,omitempty"`
	Parents   []graph.ProvenanceRef `json:"parents,omitempty"`
}

type SourceSnapshotMetadata struct {
	Available           bool                  `json:"available"`
	Format              graph.SourceFormat    `json:"format,omitempty"`
	SchemaID            string                `json:"schema_id,omitempty"`
	SchemaVersion       string                `json:"schema_version,omitempty"`
	TrustClass          string                `json:"trust_class,omitempty"`
	Digest              string                `json:"digest,omitempty"`
	ContentBytes        int                   `json:"content_bytes,omitempty"`
	MovableAtResolution bool                  `json:"movable_at_resolution,omitempty"`
	Redaction           values.RedactionClass `json:"redaction,omitempty"`
	Retention           values.RetentionClass `json:"retention,omitempty"`
}

func (s PlanSnapshot) Metadata() (PlanSnapshotMetadata, error) {
	if err := s.Validate(); err != nil {
		return PlanSnapshotMetadata{}, err
	}
	metadata := PlanSnapshotMetadata{
		SchemaVersion: s.SchemaVersion, SnapshotDigest: s.Digest, Plan: s.PlanRef(), GraphDigest: s.Plan.Graph.Digest,
		SourceDigests: append([]compile.SourceDigest(nil), s.Plan.SourceDigests...),
		SourceMap:     sanitizedSourceMap(s.SourceMap), Compile: s.Compile,
		Definition: DefinitionMetadata{
			Authority: s.Plan.Definition.Authority, Kind: s.Plan.Definition.Kind,
			ID: s.Plan.Definition.ID, Locator: SanitizeLocator(s.Plan.Definition.Locator),
			Version: s.Plan.Definition.Version, Digest: s.Plan.Definition.Digest,
		},
	}
	if provenance := s.Plan.Definition.Provenance; provenance != nil {
		metadata.Definition.Origin = provenance.Origin
		metadata.Definition.Revision = provenance.Revision
		metadata.Definition.Parents = append([]graph.ProvenanceRef(nil), provenance.Parents...)
		for index := range metadata.Definition.Parents {
			metadata.Definition.Parents[index].Locator = SanitizeLocator(metadata.Definition.Parents[index].Locator)
		}
	}
	if s.Source != nil {
		metadata.Source = SourceSnapshotMetadata{
			Available: true, Format: s.Source.Format, SchemaID: s.Source.SourceSchemaID,
			SchemaVersion: s.Source.SourceSchemaVersion, TrustClass: s.Source.TrustClass,
			Digest: s.Source.Digest, ContentBytes: len(s.Source.Content),
			MovableAtResolution: s.Source.MovableAtResolution,
			Redaction:           s.Source.Redaction, Retention: s.Source.Retention,
		}
	}
	return cloneJSON(metadata)
}

func sanitizedSourceMap(input graph.SourceMap) graph.SourceMap {
	output, err := cloneJSON(input)
	if err != nil {
		return graph.SourceMap{}
	}
	sanitize := func(ref *graph.SourceRef) {
		if ref != nil {
			ref.Locator = SanitizeLocator(ref.Locator)
		}
	}
	sanitize(output.Graph)
	for key, ref := range output.Inputs {
		sanitize(&ref)
		output.Inputs[key] = ref
	}
	for key, ref := range output.Outputs {
		sanitize(&ref)
		output.Outputs[key] = ref
	}
	for key, ref := range output.Nodes {
		sanitize(&ref)
		output.Nodes[key] = ref
	}
	for key, ref := range output.Edges {
		sanitize(&ref)
		output.Edges[key] = ref
	}
	for key, ref := range output.Activations {
		sanitize(&ref)
		output.Activations[key] = ref
	}
	return output
}

// SanitizeLocator returns a stable source reference safe for ordinary
// diagnostics. It removes credentials and URL parameters, rejects malformed
// values, and masks secret-reference schemes without resolving them.
func SanitizeLocator(input string) string {
	if !utf8.ValidString(input) || strings.IndexFunc(input, unicode.IsControl) >= 0 {
		return "<invalid-locator>"
	}
	candidate := input
	if index := strings.IndexAny(candidate, "?#"); index >= 0 {
		candidate = candidate[:index]
	}
	parsed, err := url.Parse(candidate)
	if err != nil {
		return "<invalid-locator>"
	}
	if strings.EqualFold(parsed.Scheme, "secret") {
		return values.RedactedMarker
	}
	if parsed.Scheme == "" {
		if parsed.Host != "" {
			parsed.User = nil
			return parsed.String()
		}
		prefix := candidate
		if slash := strings.IndexAny(prefix, "/\\"); slash >= 0 {
			prefix = prefix[:slash]
		}
		if at := strings.LastIndex(prefix, "@"); at >= 0 {
			candidate = candidate[at+1:]
		}
		return candidate
	}
	parsed.User = nil
	if parsed.Opaque != "" {
		if strings.Contains(parsed.Opaque, "@") {
			return parsed.Scheme + ":<redacted-locator>"
		}
	}
	return parsed.String()
}

func (s PlanSnapshot) PlanRef() runtime.PlanRef {
	return runtime.PlanRef{ID: s.Plan.ID, Version: s.Plan.Graph.Version, Digest: s.Plan.Digest, SchemaVersion: s.Plan.SchemaVersion}
}

func (s PlanSnapshot) Validate() error {
	if s.SchemaVersion != PlanSnapshotSchemaVersion || s.Plan.SchemaVersion != compile.ExecutionPlanSchemaVersion {
		return errors.New("unsupported plan snapshot schema")
	}
	if err := s.PlanRef().Validate(); err != nil {
		return fmt.Errorf("plan snapshot reference: %w", err)
	}
	digest, err := s.canonicalDigest()
	if err != nil || digest != s.Digest {
		return errors.New("plan snapshot digest does not match exact snapshot material")
	}
	if s.Plan.ID != s.Plan.Graph.ID || s.Plan.Graph.Version != s.Plan.Definition.Version || s.Plan.ID != s.Plan.Definition.ID {
		return errors.New("plan snapshot definition and graph identities do not match")
	}
	graphDigest, err := compile.GraphDigest(s.Plan.Graph)
	if err != nil || graphDigest != s.Plan.Graph.Digest {
		return errors.New("plan snapshot graph digest does not match graph semantics")
	}
	planDigest, err := compile.PlanDigest(s.Plan)
	if err != nil || planDigest != s.Plan.Digest {
		return errors.New("plan snapshot digest does not match plan content")
	}
	if !reflect.DeepEqual(s.SourceMap, s.Plan.SourceMap) || !reflect.DeepEqual(s.SourceMap, s.Plan.Graph.SourceMap) {
		return errors.New("plan snapshot source maps do not match")
	}
	if _, err := compile.NewBundledDefinitionResolver(&s.Plan); err != nil {
		return fmt.Errorf("plan snapshot bundled definitions: %w", err)
	}
	if err := s.Compile.Validate(); err != nil {
		return fmt.Errorf("plan snapshot compile descriptor: %w", err)
	}
	if s.Source != nil {
		if err := s.Source.Validate(); err != nil {
			return fmt.Errorf("plan snapshot source: %w", err)
		}
		if !reflect.DeepEqual(s.Source.Definition, s.Plan.Definition) {
			return errors.New("plan snapshot source definition does not match plan definition")
		}
		found := false
		for _, digest := range s.Plan.SourceDigests {
			if digest.Format == s.Source.Format && digest.Digest == s.Source.Digest {
				found = true
				break
			}
		}
		if !found {
			return errors.New("plan snapshot source is absent from plan source digests")
		}
	}
	return nil
}

func (s PlanSnapshot) Clone() (PlanSnapshot, error) {
	return cloneJSON(s)
}

// SealPlanSnapshot returns a defensive copy carrying the digest of its full
// canonical, locator-sensitive plan/source/compiler material.
func SealPlanSnapshot(input PlanSnapshot) (PlanSnapshot, error) {
	sealed, err := input.Clone()
	if err != nil {
		return PlanSnapshot{}, err
	}
	sealed.Digest = ""
	digest, err := sealed.canonicalDigest()
	if err != nil {
		return PlanSnapshot{}, err
	}
	sealed.Digest = digest
	return sealed, nil
}

func (s PlanSnapshot) canonicalDigest() (string, error) {
	s.Digest = ""
	encoded, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

func cloneJSON[T any](input T) (T, error) {
	var output T
	encoded, err := json.Marshal(input)
	if err != nil {
		return output, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&output); err != nil {
		return output, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return output, errors.New("JSON contains trailing material")
	}
	return output, nil
}

func equalJSON(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
