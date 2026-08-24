package hoststate

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/graph"
)

const (
	// ScopeTargetVersionV1 is the first durable Hadron scope/target contract.
	ScopeTargetVersionV1 = "v1"

	MaximumScopeTargetAttributes     = 64
	MaximumScopeTargetAttributeBytes = 16 << 10
	MaximumTargetCapabilities        = 128
	MaximumIdentityGrants            = 128
	MaximumTargetReferences          = 128
	MaximumTargetRequirementSets     = 4096
)

// RunScopeKind is a closed logical grouping vocabulary. It conveys no
// filesystem, compute, environment, capability, sandbox, readiness, or lease
// authority.
type RunScopeKind string

const (
	RunScopeProject RunScopeKind = "project"
	RunScopeAccount RunScopeKind = "account"
	RunScopeSession RunScopeKind = "session"
	RunScopeTeam    RunScopeKind = "team"
	RunScopeUser    RunScopeKind = "user"
)

func (k RunScopeKind) Valid() bool {
	switch k {
	case RunScopeProject, RunScopeAccount, RunScopeSession, RunScopeTeam, RunScopeUser:
		return true
	default:
		return false
	}
}

// RunScope is the exact logical namespace bound to one Hadron run.
type RunScope struct {
	Version    string            `json:"version"`
	Kind       RunScopeKind      `json:"kind"`
	ID         string            `json:"id"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (s RunScope) Validate() error {
	if s.Version != ScopeTargetVersionV1 || !s.Kind.Valid() || !validStableText(s.ID, 256, true) || unsafeMetadataValue(s.ID) {
		return errors.New("run scope requires a supported version, kind, and stable identity")
	}
	if err := validateMetadata(s.Attributes, true); err != nil {
		return fmt.Errorf("run scope attributes: %w", err)
	}
	return nil
}

// Clone returns a defensive copy suitable for persistence or policy input.
func (s RunScope) Clone() RunScope {
	s.Attributes = cloneStringMap(s.Attributes)
	return s
}

// RunScopeSelector is optional caller context for an identity provider. When
// supplied, the resolved scope must match it exactly.
type RunScopeSelector struct {
	Version string       `json:"version"`
	Kind    RunScopeKind `json:"kind"`
	ID      string       `json:"id"`
}

func (s RunScopeSelector) Validate() error {
	if s.Version != ScopeTargetVersionV1 || !s.Kind.Valid() || !validStableText(s.ID, 256, true) || unsafeMetadataValue(s.ID) {
		return errors.New("run scope selector requires a supported version, kind, and stable identity")
	}
	return nil
}

func (s RunScopeSelector) Matches(scope RunScope) bool {
	return s.Version == scope.Version && s.Kind == scope.Kind && s.ID == scope.ID
}

func (s RunScopeSelector) Clone() RunScopeSelector { return s }

// ExecutionTargetKind identifies where work executes. It is deliberately
// unrelated to RunScopeKind.
type ExecutionTargetKind string

const (
	ExecutionTargetLocal     ExecutionTargetKind = "local"
	ExecutionTargetWorkspace ExecutionTargetKind = "external_workspace"
	ExecutionTargetRemote    ExecutionTargetKind = "remote_runner"
)

func (k ExecutionTargetKind) Valid() bool {
	return k == ExecutionTargetLocal || k == ExecutionTargetWorkspace || k == ExecutionTargetRemote
}

type SandboxMode string

const (
	SandboxHostDefault SandboxMode = "host_default"
	SandboxIsolated    SandboxMode = "isolated"
	SandboxDisabled    SandboxMode = "disabled"
)

func (m SandboxMode) Valid() bool {
	return m == SandboxHostDefault || m == SandboxIsolated || m == SandboxDisabled
}

// SandboxPolicy is the exact target isolation policy. An isolated target must
// name a stable host-owned profile; other modes cannot carry one.
type SandboxPolicy struct {
	Mode    SandboxMode `json:"mode"`
	Profile string      `json:"profile,omitempty"`
}

func (p SandboxPolicy) Validate() error {
	if !p.Mode.Valid() {
		return errors.New("sandbox mode is invalid")
	}
	if p.Mode == SandboxIsolated {
		if !validStableText(p.Profile, 256, true) || unsafeMetadataValue(p.Profile) {
			return errors.New("isolated sandbox requires a stable non-secret profile")
		}
	} else if p.Profile != "" {
		return errors.New("sandbox profile is valid only for isolated mode")
	}
	return nil
}

type ExecutionTargetReadiness string

const (
	TargetReady       ExecutionTargetReadiness = "ready"
	TargetPending     ExecutionTargetReadiness = "pending"
	TargetUnavailable ExecutionTargetReadiness = "unavailable"
)

func (r ExecutionTargetReadiness) Valid() bool {
	return r == TargetReady || r == TargetPending || r == TargetUnavailable
}

// TargetReadiness is the host-observed target state bound at selection time.
type TargetReadiness struct {
	State     ExecutionTargetReadiness `json:"state"`
	CheckedAt time.Time                `json:"checked_at"`
	Reason    string                   `json:"reason,omitempty"`
}

func (r TargetReadiness) Validate() error {
	if !r.State.Valid() || r.CheckedAt.IsZero() || r.CheckedAt.Location() != time.UTC || !validStableText(r.Reason, 512, false) || unsafeMetadataValue(r.Reason) {
		return errors.New("target readiness requires valid state, time, and non-secret reason")
	}
	if r.State != TargetReady && r.Reason == "" {
		return errors.New("non-ready target readiness requires a reason")
	}
	return nil
}

// ExecutionTargetLease records a host-issued, non-credential lease identity.
type ExecutionTargetLease struct {
	ID         string    `json:"id"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

func (l ExecutionTargetLease) Validate() error {
	if !validStableText(l.ID, 256, true) || unsafeMetadataValue(l.ID) || l.AcquiredAt.IsZero() || l.AcquiredAt.Location() != time.UTC || l.ExpiresAt.Location() != time.UTC || !l.ExpiresAt.After(l.AcquiredAt) {
		return errors.New("target lease requires a stable non-secret identity and ordered times")
	}
	return nil
}

// TargetConfigReference identifies host-resolved configuration without
// carrying a resolved environment value or credential.
type TargetConfigReference struct {
	Authority string `json:"authority"`
	Name      string `json:"name"`
	Version   string `json:"version,omitempty"`
}

func (r TargetConfigReference) Validate() error {
	for _, value := range []struct {
		text     string
		maximum  int
		required bool
	}{{r.Authority, 64, true}, {r.Name, 256, true}, {r.Version, 128, false}} {
		if !validStableText(value.text, value.maximum, value.required) || unsafeMetadataValue(value.text) {
			return errors.New("target config reference contains invalid or secret-shaped text")
		}
	}
	return nil
}

// TargetProvenance explains the non-secret host binding decision.
type TargetProvenance struct {
	Authority  string            `json:"authority"`
	Reference  string            `json:"reference"`
	Revision   string            `json:"revision,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (p TargetProvenance) Validate() error {
	for _, value := range []struct {
		text     string
		maximum  int
		required bool
	}{{p.Authority, 128, true}, {p.Reference, 512, true}, {p.Revision, 256, false}} {
		if !validStableText(value.text, value.maximum, value.required) || unsafeMetadataValue(value.text) {
			return errors.New("target provenance contains invalid or secret-shaped text")
		}
	}
	return validateMetadata(p.Attributes, false)
}

// ExecutionTarget is the complete compute binding for a run. WorkspaceHandle
// is an opaque provider handle, never a legacy Hadron workspace identifier.
type ExecutionTarget struct {
	Version         string                           `json:"version"`
	ID              string                           `json:"id"`
	Kind            ExecutionTargetKind              `json:"kind"`
	CWD             string                           `json:"cwd,omitempty"`
	WorkspaceHandle string                           `json:"workspace_handle,omitempty"`
	EnvironmentRefs map[string]TargetConfigReference `json:"environment_refs,omitempty"`
	ConfigRefs      map[string]TargetConfigReference `json:"config_refs,omitempty"`
	Capabilities    []string                         `json:"capabilities,omitempty"`
	Labels          map[string]string                `json:"labels,omitempty"`
	Sandbox         SandboxPolicy                    `json:"sandbox"`
	Readiness       TargetReadiness                  `json:"readiness"`
	Lease           *ExecutionTargetLease            `json:"lease,omitempty"`
	Provenance      TargetProvenance                 `json:"provenance"`
}

func (t ExecutionTarget) Validate() error {
	if t.Version != ScopeTargetVersionV1 || !t.Kind.Valid() || !validStableText(t.ID, 256, true) || unsafeMetadataValue(t.ID) {
		return errors.New("execution target requires a supported version, kind, and non-secret identity")
	}
	if !validStableText(t.CWD, 4096, false) || unsafeMetadataValue(t.CWD) || !validStableText(t.WorkspaceHandle, 512, false) || unsafeMetadataValue(t.WorkspaceHandle) {
		return errors.New("execution target cwd or workspace handle is invalid")
	}
	switch t.Kind {
	case ExecutionTargetLocal:
		if t.WorkspaceHandle != "" || t.Lease != nil {
			return errors.New("local execution target cannot carry a workspace handle or lease")
		}
	case ExecutionTargetWorkspace:
		if t.WorkspaceHandle == "" || t.Lease == nil {
			return errors.New("external workspace target requires a workspace handle and lease")
		}
	case ExecutionTargetRemote:
		if t.WorkspaceHandle != "" {
			return errors.New("remote runner target cannot carry a workspace handle")
		}
	}
	if err := validateConfigReferences(t.EnvironmentRefs); err != nil {
		return fmt.Errorf("environment refs: %w", err)
	}
	if err := validateConfigReferences(t.ConfigRefs); err != nil {
		return fmt.Errorf("config refs: %w", err)
	}
	if err := validateSortedUnique(t.Capabilities, MaximumTargetCapabilities, "target capabilities"); err != nil {
		return err
	}
	if err := validateMetadata(t.Labels, false); err != nil {
		return fmt.Errorf("target labels: %w", err)
	}
	if err := t.Sandbox.Validate(); err != nil {
		return err
	}
	if err := t.Readiness.Validate(); err != nil {
		return err
	}
	if t.Lease != nil {
		if err := t.Lease.Validate(); err != nil {
			return err
		}
		if !t.Lease.ExpiresAt.After(t.Readiness.CheckedAt) {
			return errors.New("target lease expires at or before its readiness observation")
		}
	}
	return t.Provenance.Validate()
}

func (t ExecutionTarget) Clone() ExecutionTarget {
	t.EnvironmentRefs = cloneConfigReferences(t.EnvironmentRefs)
	t.ConfigRefs = cloneConfigReferences(t.ConfigRefs)
	t.Capabilities = append([]string(nil), t.Capabilities...)
	t.Labels = cloneStringMap(t.Labels)
	t.Provenance.Attributes = cloneStringMap(t.Provenance.Attributes)
	if t.Lease != nil {
		lease := *t.Lease
		lease.AcquiredAt = lease.AcquiredAt.UTC()
		lease.ExpiresAt = lease.ExpiresAt.UTC()
		t.Lease = &lease
	}
	t.Readiness.CheckedAt = t.Readiness.CheckedAt.UTC()
	return t
}

// ExecutionTargetSelector describes caller-requested target properties. The
// identity provider resolves it to one exact ExecutionTarget.
type ExecutionTargetSelector struct {
	Version              string                `json:"version"`
	ID                   string                `json:"id,omitempty"`
	Kinds                []ExecutionTargetKind `json:"kinds,omitempty"`
	RequiredCapabilities []string              `json:"required_capabilities,omitempty"`
	RequiredLabels       map[string]string     `json:"required_labels,omitempty"`
	SandboxModes         []SandboxMode         `json:"sandbox_modes,omitempty"`
}

func (s ExecutionTargetSelector) Validate() error {
	if s.Version != ScopeTargetVersionV1 || !validStableText(s.ID, 256, false) || unsafeMetadataValue(s.ID) {
		return errors.New("execution target selector has an invalid version or identity")
	}
	if s.ID == "" && len(s.Kinds) == 0 && len(s.RequiredCapabilities) == 0 && len(s.RequiredLabels) == 0 && len(s.SandboxModes) == 0 {
		return errors.New("execution target selector must constrain at least one target property")
	}
	if len(s.Kinds) > 8 || !sort.SliceIsSorted(s.Kinds, func(i, j int) bool { return s.Kinds[i] < s.Kinds[j] }) {
		return errors.New("execution target selector kinds must be sorted and bounded")
	}
	for index, kind := range s.Kinds {
		if !kind.Valid() || (index > 0 && kind == s.Kinds[index-1]) {
			return errors.New("execution target selector kinds must be valid and unique")
		}
	}
	if err := validateSortedUnique(s.RequiredCapabilities, MaximumTargetCapabilities, "selector capabilities"); err != nil {
		return err
	}
	if err := validateMetadata(s.RequiredLabels, false); err != nil {
		return fmt.Errorf("selector labels: %w", err)
	}
	if len(s.SandboxModes) > 3 || !sort.SliceIsSorted(s.SandboxModes, func(i, j int) bool { return s.SandboxModes[i] < s.SandboxModes[j] }) {
		return errors.New("selector sandbox modes must be sorted and bounded")
	}
	for index, mode := range s.SandboxModes {
		if !mode.Valid() || (index > 0 && mode == s.SandboxModes[index-1]) {
			return errors.New("selector sandbox modes must be valid and unique")
		}
	}
	return nil
}

func (s ExecutionTargetSelector) Matches(target ExecutionTarget) bool {
	if s.Version != target.Version || (s.ID != "" && s.ID != target.ID) || (len(s.Kinds) != 0 && !containsTargetKind(s.Kinds, target.Kind)) || (len(s.SandboxModes) != 0 && !containsSandboxMode(s.SandboxModes, target.Sandbox.Mode)) {
		return false
	}
	return containsAll(target.Capabilities, s.RequiredCapabilities) && labelsMatch(target.Labels, s.RequiredLabels)
}

// Clone returns a defensive selector copy for provider calls and intent
// persistence.
func (s ExecutionTargetSelector) Clone() ExecutionTargetSelector {
	s.Kinds = append([]ExecutionTargetKind(nil), s.Kinds...)
	s.RequiredCapabilities = append([]string(nil), s.RequiredCapabilities...)
	s.RequiredLabels = cloneStringMap(s.RequiredLabels)
	s.SandboxModes = append([]SandboxMode(nil), s.SandboxModes...)
	return s
}

// ValidateExecutionTargetBinding proves all graph and executor requirements
// against the selected compute target. Logical scope facts are intentionally
// absent from this function and therefore cannot grant compute access.
func ValidateExecutionTargetBinding(target *ExecutionTarget, capabilities []string, requirements map[string]graph.ExecutionTargetRequirements) error {
	if err := validateSortedUnique(capabilities, MaximumTargetCapabilities, "required capabilities"); err != nil {
		return err
	}
	if len(requirements) > MaximumTargetRequirementSets {
		return errors.New("execution target requirements exceed their set limit")
	}
	required := len(capabilities) != 0
	keys := make([]string, 0, len(requirements))
	for key := range requirements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		requirement := requirements[key]
		if !validRequirementKey(key) {
			return errors.New("execution target requirement has an invalid identity")
		}
		if err := validateRequirement(requirement); err != nil {
			return fmt.Errorf("execution target requirement %s: %w", key, err)
		}
		required = required || len(requirement.Kinds) != 0 || len(requirement.Capabilities) != 0 || len(requirement.Labels) != 0 || len(requirement.Constraints) != 0
	}
	if target == nil {
		if required {
			return errors.New("execution target is required by workflow capabilities or target requirements")
		}
		return nil
	}
	if err := target.Validate(); err != nil {
		return err
	}
	if target.Readiness.State != TargetReady {
		return errors.New("execution target is not ready")
	}
	if !containsAll(target.Capabilities, capabilities) {
		return errors.New("execution target does not provide every required capability")
	}
	for _, key := range keys {
		requirement := requirements[key]
		if len(requirement.Kinds) != 0 && !containsString(requirement.Kinds, string(target.Kind)) {
			return fmt.Errorf("execution target kind does not satisfy %s", key)
		}
		if !containsAll(target.Capabilities, requirement.Capabilities) {
			return fmt.Errorf("execution target capabilities do not satisfy %s", key)
		}
		if !labelsMatch(target.Labels, requirement.Labels) {
			return fmt.Errorf("execution target labels do not satisfy %s", key)
		}
		constraintKeys := make([]string, 0, len(requirement.Constraints))
		for constraint := range requirement.Constraints {
			constraintKeys = append(constraintKeys, constraint)
		}
		sort.Strings(constraintKeys)
		for _, constraint := range constraintKeys {
			raw := requirement.Constraints[constraint]
			switch constraint {
			case "sandbox":
				mode, ok := raw.(string)
				if !ok || mode != string(target.Sandbox.Mode) {
					return fmt.Errorf("execution target sandbox does not satisfy %s", key)
				}
			case "sandbox_profile":
				profile, ok := raw.(string)
				if !ok || profile != target.Sandbox.Profile {
					return fmt.Errorf("execution target sandbox profile does not satisfy %s", key)
				}
			default:
				return fmt.Errorf("execution target constraint %q for %s is unsupported", constraint, key)
			}
		}
	}
	return nil
}

func validateRequirement(requirement graph.ExecutionTargetRequirements) error {
	if err := validateSortedUnique(requirement.Kinds, 8, "target kinds"); err != nil {
		return err
	}
	for _, kind := range requirement.Kinds {
		if !ExecutionTargetKind(kind).Valid() {
			return errors.New("target kinds contain an unknown Hadron target kind")
		}
	}
	if err := validateSortedUnique(requirement.Capabilities, MaximumTargetCapabilities, "target capabilities"); err != nil {
		return err
	}
	if err := validateMetadata(requirement.Labels, false); err != nil {
		return fmt.Errorf("target labels: %w", err)
	}
	if len(requirement.Constraints) > 2 {
		return errors.New("target constraints exceed the closed contract")
	}
	keys := make([]string, 0, len(requirement.Constraints))
	for key := range requirement.Constraints {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value, ok := requirement.Constraints[key].(string)
		if !ok || !validStableText(value, 256, true) || unsafeMetadataValue(value) {
			return errors.New("target constraint must be stable non-secret text")
		}
		switch key {
		case "sandbox":
			if !SandboxMode(value).Valid() {
				return errors.New("target sandbox constraint is invalid")
			}
		case "sandbox_profile":
		default:
			return errors.New("target constraint is unsupported")
		}
	}
	return nil
}

func validRequirementKey(value string) bool {
	if value == "$graph" {
		return true
	}
	return validStableText(value, 256, true) && !unsafeMetadataValue(value)
}

func validateConfigReferences(input map[string]TargetConfigReference) error {
	if len(input) > MaximumTargetReferences {
		return errors.New("too many target config references")
	}
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !validStableText(key, 128, true) || sensitiveMetadataKey(key) || legacyWorkspaceKey(key) {
			return errors.New("target config reference name is invalid or credential-shaped")
		}
		if err := input[key].Validate(); err != nil {
			return err
		}
	}
	return nil
}

func validateMetadata(input map[string]string, scope bool) error {
	if len(input) > MaximumScopeTargetAttributes {
		return errors.New("metadata has too many entries")
	}
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	total := 0
	for _, key := range keys {
		value := input[key]
		if !validStableText(key, 128, true) || !validStableText(value, 512, true) || sensitiveMetadataKey(key) || unsafeMetadataValue(value) || legacyWorkspaceKey(key) || (scope && scopeComputeKey(key)) {
			return errors.New("metadata contains invalid, secret-shaped, or reserved compute facts")
		}
		total += len(key) + len(value)
		if total > MaximumScopeTargetAttributeBytes {
			return errors.New("metadata exceeds its aggregate byte limit")
		}
	}
	return nil
}

// ValidatePublicText applies the stable, non-secret public transport rule used
// by identity requests and durable bindings without echoing rejected input.
func ValidatePublicText(value string, maximum int, required bool) error {
	if !validStableText(value, maximum, required) || unsafeMetadataValue(value) {
		return errors.New("public identity text is invalid or secret-shaped")
	}
	return nil
}

// ValidatePublicAttributes applies the same bounded non-secret metadata rule
// to appworkflow request attributes and durable identity extensions.
func ValidatePublicAttributes(input map[string]string) error {
	return validateMetadata(input, false)
}

func validateSortedUnique(input []string, maximum int, field string) error {
	if len(input) > maximum || !sort.StringsAreSorted(input) {
		return fmt.Errorf("%s must be sorted and bounded", field)
	}
	total := 0
	for index, value := range input {
		if !validStableText(value, 128, true) || sensitiveMetadataKey(value) || unsafeIdentifierValue(value) || (index > 0 && value == input[index-1]) {
			return fmt.Errorf("%s must contain stable unique non-secret values", field)
		}
		total += len(value)
		if total > MaximumScopeTargetAttributeBytes {
			return fmt.Errorf("%s exceeds its aggregate byte limit", field)
		}
	}
	return nil
}

func unsafeIdentifierValue(value string) bool {
	return unsafeMetadataValue(value) || strings.Contains(value, "://")
}

func validStableText(value string, maximum int, required bool) bool {
	if value == "" {
		return !required
	}
	if len(value) > maximum || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return false
		}
	}
	return true
}

var credentialAssignment = regexp.MustCompile(`(?i)(^|[^a-z0-9])(token|password|api[-_. ]?key|signature)[[:space:]]*[:=]`)

func unsafeMetadataValue(value string) bool {
	if value == "" {
		return false
	}
	lower := strings.ToLower(value)
	if strings.Contains(lower, "secret://") || strings.Contains(lower, "bearer ") || strings.Contains(lower, "basic ") || credentialAssignment.MatchString(value) {
		return true
	}
	parsed, err := url.ParseRequestURI(value)
	if err == nil && parsed.Scheme != "" {
		return parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != ""
	}
	return strings.Contains(value, "://") || uriShaped(value)
}

func uriShaped(value string) bool {
	separator := strings.IndexByte(value, ':')
	if separator <= 0 {
		return false
	}
	for index, current := range value[:separator] {
		if (current >= 'a' && current <= 'z') || (current >= 'A' && current <= 'Z') || (index > 0 && ((current >= '0' && current <= '9') || current == '+' || current == '-' || current == '.')) {
			continue
		}
		return false
	}
	return true
}

func sensitiveMetadataKey(key string) bool {
	normalized := strings.NewReplacer("-", "", "_", "", ".", "", " ", "").Replace(strings.ToLower(key))
	for _, marker := range []string{"authorization", "credential", "password", "secret", "token", "apikey", "accesskey", "privatekey", "cookie", "bearer", "signature"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func scopeComputeKey(key string) bool {
	normalized := strings.NewReplacer("-", "", "_", "", ".", "", " ", "").Replace(strings.ToLower(key))
	for _, marker := range []string{"workspace", "cwd", "environment", "capability", "sandbox", "readiness", "lease", "executiontarget", "compute"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func legacyWorkspaceKey(key string) bool {
	normalized := strings.NewReplacer("-", "", "_", "", ".", "", " ", "").Replace(strings.ToLower(key))
	return strings.Contains(normalized, "workspaceid")
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func cloneConfigReferences(input map[string]TargetConfigReference) map[string]TargetConfigReference {
	if input == nil {
		return nil
	}
	result := make(map[string]TargetConfigReference, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func containsAll(have, required []string) bool {
	for _, value := range required {
		if !containsString(have, value) {
			return false
		}
	}
	return true
}

func containsString(input []string, target string) bool {
	for _, value := range input {
		if value == target {
			return true
		}
	}
	return false
}

func labelsMatch(have, required map[string]string) bool {
	for key, value := range required {
		if have[key] != value {
			return false
		}
	}
	return true
}

func containsTargetKind(input []ExecutionTargetKind, target ExecutionTargetKind) bool {
	for _, value := range input {
		if value == target {
			return true
		}
	}
	return false
}

func containsSandboxMode(input []SandboxMode, target SandboxMode) bool {
	for _, value := range input {
		if value == target {
			return true
		}
	}
	return false
}
