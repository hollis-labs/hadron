package hoststate

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
)

const (
	DefaultMaximumDirectTools = 24
	MaximumDirectTools        = 256
	MaximumExposureNamespaces = 128
	MaximumExposurePins       = 256
	MaximumMCPTokenBytes      = 16 << 10
)

// ExposureSearchScope is the closed registry-discovery boundary retained by
// one exposure profile. Direct pins and agent-owned tools remain separate.
type ExposureSearchScope string

const (
	ExposureSearchNone       ExposureSearchScope = "none"
	ExposureSearchPublic     ExposureSearchScope = "public"
	ExposureSearchNamespaces ExposureSearchScope = "namespaces"
	ExposureSearchAll        ExposureSearchScope = "all"
)

func (s ExposureSearchScope) Valid() bool {
	return s == ExposureSearchNone || s == ExposureSearchPublic || s == ExposureSearchNamespaces || s == ExposureSearchAll
}

// ExposureProfileRecord is Hadron's durable local direct/discovery policy.
// Exact pins never carry source bytes or transport credentials.
type ExposureProfileRecord struct {
	ID             string                `json:"id"`
	Namespaces     []string              `json:"namespaces,omitempty"`
	Pins           []graph.DefinitionRef `json:"pins,omitempty"`
	DeniedEffects  graph.EffectSet       `json:"denied_effects,omitempty"`
	MaxDirectTools int                   `json:"max_direct_tools"`
	SearchScope    ExposureSearchScope   `json:"search_scope"`
	LazyLoad       bool                  `json:"lazy_load"`
	Display        values.DisplayPolicy  `json:"display,omitempty"`
}

func (r ExposureProfileRecord) Validate() error {
	if err := ValidatePublicText(r.ID, 256, true); err != nil {
		return errors.New("exposure profile id is invalid")
	}
	if len(r.Namespaces) > MaximumExposureNamespaces || !sort.StringsAreSorted(r.Namespaces) {
		return errors.New("exposure profile namespaces must be bounded and sorted")
	}
	for index, namespace := range r.Namespaces {
		if err := validateExposureRegistryName(namespace); err != nil {
			return fmt.Errorf("exposure namespace[%d]: %w", index, err)
		}
		if index > 0 && r.Namespaces[index-1] == namespace {
			return errors.New("exposure profile namespaces must be unique")
		}
	}
	if len(r.Pins) > MaximumExposurePins {
		return errors.New("exposure profile pins exceed the supported bound")
	}
	for index, pin := range r.Pins {
		if err := validateExposurePin(pin); err != nil {
			return fmt.Errorf("exposure pin[%d]: %w", index, err)
		}
		if index > 0 && r.Pins[index-1].ID >= pin.ID {
			return errors.New("exposure profile pins must use canonical unique workflow-name order")
		}
	}
	if len(r.DeniedEffects) > 5 || !sort.SliceIsSorted(r.DeniedEffects, func(i, j int) bool { return r.DeniedEffects[i] < r.DeniedEffects[j] }) {
		return errors.New("denied effects must be bounded and sorted")
	}
	for index, effect := range r.DeniedEffects {
		if !effect.Valid() || (index > 0 && r.DeniedEffects[index-1] == effect) {
			return errors.New("denied effects must be valid and unique")
		}
	}
	if r.MaxDirectTools < 1 || r.MaxDirectTools > MaximumDirectTools {
		return fmt.Errorf("maximum direct tools must be between 1 and %d", MaximumDirectTools)
	}
	if !r.SearchScope.Valid() {
		return errors.New("exposure search scope is invalid")
	}
	if err := r.Display.Validate(); err != nil {
		return fmt.Errorf("exposure display policy: %w", err)
	}
	return nil
}

func (r ExposureProfileRecord) Clone() ExposureProfileRecord {
	r.Namespaces = append([]string(nil), r.Namespaces...)
	r.Pins = append([]graph.DefinitionRef(nil), r.Pins...)
	r.DeniedEffects = append(graph.EffectSet(nil), r.DeniedEffects...)
	return r
}

type ExposureProfileSnapshot struct {
	Record     ExposureProfileRecord `json:"record"`
	Generation uint64                `json:"generation"`
	CreatedAt  time.Time             `json:"created_at"`
	UpdatedAt  time.Time             `json:"updated_at"`
}

func (s ExposureProfileSnapshot) Validate() error {
	if err := s.Record.Validate(); err != nil {
		return err
	}
	if s.Generation == 0 || s.CreatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) || s.CreatedAt.Location() != time.UTC || s.UpdatedAt.Location() != time.UTC {
		return errors.New("exposure profile snapshot requires generation and ordered UTC times")
	}
	return nil
}

func (s ExposureProfileSnapshot) Clone() ExposureProfileSnapshot {
	s.Record = s.Record.Clone()
	s.CreatedAt = s.CreatedAt.UTC()
	s.UpdatedAt = s.UpdatedAt.UTC()
	return s
}

// MCPPrincipalRecord binds one credential digest to a full default workflow
// identity and an optional named exposure profile. CredentialDigest is
// deliberately excluded from JSON and every transport projection.
type MCPPrincipalRecord struct {
	ID               string          `json:"id"`
	ProfileID        string          `json:"profile_id,omitempty"`
	Identity         IdentityBinding `json:"identity"`
	AgentNamespace   string          `json:"agent_namespace,omitempty"`
	CredentialDigest string          `json:"-"`
}

func (r MCPPrincipalRecord) Validate() error {
	if err := ValidatePublicText(r.ID, 256, true); err != nil || r.Identity.Principal != r.ID {
		return errors.New("MCP principal id must match its identity binding")
	}
	if err := r.Identity.Validate(); err != nil {
		return fmt.Errorf("MCP principal identity: %w", err)
	}
	if err := ValidatePublicText(r.ProfileID, 256, false); err != nil {
		return errors.New("MCP principal profile id is invalid")
	}
	derived, err := AgentNamespaceForPrincipal(r.ID, r.AgentNamespace)
	if err != nil || derived != r.AgentNamespace {
		return errors.New("MCP principal agent namespace is invalid or non-canonical")
	}
	if !validCredentialDigest(r.CredentialDigest) {
		return errors.New("MCP principal credential digest is invalid")
	}
	return nil
}

// AgentNamespaceForPrincipal validates an explicit namespace or derives the
// canonical slash-delimited suffix of an agent:<namespace> principal.
func AgentNamespaceForPrincipal(principal, explicit string) (string, error) {
	if strings.HasPrefix(principal, "agent:") {
		derived := strings.TrimPrefix(principal, "agent:")
		if err := validateExposureRegistryName(derived); err != nil {
			return "", errors.New("agent principal has an invalid owned namespace")
		}
		if explicit != "" && explicit != derived {
			return "", errors.New("agent principal namespace differs from its canonical suffix")
		}
		return derived, nil
	}
	if explicit != "" {
		if err := validateExposureRegistryName(explicit); err != nil {
			return "", fmt.Errorf("explicit agent namespace: %w", err)
		}
	}
	return explicit, nil
}

func (r MCPPrincipalRecord) Clone() MCPPrincipalRecord {
	r.Identity = r.Identity.Clone()
	return r
}

// Public removes the credential verifier before returning a principal through
// a service or transport DTO.
func (r MCPPrincipalRecord) Public() MCPPrincipalRecord {
	r = r.Clone()
	r.CredentialDigest = ""
	return r
}

type MCPPrincipalSnapshot struct {
	Record     MCPPrincipalRecord `json:"record"`
	Generation uint64             `json:"generation"`
	CreatedAt  time.Time          `json:"created_at"`
	UpdatedAt  time.Time          `json:"updated_at"`
}

func (s MCPPrincipalSnapshot) Validate() error {
	if err := s.Record.Validate(); err != nil {
		return err
	}
	if s.Generation == 0 || s.CreatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) || s.CreatedAt.Location() != time.UTC || s.UpdatedAt.Location() != time.UTC {
		return errors.New("MCP principal snapshot requires generation and ordered UTC times")
	}
	return nil
}

func (s MCPPrincipalSnapshot) Clone() MCPPrincipalSnapshot {
	s.Record = s.Record.Clone()
	s.CreatedAt = s.CreatedAt.UTC()
	s.UpdatedAt = s.UpdatedAt.UTC()
	return s
}

func (s MCPPrincipalSnapshot) Public() MCPPrincipalSnapshot {
	s = s.Clone()
	s.Record = s.Record.Public()
	return s
}

func DigestMCPToken(token string) (string, error) {
	if err := ValidateMCPToken(token); err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(token))
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

// ValidateMCPToken defines the one lossless credential form accepted by both
// durable principal records and MCP transport parsing. Secret material is
// rejected rather than trimmed or otherwise rewritten.
func ValidateMCPToken(token string) error {
	if token == "" || len(token) > MaximumMCPTokenBytes || !utf8.ValidString(token) || token != strings.TrimSpace(token) || strings.IndexFunc(token, unicode.IsControl) >= 0 {
		return errors.New("MCP token is invalid")
	}
	return nil
}

func MatchMCPTokenDigest(stored, candidate string) bool {
	if !validCredentialDigest(stored) || !validCredentialDigest(candidate) || len(stored) != len(candidate) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(stored), []byte(candidate)) == 1
}

func validCredentialDigest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func validateExposurePin(ref graph.DefinitionRef) error {
	if ref.Kind != "registry" || ref.ID == "" || ref.Version == "" || ref.Digest == "" || ref.Authority != "" || ref.Locator != "" || ref.Provenance != nil {
		return errors.New("pin must be an exact registry name, version, and digest")
	}
	if err := validateExposureRegistryName(ref.ID); err != nil {
		return err
	}
	if err := ValidatePublicText(ref.Version, 256, true); err != nil {
		return errors.New("pin version is invalid")
	}
	if err := values.ValidateDigest(ref.Digest); err != nil {
		return fmt.Errorf("pin digest: %w", err)
	}
	return nil
}

func validateExposureRegistryName(value string) error {
	if value == "" || !utf8.ValidString(value) || strings.Contains(value, "\\") || strings.HasPrefix(value, "/") || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return errors.New("registry name is invalid")
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errors.New("registry name is invalid")
		}
	}
	return nil
}
