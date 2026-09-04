package hoststate

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

const MaximumA2ATaskIDBytes = 128

// A2ATaskCorrelation is the immutable durable link between a public A2A task
// and its graph-native run. Workflow lifecycle state remains owned by Run.
type A2ATaskCorrelation struct {
	TaskID         string              `json:"task_id"`
	RunID          runtime.RunID       `json:"run_id"`
	Definition     graph.DefinitionRef `json:"definition"`
	RequestDigest  string              `json:"request_digest"`
	IdempotencyKey string              `json:"idempotency_key"`
	// HostStartKey is the owner/task-scoped internal key supplied to the
	// graph-native Host. The caller key above remains the public A2A intent.
	HostStartKey string `json:"-"`
	// Owner is an internal authorization binding. It must never be rendered by
	// an A2A transport or treated as caller-supplied identity.
	Owner     IdentityBinding `json:"-"`
	CreatedAt time.Time       `json:"created_at"`
}

func (c A2ATaskCorrelation) Validate() error {
	if err := ValidateA2ATaskID(c.TaskID); err != nil {
		return err
	}
	if err := ValidatePublicText(string(c.RunID), 256, true); err != nil {
		return errors.New("A2A run id is invalid")
	}
	if err := ValidateA2ADefinition(c.Definition); err != nil {
		return err
	}
	if err := values.ValidateDigest(c.RequestDigest); err != nil {
		return fmt.Errorf("A2A request digest: %w", err)
	}
	if err := ValidatePublicText(c.IdempotencyKey, 512, true); err != nil {
		return errors.New("A2A idempotency key is invalid")
	}
	if err := ValidatePublicText(c.HostStartKey, 512, true); err != nil {
		return errors.New("A2A host start key is invalid")
	}
	if err := c.Owner.Validate(); err != nil {
		return fmt.Errorf("A2A owner binding: %w", err)
	}
	if c.CreatedAt.IsZero() || c.CreatedAt.Location() != time.UTC {
		return errors.New("A2A correlation requires a UTC creation time")
	}
	return nil
}

// ValidateA2ATaskID rejects task identifiers that are ambiguous in the
// repository's path-based A2A transport.
func ValidateA2ATaskID(value string) error {
	if value == "" || len(value) > MaximumA2ATaskIDBytes || !utf8.ValidString(value) {
		return errors.New("A2A task id is invalid")
	}
	for _, current := range value {
		allowed := current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z' || current >= '0' && current <= '9' || current == '-' || current == '_' || current == '.' || current == '~'
		if unicode.IsControl(current) || !allowed {
			return errors.New("A2A task id is invalid")
		}
	}
	return nil
}

// Clone returns an independent copy of all owner authority facts.
func (c A2ATaskCorrelation) Clone() A2ATaskCorrelation {
	c.Owner = c.Owner.Clone()
	return c
}

// ValidateA2ADefinition requires the immutable registry tuple published by an
// agent-card skill. Resolution remains an appworkflow responsibility.
func ValidateA2ADefinition(ref graph.DefinitionRef) error {
	if ref.Kind != "registry" || ref.ID == "" || ref.Version == "" || ref.Digest == "" || ref.Authority != "" || ref.Locator != "" || ref.Provenance != nil {
		return errors.New("A2A definition must be an exact registry name, version, and digest")
	}
	if !utf8.ValidString(ref.ID) || strings.HasPrefix(ref.ID, "/") || strings.Contains(ref.ID, "\\") || strings.IndexFunc(ref.ID, unicode.IsControl) >= 0 {
		return errors.New("A2A registry name is invalid")
	}
	for _, segment := range strings.Split(ref.ID, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errors.New("A2A registry name is invalid")
		}
	}
	if err := ValidatePublicText(ref.Version, 256, true); err != nil {
		return errors.New("A2A definition version is invalid")
	}
	if err := values.ValidateDigest(ref.Digest); err != nil {
		return fmt.Errorf("A2A definition digest: %w", err)
	}
	return nil
}
