package hoststate

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"time"
)

const (
	DefaultCredentialOverlapSeconds int64 = 900
	MaximumCredentialOverlapSeconds int64 = 86400
	MaximumCredentialTTLSeconds     int64 = 31536000
)

// IssueCredentialRequest names existing authority, never replacement grants.
type IssueCredentialRequest struct {
	PrincipalID        string `json:"principal_id"`
	CredentialID       string `json:"credential_id"`
	ExpectedGeneration uint64 `json:"expected_generation"`
	IdempotencyKey     string `json:"idempotency_key"`
	OverlapSeconds     *int64 `json:"overlap_seconds,omitempty"`
	TTLSeconds         *int64 `json:"ttl_seconds,omitempty"`
}

func (r IssueCredentialRequest) Validate() error {
	if err := validateCredentialMutation(r.PrincipalID, r.CredentialID, r.ExpectedGeneration, r.IdempotencyKey); err != nil {
		return err
	}
	if r.OverlapSeconds != nil && (*r.OverlapSeconds < 0 || *r.OverlapSeconds > MaximumCredentialOverlapSeconds) {
		return errors.New("invalid credential overlap")
	}
	if r.TTLSeconds != nil && (*r.TTLSeconds <= 0 || *r.TTLSeconds > MaximumCredentialTTLSeconds) {
		return errors.New("invalid credential TTL")
	}
	return nil
}

func (r IssueCredentialRequest) EffectiveOverlapSeconds() int64 {
	if r.OverlapSeconds == nil {
		return DefaultCredentialOverlapSeconds
	}
	return *r.OverlapSeconds
}

type RevokeCredentialRequest struct {
	PrincipalID        string `json:"principal_id"`
	CredentialID       string `json:"credential_id"`
	ExpectedGeneration uint64 `json:"expected_generation"`
	IdempotencyKey     string `json:"idempotency_key"`
}

func (r RevokeCredentialRequest) Validate() error {
	return validateCredentialMutation(r.PrincipalID, r.CredentialID, r.ExpectedGeneration, r.IdempotencyKey)
}

func validateCredentialMutation(principal, credential string, generation uint64, key string) error {
	if ValidatePublicText(principal, 256, true) != nil || generation == 0 || generation > math.MaxInt64 || ValidatePublicText(key, 256, true) != nil || key != strings.TrimSpace(key) || !ValidCredentialID(credential) {
		return errors.New("invalid credential mutation")
	}
	return nil
}

func ValidCredentialID(id string) bool {
	if !strings.HasPrefix(id, "cred_") || len(id) != 37 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(id, "cred_"))
	return err == nil && id == strings.ToLower(id)
}

// CredentialMetadata is safe for audit and public administration responses.
type CredentialMetadata struct {
	PrincipalID         string     `json:"principal_id"`
	CredentialID        string     `json:"credential_id"`
	Generation          uint64     `json:"generation"`
	OperationGeneration uint64     `json:"operation_generation,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	ExpiresAt           *time.Time `json:"expires_at,omitempty"`
	OverlapUntil        *time.Time `json:"overlap_until,omitempty"`
	LastUsedAt          *time.Time `json:"last_used_at,omitempty"`
	RevokedAt           *time.Time `json:"revoked_at,omitempty"`
	Replayed            bool       `json:"replayed"`
	SecretAvailable     bool       `json:"secret_available"`
	Status              string     `json:"status"`
}

type CredentialList struct {
	PrincipalID string               `json:"principal_id"`
	Generation  uint64               `json:"generation"`
	Credentials []CredentialMetadata `json:"credentials"`
}

// CredentialAudit contains only authorized, secret-free mutation provenance.
type CredentialAudit struct {
	ID           uint64    `json:"id"`
	PrincipalID  string    `json:"principal_id"`
	CredentialID string    `json:"credential_id"`
	Generation   uint64    `json:"generation"`
	Operation    string    `json:"operation"`
	Actor        string    `json:"actor"`
	Source       string    `json:"source"`
	CreatedAt    time.Time `json:"created_at"`
}

// CredentialRefusal includes current metadata for an already-revoked target.
type CredentialRefusal struct {
	Credential CredentialMetadata `json:"credential"`
}

func (r *CredentialRefusal) Error() string { return "credential is already revoked" }
func (r *CredentialRefusal) Unwrap() error { return ErrConflict }

// CredentialIssue separates the one-time secret from durable public metadata.
// Formatting it deliberately cannot reveal the secret.
type CredentialIssue struct {
	CredentialMetadata
	Secret string `json:"secret,omitempty"`
}

func (r CredentialIssue) String() string   { return "credential issuance (secret redacted)" }
func (r CredentialIssue) GoString() string { return r.String() }

// CredentialAdministrator comes only from an authenticated transport. Recheck
// executes inside the issuer transaction; it and its bearer are never stored.
type CredentialAdministrator struct {
	Actor   string
	Source  string
	Recheck func(context.Context) error
}

// ErrCredentialAdministrator denotes missing or no-longer-valid issuer authority.
var ErrCredentialAdministrator = errors.New("credential administrator refused")

func (a CredentialAdministrator) Validate(ctx context.Context) error {
	if ctx == nil || a.Recheck == nil || ValidatePublicText(a.Actor, 256, true) != nil || ValidatePublicText(a.Source, 512, true) != nil {
		return ErrCredentialAdministrator
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.Recheck(ctx) != nil {
		return ErrCredentialAdministrator
	}
	return nil
}
