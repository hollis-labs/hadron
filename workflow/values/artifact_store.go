package values

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// ArtifactOperation identifies an authorization boundary. Implementations
// must authorize before resolving a local path or contacting a delegate.
type ArtifactOperation string

const (
	ArtifactOperationPut     ArtifactOperation = "put"
	ArtifactOperationOpen    ArtifactOperation = "open"
	ArtifactOperationStat    ArtifactOperation = "stat"
	ArtifactOperationDelete  ArtifactOperation = "delete"
	ArtifactOperationCleanup ArtifactOperation = "cleanup"
)

// Valid reports whether o is a supported artifact operation.
func (o ArtifactOperation) Valid() bool {
	switch o {
	case ArtifactOperationPut, ArtifactOperationOpen, ArtifactOperationStat,
		ArtifactOperationDelete, ArtifactOperationCleanup:
		return true
	default:
		return false
	}
}

// ArtifactOwnerScope names the only locally durable ownership scopes.
type ArtifactOwnerScope string

const (
	ArtifactOwnerRun     ArtifactOwnerScope = "run"
	ArtifactOwnerProject ArtifactOwnerScope = "project"
)

// Valid reports whether s is a supported local ownership scope.
func (s ArtifactOwnerScope) Valid() bool {
	return s == ArtifactOwnerRun || s == ArtifactOwnerProject
}

// ArtifactOwner identifies a host-owned lifetime without importing host run
// or project types. ID is opaque and must never be used directly as a path.
type ArtifactOwner struct {
	Scope ArtifactOwnerScope `json:"scope"`
	ID    string             `json:"id"`
}

// Validate checks that owner is a complete local ownership claim.
func (o ArtifactOwner) Validate() error {
	if !o.Scope.Valid() || !stableArtifactIdentity(o.ID, false) {
		return NewArtifactError("", ArtifactFailureInvalid, nil, ErrArtifactInvalid)
	}
	return nil
}

// ArtifactAccess contains opaque claims supplied by a host authorization
// boundary. At is explicit so expiry checks and tests remain deterministic.
type ArtifactAccess struct {
	Principal string    `json:"principal"`
	RunID     string    `json:"run_id,omitempty"`
	ProjectID string    `json:"project_id,omitempty"`
	At        time.Time `json:"at"`
}

// Validate rejects incomplete access rather than selecting an implicit
// principal or clock.
func (a ArtifactAccess) Validate(operation ArtifactOperation) error {
	if !operation.Valid() || !stableArtifactIdentity(a.Principal, false) ||
		!stableArtifactIdentity(a.RunID, true) || !stableArtifactIdentity(a.ProjectID, true) || a.At.IsZero() {
		return NewArtifactError(operation, ArtifactFailureUnauthorized, nil, ErrArtifactUnauthorized)
	}
	return nil
}

func stableArtifactIdentity(value string, optional bool) bool {
	if value == "" {
		return optional
	}
	if !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	return !strings.ContainsFunc(value, func(character rune) bool {
		return unicode.IsControl(character) || (unicode.IsSpace(character) && character != ' ')
	})
}

// ArtifactAuthorization is the complete application-neutral policy request.
// Ref is nil only for Put and owner-wide cleanup before an immutable reference
// exists. Owner is present whenever it is known without resolving storage.
type ArtifactAuthorization struct {
	Operation ArtifactOperation
	Access    ArtifactAccess
	Ref       *ArtifactRef
	Owner     *ArtifactOwner
}

// ArtifactAuthorizer makes resolution fail closed without prescribing a host
// principal, ACL, or identity implementation.
type ArtifactAuthorizer interface {
	AuthorizeArtifact(context.Context, ArtifactAuthorization) error
}

// ArtifactAuthorizerFunc adapts a function to ArtifactAuthorizer.
type ArtifactAuthorizerFunc func(context.Context, ArtifactAuthorization) error

// AuthorizeArtifact implements ArtifactAuthorizer.
func (f ArtifactAuthorizerFunc) AuthorizeArtifact(ctx context.Context, request ArtifactAuthorization) error {
	if f == nil {
		return ErrArtifactUnauthorized
	}
	return f(ctx, request)
}

// ArtifactPutRequest describes one immutable streaming write. MaxBytes is a
// required hard stream limit and may be much larger than the inline cap.
type ArtifactPutRequest struct {
	Store          string
	Owner          ArtifactOwner
	Metadata       Metadata
	ExpectedDigest string
	ExpectedSize   *int64
	MaxBytes       int64
	CreatedAt      time.Time
	ExpiresAt      time.Time
	Access         ArtifactAccess
}

// Validate checks request invariants before an adapter consumes source.
func (r ArtifactPutRequest) Validate() error {
	return r.validate(false)
}

func (r ArtifactPutRequest) snapshot() ArtifactPutRequest {
	if r.ExpectedSize != nil {
		expected := *r.ExpectedSize
		r.ExpectedSize = &expected
	}
	return r
}

func (r ArtifactPutRequest) validate(allowEphemeral bool) error {
	if strings.TrimSpace(r.Store) == "" || strings.TrimSpace(r.Store) != r.Store {
		return NewArtifactError(ArtifactOperationPut, ArtifactFailureAuthority, nil, ErrArtifactAuthority)
	}
	if err := r.Owner.Validate(); err != nil {
		return NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, err)
	}
	if err := r.Metadata.Validate(); err != nil {
		return NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, err)
	}
	if err := r.Access.Validate(ArtifactOperationPut); err != nil {
		return err
	}
	if r.MaxBytes <= 0 || r.MaxBytes == math.MaxInt64 || r.CreatedAt.IsZero() || (!r.ExpiresAt.IsZero() && !r.ExpiresAt.After(r.CreatedAt)) {
		return NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, ErrArtifactInvalid)
	}
	if r.ExpectedDigest != "" {
		if err := ValidateDigest(r.ExpectedDigest); err != nil {
			return NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, err)
		}
	}
	if r.ExpectedSize != nil && (*r.ExpectedSize < 0 || *r.ExpectedSize > r.MaxBytes) {
		return NewArtifactError(ArtifactOperationPut, ArtifactFailureSize, nil, ErrArtifactSizeLimit)
	}
	switch r.Metadata.Retention {
	case RetentionRun:
		if r.Owner.Scope != ArtifactOwnerRun {
			return NewArtifactError(ArtifactOperationPut, ArtifactFailureRetention, nil, ErrArtifactRetention)
		}
	case RetentionProject:
		if r.Owner.Scope != ArtifactOwnerProject {
			return NewArtifactError(ArtifactOperationPut, ArtifactFailureRetention, nil, ErrArtifactRetention)
		}
	case RetentionNone:
		if allowEphemeral {
			return nil
		}
		return NewArtifactError(ArtifactOperationPut, ArtifactFailureRetention, nil, ErrArtifactRetention)
	case RetentionExternal:
		return NewArtifactError(ArtifactOperationPut, ArtifactFailureRetention, nil, ErrArtifactRetention)
	default:
		return NewArtifactError(ArtifactOperationPut, ArtifactFailureRetention, nil, ErrArtifactRetention)
	}
	return nil
}

// ArtifactMetadata is the immutable storage-side record for one reference.
// Owner and expiry remain adapter metadata; workflow state stores only Ref.
type ArtifactMetadata struct {
	Ref       ArtifactRef   `json:"ref"`
	Owner     ArtifactOwner `json:"owner"`
	CreatedAt time.Time     `json:"created_at"`
	ExpiresAt time.Time     `json:"expires_at,omitempty"`
}

// Validate checks immutable metadata without resolving content.
func (m ArtifactMetadata) Validate() error {
	if err := m.Ref.Validate(); err != nil {
		return NewArtifactError(ArtifactOperationStat, ArtifactFailureInvalid, &m.Ref, err)
	}
	if err := m.Owner.Validate(); err != nil {
		return NewArtifactError(ArtifactOperationStat, ArtifactFailureInvalid, &m.Ref, err)
	}
	if m.CreatedAt.IsZero() || (!m.ExpiresAt.IsZero() && !m.ExpiresAt.After(m.CreatedAt)) {
		return NewArtifactError(ArtifactOperationStat, ArtifactFailureInvalid, &m.Ref, ErrArtifactInvalid)
	}
	if (m.Ref.Retention == RetentionRun && m.Owner.Scope != ArtifactOwnerRun) ||
		(m.Ref.Retention == RetentionProject && m.Owner.Scope != ArtifactOwnerProject) ||
		(m.Ref.Retention != RetentionRun && m.Ref.Retention != RetentionProject && m.Ref.Retention != RetentionExternal) {
		return NewArtifactError(ArtifactOperationStat, ArtifactFailureRetention, &m.Ref, ErrArtifactRetention)
	}
	return nil
}

// ArtifactDeleteRequest requests deletion of one locally owned reference.
type ArtifactDeleteRequest struct {
	Access ArtifactAccess
	Ref    ArtifactRef
}

// Validate checks an exact-reference deletion request without resolving it.
func (r ArtifactDeleteRequest) Validate() error { return validateDeleteRequest(r) }

// ArtifactCleanupKind selects a deterministic cleanup boundary.
type ArtifactCleanupKind string

const (
	ArtifactCleanupRun      ArtifactCleanupKind = "run"
	ArtifactCleanupProject  ArtifactCleanupKind = "project"
	ArtifactCleanupExpired  ArtifactCleanupKind = "expired"
	ArtifactCleanupPartials ArtifactCleanupKind = "partials"
	ArtifactCleanupNone     ArtifactCleanupKind = "none"
	ArtifactCleanupExternal ArtifactCleanupKind = "external"
)

// Valid reports whether k is a supported cleanup boundary.
func (k ArtifactCleanupKind) Valid() bool {
	switch k {
	case ArtifactCleanupRun, ArtifactCleanupProject, ArtifactCleanupExpired,
		ArtifactCleanupPartials, ArtifactCleanupNone, ArtifactCleanupExternal:
		return true
	default:
		return false
	}
}

// ArtifactCleanupRequest selects owner, expiry, partial, or non-local cleanup.
type ArtifactCleanupRequest struct {
	Access ArtifactAccess
	Kind   ArtifactCleanupKind
	Owner  ArtifactOwner
	Ref    *ArtifactRef
	Before time.Time
}

// Validate checks a cleanup request without resolving storage.
func (r ArtifactCleanupRequest) Validate() error { return validateCleanupRequest(r) }

// ArtifactCleanupOutcome is an observable idempotent cleanup result.
type ArtifactCleanupOutcome string

const (
	ArtifactCleanupDeleted           ArtifactCleanupOutcome = "deleted"
	ArtifactCleanupAlreadyAbsent     ArtifactCleanupOutcome = "already_absent"
	ArtifactCleanupNotStored         ArtifactCleanupOutcome = "not_stored"
	ArtifactCleanupPreservedExternal ArtifactCleanupOutcome = "preserved_external"
)

// ArtifactCleanupResult exposes only an aggregate count, never references,
// digests, URIs, owner identifiers, local paths, or content.
type ArtifactCleanupResult struct {
	Outcome      ArtifactCleanupOutcome `json:"outcome"`
	DeletedCount int                    `json:"deleted_count,omitempty"`
}

// Valid reports whether o is a supported cleanup result.
func (o ArtifactCleanupOutcome) Valid() bool {
	switch o {
	case ArtifactCleanupDeleted, ArtifactCleanupAlreadyAbsent,
		ArtifactCleanupNotStored, ArtifactCleanupPreservedExternal:
		return true
	default:
		return false
	}
}

// Validate rejects ambiguous cleanup results. Aggregate counts deliberately
// avoid exposing secret artifact references or digests.
func (r ArtifactCleanupResult) Validate() error {
	if !r.Outcome.Valid() || r.DeletedCount < 0 ||
		(r.Outcome == ArtifactCleanupDeleted && r.DeletedCount == 0) ||
		(r.Outcome != ArtifactCleanupDeleted && r.DeletedCount != 0) {
		return NewArtifactError(ArtifactOperationCleanup, ArtifactFailureInvalid, nil, ErrArtifactInvalid)
	}
	return nil
}

// ArtifactReadCloser is a streaming, digest-verifying artifact body.
type ArtifactReadCloser interface {
	io.ReadCloser
	Metadata() ArtifactMetadata
	Verified() bool
}

// ArtifactStore is the extraction-ready artifact producer/resolver contract.
// Implementations authorize before resolution; external delegates enforce the
// same contract themselves.
type ArtifactStore interface {
	Put(context.Context, ArtifactPutRequest, io.Reader) (ArtifactMetadata, error)
	Open(context.Context, ArtifactAccess, ArtifactRef) (ArtifactReadCloser, error)
	Stat(context.Context, ArtifactAccess, ArtifactRef) (ArtifactMetadata, error)
	Delete(context.Context, ArtifactDeleteRequest) (ArtifactCleanupResult, error)
	Cleanup(context.Context, ArtifactCleanupRequest) (ArtifactCleanupResult, error)
}

// CheckArtifactExpiry applies the common deterministic expiry rule.
func CheckArtifactExpiry(operation ArtifactOperation, metadata ArtifactMetadata, at time.Time) error {
	if at.IsZero() {
		return NewArtifactError(operation, ArtifactFailureUnauthorized, &metadata.Ref, ErrArtifactUnauthorized)
	}
	if !metadata.ExpiresAt.IsZero() && !metadata.ExpiresAt.After(at) {
		return NewArtifactError(operation, ArtifactFailureExpired, &metadata.Ref, ErrArtifactExpired)
	}
	return nil
}

func validateCleanupRequest(request ArtifactCleanupRequest) error {
	if err := request.Access.Validate(ArtifactOperationCleanup); err != nil {
		return err
	}
	if !request.Kind.Valid() {
		return NewArtifactError(ArtifactOperationCleanup, ArtifactFailureInvalid, request.Ref, ErrArtifactInvalid)
	}
	switch request.Kind {
	case ArtifactCleanupRun:
		if request.Owner.Scope != ArtifactOwnerRun || request.Ref != nil || !request.Before.IsZero() {
			return NewArtifactError(ArtifactOperationCleanup, ArtifactFailureInvalid, request.Ref, ErrArtifactInvalid)
		}
		if err := request.Owner.Validate(); err != nil {
			return NewArtifactError(ArtifactOperationCleanup, ArtifactFailureInvalid, request.Ref, err)
		}
		return nil
	case ArtifactCleanupProject:
		if request.Owner.Scope != ArtifactOwnerProject || request.Ref != nil || !request.Before.IsZero() {
			return NewArtifactError(ArtifactOperationCleanup, ArtifactFailureInvalid, request.Ref, ErrArtifactInvalid)
		}
		if err := request.Owner.Validate(); err != nil {
			return NewArtifactError(ArtifactOperationCleanup, ArtifactFailureInvalid, request.Ref, err)
		}
		return nil
	case ArtifactCleanupExpired, ArtifactCleanupPartials:
		if request.Before.IsZero() || request.Ref != nil || request.Owner != (ArtifactOwner{}) {
			return NewArtifactError(ArtifactOperationCleanup, ArtifactFailureInvalid, request.Ref, ErrArtifactInvalid)
		}
	case ArtifactCleanupExternal:
		if request.Ref == nil || request.Ref.Retention != RetentionExternal ||
			request.Owner != (ArtifactOwner{}) || !request.Before.IsZero() {
			return NewArtifactError(ArtifactOperationCleanup, ArtifactFailureInvalid, request.Ref, ErrArtifactInvalid)
		}
	case ArtifactCleanupNone:
		// A no-retain value never has an artifact to resolve.
		if request.Ref != nil || request.Owner != (ArtifactOwner{}) || !request.Before.IsZero() {
			return NewArtifactError(ArtifactOperationCleanup, ArtifactFailureInvalid, request.Ref, ErrArtifactInvalid)
		}
	}
	return nil
}

func validateDeleteRequest(request ArtifactDeleteRequest) error {
	if err := request.Access.Validate(ArtifactOperationDelete); err != nil {
		return err
	}
	if err := request.Ref.Validate(); err != nil {
		return NewArtifactError(ArtifactOperationDelete, ArtifactFailureInvalid, &request.Ref, err)
	}
	return nil
}

func safeArtifactOperation(operation ArtifactOperation) ArtifactOperation {
	if operation.Valid() {
		return operation
	}
	return ArtifactOperation("operation")
}

func artifactInvariant(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrArtifactInvalid, fmt.Sprintf(format, args...))
}
