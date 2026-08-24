package values

import (
	"errors"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
)

var (
	ErrArtifactInvalid      = errors.New("invalid artifact request")
	ErrArtifactAuthority    = errors.New("artifact authority unavailable")
	ErrArtifactUnauthorized = errors.New("artifact access unauthorized")
	ErrArtifactNotFound     = errors.New("artifact not found")
	ErrArtifactDigest       = errors.New("artifact digest mismatch")
	ErrArtifactExpired      = errors.New("artifact expired")
	ErrArtifactSizeLimit    = errors.New("artifact size limit exceeded")
	ErrArtifactRetention    = errors.New("artifact retention violation")
	ErrArtifactUnverified   = errors.New("artifact stream closed before digest verification")
)

// ArtifactFailure classifies failures without exposing payloads or locators.
type ArtifactFailure string

const (
	ArtifactFailureInvalid      ArtifactFailure = "invalid"
	ArtifactFailureAuthority    ArtifactFailure = "authority"
	ArtifactFailureUnauthorized ArtifactFailure = "unauthorized"
	ArtifactFailureNotFound     ArtifactFailure = "not_found"
	ArtifactFailureDigest       ArtifactFailure = "digest"
	ArtifactFailureExpired      ArtifactFailure = "expired"
	ArtifactFailureSize         ArtifactFailure = "size"
	ArtifactFailureRetention    ArtifactFailure = "retention"
)

var artifactFailureCodes = map[ArtifactFailure]diagnostic.Code{
	ArtifactFailureInvalid:      "HADR-ARTIFACT-001",
	ArtifactFailureAuthority:    "HADR-ARTIFACT-002",
	ArtifactFailureUnauthorized: "HADR-ARTIFACT-003",
	ArtifactFailureNotFound:     "HADR-ARTIFACT-004",
	ArtifactFailureDigest:       "HADR-ARTIFACT-005",
	ArtifactFailureExpired:      "HADR-ARTIFACT-006",
	ArtifactFailureSize:         "HADR-ARTIFACT-007",
	ArtifactFailureRetention:    "HADR-ARTIFACT-008",
}

// ArtifactError is safe to render. Cause remains available through Unwrap for
// process-local inspection but Error and Diagnostic never format it, a URI,
// owner identifier, filesystem path, or payload.
type ArtifactError struct {
	operation ArtifactOperation
	failure   ArtifactFailure
	ref       *ArtifactRef
	cause     error
}

func safeArtifactFailure(failure ArtifactFailure) ArtifactFailure {
	if _, ok := artifactFailureCodes[failure]; ok {
		return failure
	}
	return ArtifactFailureInvalid
}

func artifactFailureSentinel(failure ArtifactFailure) error {
	switch safeArtifactFailure(failure) {
	case ArtifactFailureAuthority:
		return ErrArtifactAuthority
	case ArtifactFailureUnauthorized:
		return ErrArtifactUnauthorized
	case ArtifactFailureNotFound:
		return ErrArtifactNotFound
	case ArtifactFailureDigest:
		return ErrArtifactDigest
	case ArtifactFailureExpired:
		return ErrArtifactExpired
	case ArtifactFailureSize:
		return ErrArtifactSizeLimit
	case ArtifactFailureRetention:
		return ErrArtifactRetention
	default:
		return ErrArtifactInvalid
	}
}

// NewArtifactError constructs a render-safe typed artifact failure.
func NewArtifactError(operation ArtifactOperation, failure ArtifactFailure, ref *ArtifactRef, cause error) *ArtifactError {
	var refCopy *ArtifactRef
	if ref != nil {
		copyValue := *ref
		refCopy = &copyValue
	}
	if cause == nil {
		cause = ErrArtifactInvalid
	}
	return &ArtifactError{operation: operation, failure: failure, ref: refCopy, cause: cause}
}

// Error intentionally contains only closed vocabulary.
func (e *ArtifactError) Error() string {
	if e == nil {
		return "artifact operation failed"
	}
	return "artifact " + string(safeArtifactOperation(e.operation)) + " failed: " + string(safeArtifactFailure(e.failure))
}

// Is makes the stable failure sentinel searchable independently of the raw,
// process-local cause returned by Unwrap.
func (e *ArtifactError) Is(target error) bool {
	if e == nil {
		return false
	}
	return target == artifactFailureSentinel(e.failure)
}

// Unwrap exposes the raw cause to process-local error handling only.
func (e *ArtifactError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// Operation reports the failed operation.
func (e *ArtifactError) Operation() ArtifactOperation {
	if e == nil {
		return safeArtifactOperation("")
	}
	return safeArtifactOperation(e.operation)
}

// Failure reports the stable failure category.
func (e *ArtifactError) Failure() ArtifactFailure {
	if e == nil {
		return ArtifactFailureInvalid
	}
	return safeArtifactFailure(e.failure)
}

// Reference returns a defensive copy for programmatic handling. Callers must
// apply normal redaction rules before presentation.
func (e *ArtifactError) Reference() *ArtifactRef {
	if e == nil || e.ref == nil {
		return nil
	}
	copyValue := *e.ref
	return &copyValue
}

// Diagnostic returns the stable structured presentation of the failure.
func (e *ArtifactError) Diagnostic() diagnostic.Diagnostic {
	failure := ArtifactFailureInvalid
	operation := ArtifactOperation("operation")
	if e != nil {
		failure = safeArtifactFailure(e.failure)
		operation = safeArtifactOperation(e.operation)
	}
	code, ok := artifactFailureCodes[failure]
	if !ok {
		code = artifactFailureCodes[ArtifactFailureInvalid]
		failure = ArtifactFailureInvalid
	}
	message := "Artifact " + string(operation) + " failed: " + string(failure) + "."
	remediation := "Correct the artifact request or storage policy and retry."
	switch failure {
	case ArtifactFailureInvalid:
		// Use the general request remediation.
	case ArtifactFailureAuthority:
		remediation = "Configure an approved artifact authority and retry."
	case ArtifactFailureUnauthorized:
		remediation = "Request artifact access through an authorized adapter boundary."
	case ArtifactFailureNotFound:
		remediation = "Use a current immutable artifact reference."
	case ArtifactFailureDigest:
		remediation = "Discard the content and obtain a newly verified artifact reference."
	case ArtifactFailureExpired:
		remediation = "Produce a new artifact under an active retention policy."
	case ArtifactFailureSize:
		remediation = "Use an artifact limit large enough for the expected streamed content."
	case ArtifactFailureRetention:
		remediation = "Use run or project retention for local durable content."
	}
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     code,
		Message:  message,
		Remediation: &diagnostic.Remediation{
			Message: remediation,
		},
	}
}
