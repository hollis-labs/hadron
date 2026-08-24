package appworkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
)

const (
	DefinitionKindFile     = "file"
	DefinitionKindRegistry = "registry"
	DefinitionKindPackage  = "package"

	CodeDefinitionInvalid      diagnostic.Code = "HADR-HOST-020"
	CodeDefinitionUnauthorized diagnostic.Code = "HADR-HOST-021"
	CodeDefinitionUnresolved   diagnostic.Code = "HADR-HOST-022"
	CodeDefinitionPinConflict  diagnostic.Code = "HADR-HOST-023"
	CodeDefinitionUnsafe       diagnostic.Code = "HADR-HOST-024"
)

var (
	ErrInvalidDefinitionOptions = errors.New("invalid Hadron definition resolver options")
	ErrDefinitionUnauthorized   = errors.New("workflow definition resolution is unauthorized")
	ErrDefinitionUnresolved     = errors.New("workflow definition could not be resolved")
	ErrDefinitionPinConflict    = errors.New("workflow definition pin conflict")
	ErrUnsafeDefinitionSource   = errors.New("unsafe workflow definition source")
)

type DefinitionAuthorizationStage string

const (
	AuthorizationRequested DefinitionAuthorizationStage = "requested"
	AuthorizationResolved  DefinitionAuthorizationStage = "resolved"
	AuthorizationPlanLoad  DefinitionAuthorizationStage = "plan_load"
)

// DefinitionAuthorization carries no source bytes. Resolved is populated only
// after the source identity is known and before compilation/cache access.
type DefinitionAuthorization struct {
	Stage      DefinitionAuthorizationStage
	Requested  graph.DefinitionRef
	Resolved   *graph.DefinitionRef
	TrustClass string
}

type DefinitionAuthorizer interface {
	AuthorizeDefinition(context.Context, DefinitionAuthorization) error
}

type DefinitionAuthorizerFunc func(context.Context, DefinitionAuthorization) error

func (f DefinitionAuthorizerFunc) AuthorizeDefinition(ctx context.Context, request DefinitionAuthorization) error {
	return f(ctx, request)
}

// ResolvedSource is the exact pre-compile Hadron resolution result. Bytes and
// provenance are defensively copied by resolver boundaries. Digest always
// hashes selected source bytes, never a package/container.
type ResolvedSource struct {
	Requested  graph.DefinitionRef
	Definition graph.DefinitionRef
	Bytes      []byte
	Digest     string
	TrustClass string
	Movable    bool
}

type DefinitionCompileOptions struct {
	StepKinds         compile.StepKindLookup
	PolicyHooks       []compile.PolicyHook
	DependencyOptions compile.DependencyOptions
	MaxCallDepth      int
	// SemanticRevision is the host's stable fingerprint for collaborator
	// behavior that cannot be derived from interface values (notably policy
	// hooks and verification extractors). Hosts must change it whenever those
	// semantics change; process or pointer identity is deliberately not used.
	SemanticRevision string
}

type DefinitionResolverOptions struct {
	Roots             []string
	FileAuthority     string
	FileTrustClass    string
	PackageAuthority  string
	PackageTrustClass string
	Registry          hadronregistry.WorkflowResolver
	Authorizer        DefinitionAuthorizer
	Compile           DefinitionCompileOptions
	// MaxSourceBytes bounds file and registry bytes and the complete
	// decompressed package tar stream before compilation or caching.
	MaxSourceBytes       int64
	MaxArchiveBytes      int64
	MaxArchiveEntries    int
	MaxArchiveTotalBytes int64
}

// DefinitionDiagnosticError preserves graph-native diagnostics through host
// resolution and compile boundaries.
type DefinitionDiagnosticError struct {
	Cause    error
	Findings []diagnostic.Diagnostic
}

func (e *DefinitionDiagnosticError) Error() string {
	if e == nil {
		return "workflow definition resolution failed"
	}
	if e.Cause != nil {
		return e.Cause.Error()
	}
	if len(e.Findings) != 0 {
		return e.Findings[0].Message
	}
	return "workflow definition resolution failed"
}

func (e *DefinitionDiagnosticError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *DefinitionDiagnosticError) Diagnostics() []diagnostic.Diagnostic {
	if e == nil {
		return nil
	}
	return cloneDiagnostics(e.Findings)
}

func cloneResolvedSource(input ResolvedSource) (ResolvedSource, error) {
	requested, err := cloneDefinitionReference(input.Requested)
	if err != nil {
		return ResolvedSource{}, err
	}
	definition, err := cloneDefinitionReference(input.Definition)
	if err != nil {
		return ResolvedSource{}, err
	}
	input.Requested = requested
	input.Definition = definition
	input.Bytes = bytes.Clone(input.Bytes)
	return input, nil
}

func cloneDefinitionReference(input graph.DefinitionRef) (graph.DefinitionRef, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return graph.DefinitionRef{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var output graph.DefinitionRef
	if err := decoder.Decode(&output); err != nil {
		return graph.DefinitionRef{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return graph.DefinitionRef{}, errors.New("definition reference contains trailing JSON")
	}
	return output, nil
}

func cloneDefinitionAuthorization(input DefinitionAuthorization) (DefinitionAuthorization, error) {
	requested, err := cloneDefinitionReference(input.Requested)
	if err != nil {
		return DefinitionAuthorization{}, err
	}
	input.Requested = requested
	if input.Resolved != nil {
		resolved, err := cloneDefinitionReference(*input.Resolved)
		if err != nil {
			return DefinitionAuthorization{}, err
		}
		input.Resolved = &resolved
	}
	return input, nil
}

func definitionError(code diagnostic.Code, cause error, locator, message, remediation string) error {
	var source *graph.SourceRef
	if strings.TrimSpace(locator) != "" {
		source = &graph.SourceRef{Format: graph.SourceWorkflow, Locator: locator, StartLine: 1, StartColumn: 1}
	}
	finding := diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError, Code: code, Message: message, Source: source,
		Remediation: &diagnostic.Remediation{Message: remediation},
	}
	return &DefinitionDiagnosticError{Cause: cause, Findings: []diagnostic.Diagnostic{finding}}
}

func diagnosticsError(cause error, findings []diagnostic.Diagnostic) error {
	return &DefinitionDiagnosticError{Cause: cause, Findings: cloneDiagnostics(findings)}
}

func cloneDiagnostics(input []diagnostic.Diagnostic) []diagnostic.Diagnostic {
	if input == nil {
		return nil
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return append([]diagnostic.Diagnostic(nil), input...)
	}
	var output []diagnostic.Diagnostic
	if err := json.Unmarshal(encoded, &output); err != nil {
		return append([]diagnostic.Diagnostic(nil), input...)
	}
	return output
}

func invalidDefinitionOptions(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidDefinitionOptions, message)
}
