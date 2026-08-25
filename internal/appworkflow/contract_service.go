package appworkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/internal/pack"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

const (
	ContractSuiteSchemaVersion = "hadron.workflow-contract/v1"

	maxContractSuiteCases  = 64
	maxContractMockResults = 4096
	maxContractToolCalls   = 8192
	maxContractSuiteBytes  = 4 << 20

	maxContractReportCases         = maxContractSuiteCases
	maxContractReportCalls         = maxContractToolCalls
	maxContractReportEffects       = maxContractReportCases * 5
	maxContractReportBytes         = 4 << 20
	maxContractAttestationBytes    = 4096
	maxContractReportIdentityBytes = 256
	maxContractReportMessageBytes  = 4096
)

var (
	ErrInvalidContractService    = errors.New("invalid workflow contract-test service")
	ErrContractTestFailed        = errors.New("workflow contract tests failed")
	ErrNamespaceUnauthorized     = errors.New("workflow namespace operation is unauthorized")
	ErrContractRunnerUnavailable = errors.New("canonical workflow contract runner is unavailable")
)

// ContractToolCall is a literal, deterministic external-call observation
// emitted by a controlled executor mock. Arguments must contain only native
// JSON values and never resolved credentials.
type ContractToolCall struct {
	NodeID    string                       `json:"node_id"`
	Kind      string                       `json:"kind"`
	Name      string                       `json:"name"`
	Arguments map[string]any               `json:"arguments"`
	Effect    graph.Effect                 `json:"effect"`
	Outcome   verification.ActivityOutcome `json:"outcome"`
}

// ContractMockResult is one deterministic invocation result. Iteration and
// Attempt select the exact ordinary runtime invocation; zero Attempt is
// canonicalized to one for compact single-attempt suites. ExpectedInputs, when
// present, overrides the mock-level expectation for fan-out items and retries.
type ContractMockResult struct {
	Iteration      string                   `json:"iteration,omitempty"`
	Attempt        int                      `json:"attempt,omitempty"`
	ExpectedInputs *values.ValueSet         `json:"expected_inputs,omitempty"`
	Outputs        values.ValueSet          `json:"outputs"`
	Failure        *stepkind.ExecutionError `json:"failure,omitempty"`
	Calls          []ContractToolCall       `json:"calls,omitempty"`
}

// MarshalJSON preserves the meaningful distinction between a successful
// empty output set ({}) and an execution failure with no outputs (null).
// values.ValueSet deliberately rejects marshaling a nil map on its own.
func (r ContractMockResult) MarshalJSON() ([]byte, error) {
	type wire struct {
		Iteration      string                   `json:"iteration,omitempty"`
		Attempt        int                      `json:"attempt,omitempty"`
		ExpectedInputs *values.ValueSet         `json:"expected_inputs,omitempty"`
		Outputs        *values.ValueSet         `json:"outputs"`
		Failure        *stepkind.ExecutionError `json:"failure,omitempty"`
		Calls          []ContractToolCall       `json:"calls,omitempty"`
	}
	var outputs *values.ValueSet
	if r.Outputs != nil {
		current := r.Outputs
		outputs = &current
	}
	return json.Marshal(wire{
		Iteration: r.Iteration, Attempt: r.Attempt, ExpectedInputs: r.ExpectedInputs,
		Outputs: outputs, Failure: r.Failure, Calls: r.Calls,
	})
}

func (r *ContractMockResult) UnmarshalJSON(data []byte) error {
	type wire struct {
		Iteration      string                   `json:"iteration,omitempty"`
		Attempt        int                      `json:"attempt,omitempty"`
		ExpectedInputs *values.ValueSet         `json:"expected_inputs,omitempty"`
		Outputs        *values.ValueSet         `json:"outputs"`
		Failure        *stepkind.ExecutionError `json:"failure,omitempty"`
		Calls          []ContractToolCall       `json:"calls,omitempty"`
	}
	var decoded wire
	if err := decodeContractJSON(data, &decoded); err != nil {
		return err
	}
	*r = ContractMockResult{
		Iteration: decoded.Iteration, Attempt: decoded.Attempt, ExpectedInputs: decoded.ExpectedInputs,
		Failure: decoded.Failure, Calls: decoded.Calls,
	}
	if decoded.Outputs != nil {
		r.Outputs = *decoded.Outputs
	}
	return nil
}

// ContractExecutorMock controls one exact graph node. ExpectedConfig and
// ExpectedInputs are compared before the ordinary dispatcher receives the
// result, proving production lowering and binding paths were used.
type ContractExecutorMock struct {
	NodeID         string          `json:"node_id"`
	Kind           string          `json:"kind"`
	KindVersion    string          `json:"kind_version"`
	ConfigSchema   graph.Schema    `json:"config_schema"`
	InputSchema    graph.Schema    `json:"input_schema"`
	OutputSchema   graph.Schema    `json:"output_schema"`
	ExpectedConfig graph.Config    `json:"expected_config"`
	ExpectedInputs values.ValueSet `json:"expected_inputs"`
	// ExpectedInputsEditable marks a generated placeholder which cannot be
	// qualified until an author supplies the exact dependent-node inputs.
	ExpectedInputsEditable bool                 `json:"expected_inputs_editable,omitempty"`
	Results                []ContractMockResult `json:"results"`
}

// ContractExpectedError identifies a safe typed execution failure.
type ContractExpectedError struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// WorkflowContractCase supplies typed run inputs and expected typed outputs or
// one expected terminal executor error.
type WorkflowContractCase struct {
	Name string `json:"name"`
	// Editable is set on generated scaffolds. Editable cases are useful files,
	// but cannot be executed, attested, registered, or packaged as evidence.
	Editable        bool                   `json:"editable,omitempty"`
	Inputs          values.ValueSet        `json:"inputs"`
	ExpectedOutputs values.ValueSet        `json:"expected_outputs"`
	ExpectedError   *ContractExpectedError `json:"expected_error,omitempty"`
	ExpectedEffects graph.EffectSet        `json:"expected_effects"`
	ExpectedCalls   []ContractToolCall     `json:"expected_calls,omitempty"`
	Mocks           []ContractExecutorMock `json:"mocks"`
}

// MarshalJSON preserves the expected_outputs/expected_error exclusive choice,
// including a successful workflow whose declared output set is empty.
func (c WorkflowContractCase) MarshalJSON() ([]byte, error) {
	type wire struct {
		Name            string                 `json:"name"`
		Editable        bool                   `json:"editable,omitempty"`
		Inputs          values.ValueSet        `json:"inputs"`
		ExpectedOutputs *values.ValueSet       `json:"expected_outputs"`
		ExpectedError   *ContractExpectedError `json:"expected_error,omitempty"`
		ExpectedEffects graph.EffectSet        `json:"expected_effects"`
		ExpectedCalls   []ContractToolCall     `json:"expected_calls,omitempty"`
		Mocks           []ContractExecutorMock `json:"mocks"`
	}
	var outputs *values.ValueSet
	if c.ExpectedOutputs != nil {
		current := c.ExpectedOutputs
		outputs = &current
	}
	return json.Marshal(wire{
		Name: c.Name, Editable: c.Editable, Inputs: c.Inputs, ExpectedOutputs: outputs,
		ExpectedError: c.ExpectedError, ExpectedEffects: c.ExpectedEffects,
		ExpectedCalls: c.ExpectedCalls, Mocks: c.Mocks,
	})
}

func (c *WorkflowContractCase) UnmarshalJSON(data []byte) error {
	type wire struct {
		Name            string                 `json:"name"`
		Editable        bool                   `json:"editable,omitempty"`
		Inputs          values.ValueSet        `json:"inputs"`
		ExpectedOutputs *values.ValueSet       `json:"expected_outputs"`
		ExpectedError   *ContractExpectedError `json:"expected_error,omitempty"`
		ExpectedEffects graph.EffectSet        `json:"expected_effects"`
		ExpectedCalls   []ContractToolCall     `json:"expected_calls,omitempty"`
		Mocks           []ContractExecutorMock `json:"mocks"`
	}
	var decoded wire
	if err := decodeContractJSON(data, &decoded); err != nil {
		return err
	}
	*c = WorkflowContractCase{
		Name: decoded.Name, Editable: decoded.Editable, Inputs: decoded.Inputs,
		ExpectedError: decoded.ExpectedError, ExpectedEffects: decoded.ExpectedEffects,
		ExpectedCalls: decoded.ExpectedCalls, Mocks: decoded.Mocks,
	}
	if decoded.ExpectedOutputs != nil {
		c.ExpectedOutputs = *decoded.ExpectedOutputs
	}
	return nil
}

// WorkflowContractSuite is an editable, serializable definition-level test
// contract. It deliberately carries no host store or worker implementation.
type WorkflowContractSuite struct {
	SchemaVersion string                 `json:"schema_version"`
	Cases         []WorkflowContractCase `json:"cases"`
}

// ContractCaseResult is the deterministic public result for one case.
type ContractCaseResult struct {
	Name         string                 `json:"name"`
	Passed       bool                   `json:"passed"`
	OutputDigest string                 `json:"output_digest,omitempty"`
	Failure      *ContractExpectedError `json:"failure,omitempty"`
	Effects      graph.EffectSet        `json:"effects"`
	Calls        []ContractToolCall     `json:"calls,omitempty"`
	Message      string                 `json:"message,omitempty"`
}

// ContractTestReport binds qualification evidence to an exact compiled plan
// and canonical suite. Digest excludes no fields and is stable across process
// restarts because case execution uses fixed logical identities and clocks.
type ContractTestReport struct {
	SchemaVersion string               `json:"schema_version"`
	PlanDigest    string               `json:"plan_digest"`
	SuiteDigest   string               `json:"suite_digest"`
	Passed        bool                 `json:"passed"`
	Cases         []ContractCaseResult `json:"cases"`
	Digest        string               `json:"digest"`
	Attestation   string               `json:"attestation"`
}

// ContractValidation is returned without mutation. Diagnostics retain exact
// compiler source locations.
type ContractValidation struct {
	Plan        *compile.ExecutionPlan  `json:"plan,omitempty"`
	Diagnostics []diagnostic.Diagnostic `json:"diagnostics,omitempty"`
}

type contractDefinitionResolver interface {
	DefinitionProvider
	ResolveSource(context.Context, graph.DefinitionRef) (ResolvedSource, error)
}

// NamespaceOperation is the closed service-level authorization vocabulary.
type NamespaceOperation string

const (
	NamespaceRegister NamespaceOperation = "register"
	NamespacePin      NamespaceOperation = "pin"
	NamespaceUnpin    NamespaceOperation = "unpin"
	NamespacePublish  NamespaceOperation = "publish"
	NamespaceInspect  NamespaceOperation = "inspect"
	NamespaceSearch   NamespaceOperation = "search"
	NamespaceResolve  NamespaceOperation = "resolve"
	NamespacePackage  NamespaceOperation = "package"
)

// NamespaceAuthorizationStage distinguishes fail-closed authorization of the
// requested namespace from authorization of the exact resolved definition.
type NamespaceAuthorizationStage string

const (
	NamespaceAuthorizationRequested NamespaceAuthorizationStage = "requested"
	NamespaceAuthorizationResolved  NamespaceAuthorizationStage = "resolved"
)

// NamespaceAuthorization binds one current principal to one explicit
// namespace. Requested authorization carries no resolved authority or digest;
// resolved authorization carries both. Current aliases and registry metadata
// never transfer ownership.
type NamespaceAuthorization struct {
	Stage     NamespaceAuthorizationStage
	Operation NamespaceOperation
	Namespace string
	Name      string
	Principal string
	Authority string
	Digest    string
}

type NamespaceAuthorizer interface {
	AuthorizeNamespace(context.Context, NamespaceAuthorization) error
}

type NamespaceAuthorizerFunc func(context.Context, NamespaceAuthorization) error

func (f NamespaceAuthorizerFunc) AuthorizeNamespace(ctx context.Context, request NamespaceAuthorization) error {
	return f(ctx, request)
}

// ContractTestPolicy is frozen when the service is built.
type ContractTestPolicy struct {
	MinimumCases          int
	Repetitions           int
	RequireEffectCoverage bool
}

// ContractRunner is the narrow integration seam for the canonical host driver.
// Implementations must isolate every repetition and execute ordinary plan
// binding, progression, dispatch, and finalization without production state.
type ContractRunner interface {
	Execute(context.Context, *compile.ExecutionPlan, stepkind.Registry, WorkflowContractSuite, int) (ContractTestReport, error)
}

// ContractReportAttestor prevents callers from forging a passing report DTO.
// Implementations are Hadron-owned and must remain stable across restarts.
type ContractReportAttestor interface {
	AttestContractReport(context.Context, string) (string, error)
	VerifyContractReport(context.Context, string, string) error
}

// WorkflowContractCatalog is the durable graph-native catalog port used by the
// service. Implementations must preserve the WorkflowIndex exact replay,
// defensive-copy, and immutable record semantics.
type WorkflowContractCatalog interface {
	RegisterWorkflow(context.Context, hadronregistry.WorkflowRecord, bool) (hadronregistry.WorkflowRecord, error)
	PinWorkflow(context.Context, hadronregistry.WorkflowQuery) (hadronregistry.WorkflowRecord, error)
	UnpinWorkflowExact(context.Context, hadronregistry.WorkflowQuery) error
	ResolvePinnedWorkflow(context.Context, string) (hadronregistry.WorkflowResolution, error)
	PublishWorkflow(context.Context, hadronregistry.WorkflowQuery) (hadronregistry.WorkflowRecord, error)
	InspectWorkflow(context.Context, hadronregistry.WorkflowQuery) (hadronregistry.WorkflowRecord, error)
	SearchWorkflows(context.Context, string, string) ([]hadronregistry.WorkflowRecord, error)
	ResolveWorkflow(context.Context, hadronregistry.WorkflowQuery) (hadronregistry.WorkflowResolution, error)
}

type ContractRegistrationOptions struct {
	Definitions contractDefinitionResolver
	StepKinds   stepkind.Registry
	Catalog     WorkflowContractCatalog
	Authorizer  NamespaceAuthorizer
	Policy      ContractTestPolicy
	Runner      ContractRunner
	Attestor    ContractReportAttestor
	Now         func() time.Time
}

// ContractRegistrationService is Hadron's sole qualification and immutable
// registration boundary. It never writes project source or production run
// state; contract execution always receives a newly allocated private store.
type ContractRegistrationService struct {
	definitions contractDefinitionResolver
	kinds       *stepkind.MemoryRegistry
	catalog     WorkflowContractCatalog
	authorizer  NamespaceAuthorizer
	policy      ContractTestPolicy
	runner      ContractRunner
	attestor    ContractReportAttestor
	now         func() time.Time

	registrationMu     sync.Mutex
	lastRegistrationAt time.Time
}

func NewContractRegistrationService(options ContractRegistrationOptions) (*ContractRegistrationService, error) {
	if nilInterface(options.Definitions) || nilInterface(options.StepKinds) || nilInterface(options.Catalog) ||
		nilInterface(options.Authorizer) || nilInterface(options.Attestor) {
		return nil, fmt.Errorf("%w: definitions, step kinds, catalog, authorizer, and report attestor are required", ErrInvalidContractService)
	}
	kinds, err := snapshotStepKinds(options.StepKinds)
	if err != nil {
		return nil, fmt.Errorf("%w: freeze step-kind registry: %w", ErrInvalidContractService, err)
	}
	verifiers, err := freezeHostVerifiers(options.Definitions, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: freeze verifier registry: %w", ErrInvalidContractService, err)
	}
	policy := options.Policy
	if policy.MinimumCases < 0 || policy.MinimumCases > maxContractSuiteCases || policy.Repetitions < 0 || policy.Repetitions > 10 {
		return nil, fmt.Errorf("%w: contract-test policy is invalid", ErrInvalidContractService)
	}
	if policy.Repetitions == 0 {
		policy.Repetitions = 2
	}
	if policy.MinimumCases == 0 {
		policy.MinimumCases = 1
	}
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	runner := options.Runner
	if runner != nil && nilInterface(runner) {
		return nil, fmt.Errorf("%w: contract runner must not be typed nil", ErrInvalidContractService)
	}
	if runner == nil {
		var exactVerifiers verification.Registry
		if _, ok := options.Definitions.(definitionVerifierCatalog); ok {
			exactVerifiers = verifiers
		}
		runner = newCanonicalContractRunner(compileDependencyOptions(options.Definitions), exactVerifiers)
	}
	return &ContractRegistrationService{
		definitions: options.Definitions, kinds: kinds, catalog: options.Catalog,
		authorizer: options.Authorizer, policy: policy, runner: runner, attestor: options.Attestor, now: now,
	}, nil
}

func (s *ContractRegistrationService) Validate(ctx context.Context, ref graph.DefinitionRef) (ContractValidation, error) {
	if err := s.ready(ctx); err != nil {
		return ContractValidation{}, err
	}
	plan, err := s.definitions.ResolvePlan(ctx, ref)
	if err == nil {
		return ContractValidation{Plan: plan}, nil
	}
	var diagnosticErr interface {
		Diagnostics() []diagnostic.Diagnostic
	}
	if errors.As(err, &diagnosticErr) {
		return ContractValidation{Diagnostics: diagnosticErr.Diagnostics()}, nil
	}
	return ContractValidation{}, err
}

// GenerateContractScaffold deterministically derives an editable suite from
// workflow I/O, declared effects, and the frozen registered step schemas.
func (s *ContractRegistrationService) GenerateContractScaffold(ctx context.Context, ref graph.DefinitionRef) (WorkflowContractSuite, error) {
	validation, err := s.Validate(ctx, ref)
	if err != nil {
		return WorkflowContractSuite{}, err
	}
	if validation.Plan == nil {
		return WorkflowContractSuite{}, fmt.Errorf("%w: definition validation failed", ErrInvalidContractService)
	}
	return scaffoldFor(validation.Plan, s.kinds)
}

// ExecuteContractTests delegates to the canonical isolated runtime driver.
// Tests may inject an alternate runner to exercise report validation, but the
// default service never touches the production Host or StateStore.
func (s *ContractRegistrationService) ExecuteContractTests(ctx context.Context, ref graph.DefinitionRef, suite WorkflowContractSuite) (ContractTestReport, error) {
	validation, err := s.Validate(ctx, ref)
	if err != nil {
		return ContractTestReport{}, err
	}
	if validation.Plan == nil {
		return ContractTestReport{}, fmt.Errorf("%w: definition validation failed", ErrContractTestFailed)
	}
	canonicalSuite, suiteJSON, err := canonicalContractSuite(suite)
	if err != nil {
		return ContractTestReport{}, err
	}
	if validationErr := validateContractSuiteForPlan(validation.Plan, s.kinds, canonicalSuite, s.policy.RequireEffectCoverage); validationErr != nil {
		return ContractTestReport{}, validationErr
	}
	if nilInterface(s.runner) {
		return ContractTestReport{}, ErrContractRunnerUnavailable
	}
	report, err := s.runner.Execute(ctx, validation.Plan, s.kinds, canonicalSuite, s.policy.Repetitions)
	if err != nil {
		return ContractTestReport{}, err
	}
	report.SchemaVersion = ContractSuiteSchemaVersion
	report.PlanDigest = validation.Plan.Digest
	report.SuiteDigest = values.SHA256Digest(suiteJSON)
	if admissionErr := validateContractReportAdmission(report); admissionErr != nil {
		return ContractTestReport{}, admissionErr
	}
	if err := validateContractTestReport(canonicalSuite, report); err != nil {
		return ContractTestReport{}, err
	}
	return s.sealReport(ctx, report)
}

type RegisterWorkflowRequest struct {
	Definition  graph.DefinitionRef
	Namespace   string
	Principal   string
	Report      ContractTestReport
	MakeCurrent bool
}

func (s *ContractRegistrationService) Register(ctx context.Context, request RegisterWorkflowRequest) (hadronregistry.WorkflowRecord, error) {
	if err := s.ready(ctx); err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	if strings.TrimSpace(request.Definition.Version) == "" || strings.TrimSpace(request.Definition.Digest) == "" {
		return hadronregistry.WorkflowRecord{}, fmt.Errorf("%w: registration requires an immutable version and source digest", ErrInvalidContractService)
	}
	if err := graph.ValidateID(request.Definition.ID); err != nil {
		return hadronregistry.WorkflowRecord{}, fmt.Errorf("%w: registration workflow ID: %w", ErrInvalidContractService, err)
	}
	namespace, name, principal, err := canonicalRequestedIdentity(request.Namespace, strings.TrimSpace(request.Namespace)+"/"+request.Definition.ID, request.Principal)
	if err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	if authorizationErr := s.authorizeRequested(ctx, NamespaceRegister, namespace, name, principal); authorizationErr != nil {
		return hadronregistry.WorkflowRecord{}, authorizationErr
	}
	if admissionErr := validateContractReportAdmission(request.Report); admissionErr != nil {
		return hadronregistry.WorkflowRecord{}, admissionErr
	}
	source, err := s.definitions.ResolveSource(ctx, request.Definition)
	if err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	if authorizationErr := s.authorizeResolved(ctx, NamespaceRegister, namespace, name, principal, source.Definition.Authority, source.Digest); authorizationErr != nil {
		return hadronregistry.WorkflowRecord{}, authorizationErr
	}
	if source.Movable || source.Digest != request.Definition.Digest || source.Definition.Version != request.Definition.Version {
		return hadronregistry.WorkflowRecord{}, fmt.Errorf("%w: resolved source is not the requested immutable version", ErrInvalidContractService)
	}
	validation, err := s.Validate(ctx, request.Definition)
	if err != nil || validation.Plan == nil {
		if err != nil {
			return hadronregistry.WorkflowRecord{}, err
		}
		return hadronregistry.WorkflowRecord{}, fmt.Errorf("%w: definition validation failed", ErrInvalidContractService)
	}
	if validation.Plan.Graph.ID != request.Definition.ID {
		return hadronregistry.WorkflowRecord{}, fmt.Errorf("%w: resolved workflow ID differs from the requested identity", ErrInvalidContractService)
	}
	if reportErr := s.requireReport(ctx, validation.Plan, request.Report); reportErr != nil {
		return hadronregistry.WorkflowRecord{}, reportErr
	}
	provenance := validation.Plan.Provenance
	if source.Definition.Provenance != nil {
		provenance = *source.Definition.Provenance
	}
	record := hadronregistry.WorkflowRecord{
		Name: name, Namespace: namespace, Version: source.Definition.Version,
		Digest: source.Digest, Source: source.Bytes, Authority: source.Definition.Authority,
		SourceFormat: source.SourceFormat, SourceSchemaID: source.SourceSchemaID, SourceSchemaVersion: source.SourceSchemaVersion,
		TrustClass: source.TrustClass, Provenance: provenance, PlanDigest: validation.Plan.Digest,
		ContractSuiteDigest: request.Report.SuiteDigest, ContractTestDigest: request.Report.Digest, TestsPassed: request.Report.Passed,
		PublisherPrincipal: principal,
	}
	return s.registerWorkflow(ctx, record, request.MakeCurrent)
}

func (s *ContractRegistrationService) Pin(ctx context.Context, query hadronregistry.WorkflowQuery, principal string) (hadronregistry.WorkflowRecord, error) {
	if err := s.ready(ctx); err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	namespace, name, principal, err := canonicalRequestedIdentity("", query.Name, principal)
	if err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	query.Name = name
	if authorizationErr := s.authorizeRequested(ctx, NamespacePin, namespace, name, principal); authorizationErr != nil {
		return hadronregistry.WorkflowRecord{}, authorizationErr
	}
	record, err := s.catalog.InspectWorkflow(ctx, query)
	if err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	if authorizationErr := s.authorizeRecord(ctx, NamespacePin, namespace, name, record, principal); authorizationErr != nil {
		return hadronregistry.WorkflowRecord{}, authorizationErr
	}
	exact := hadronregistry.WorkflowQuery{Name: name, Version: record.Version, Digest: record.Digest}
	return s.catalog.PinWorkflow(ctx, exact)
}

func (s *ContractRegistrationService) Unpin(ctx context.Context, name, principal string) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	namespace, name, principal, err := canonicalRequestedIdentity("", name, principal)
	if err != nil {
		return err
	}
	if authorizationErr := s.authorizeRequested(ctx, NamespaceUnpin, namespace, name, principal); authorizationErr != nil {
		return authorizationErr
	}
	record, err := s.catalog.ResolvePinnedWorkflow(ctx, name)
	if err != nil {
		return err
	}
	if authorizationErr := s.authorizeRecord(ctx, NamespaceUnpin, namespace, name, record.Record, principal); authorizationErr != nil {
		return authorizationErr
	}
	return s.catalog.UnpinWorkflowExact(ctx, hadronregistry.WorkflowQuery{Name: name, Version: record.Record.Version, Digest: record.Record.Digest})
}

func (s *ContractRegistrationService) Publish(ctx context.Context, query hadronregistry.WorkflowQuery, principal string) (hadronregistry.WorkflowRecord, error) {
	if err := s.ready(ctx); err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	namespace, name, principal, err := canonicalRequestedIdentity("", query.Name, principal)
	if err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	query.Name = name
	if authorizationErr := s.authorizeRequested(ctx, NamespacePublish, namespace, name, principal); authorizationErr != nil {
		return hadronregistry.WorkflowRecord{}, authorizationErr
	}
	record, err := s.catalog.InspectWorkflow(ctx, query)
	if err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	if authorizationErr := s.authorizeRecord(ctx, NamespacePublish, namespace, name, record, principal); authorizationErr != nil {
		return hadronregistry.WorkflowRecord{}, authorizationErr
	}
	exact := hadronregistry.WorkflowQuery{Name: name, Version: record.Version, Digest: record.Digest}
	return s.catalog.PublishWorkflow(ctx, exact)
}

func (s *ContractRegistrationService) Inspect(ctx context.Context, query hadronregistry.WorkflowQuery, principal string) (hadronregistry.WorkflowRecord, error) {
	if err := s.ready(ctx); err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	namespace, name, principal, err := canonicalRequestedIdentity("", query.Name, principal)
	if err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	query.Name = name
	if authorizationErr := s.authorizeRequested(ctx, NamespaceInspect, namespace, name, principal); authorizationErr != nil {
		return hadronregistry.WorkflowRecord{}, authorizationErr
	}
	record, err := s.catalog.InspectWorkflow(ctx, query)
	if err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	if authorizationErr := s.authorizeRecord(ctx, NamespaceInspect, namespace, name, record, principal); authorizationErr != nil {
		return hadronregistry.WorkflowRecord{}, authorizationErr
	}
	return record, nil
}

func (s *ContractRegistrationService) Search(ctx context.Context, namespace, text, principal string) ([]hadronregistry.WorkflowRecord, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	namespace, principal, err := canonicalRequestedNamespace(namespace, principal)
	if err != nil {
		return nil, err
	}
	if authorizationErr := s.authorizeRequested(ctx, NamespaceSearch, namespace, "", principal); authorizationErr != nil {
		return nil, authorizationErr
	}
	records, err := s.catalog.SearchWorkflows(ctx, namespace, text)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if authorizationErr := s.authorizeRecord(ctx, NamespaceSearch, namespace, "", record, principal); authorizationErr != nil {
			return nil, authorizationErr
		}
	}
	return records, nil
}

func (s *ContractRegistrationService) Resolve(ctx context.Context, query hadronregistry.WorkflowQuery, principal string) (hadronregistry.WorkflowResolution, error) {
	if err := s.ready(ctx); err != nil {
		return hadronregistry.WorkflowResolution{}, err
	}
	namespace, name, principal, err := canonicalRequestedIdentity("", query.Name, principal)
	if err != nil {
		return hadronregistry.WorkflowResolution{}, err
	}
	query.Name = name
	if authorizationErr := s.authorizeRequested(ctx, NamespaceResolve, namespace, name, principal); authorizationErr != nil {
		return hadronregistry.WorkflowResolution{}, authorizationErr
	}
	resolution, err := s.catalog.ResolveWorkflow(ctx, query)
	if err != nil {
		return hadronregistry.WorkflowResolution{}, err
	}
	if authorizationErr := s.authorizeRecord(ctx, NamespaceResolve, namespace, name, resolution.Record, principal); authorizationErr != nil {
		return hadronregistry.WorkflowResolution{}, authorizationErr
	}
	return resolution, nil
}

func (s *ContractRegistrationService) Package(ctx context.Context, query hadronregistry.WorkflowQuery, suite WorkflowContractSuite, report ContractTestReport, principal string) (pack.WorkflowPackage, error) {
	if err := s.ready(ctx); err != nil {
		return pack.WorkflowPackage{}, err
	}
	namespace, name, principal, err := canonicalRequestedIdentity("", query.Name, principal)
	if err != nil {
		return pack.WorkflowPackage{}, err
	}
	query.Name = name
	if authorizationErr := s.authorizeRequested(ctx, NamespacePackage, namespace, name, principal); authorizationErr != nil {
		return pack.WorkflowPackage{}, authorizationErr
	}
	if admissionErr := validateContractReportAdmission(report); admissionErr != nil {
		return pack.WorkflowPackage{}, admissionErr
	}
	record, err := s.catalog.InspectWorkflow(ctx, query)
	if err != nil {
		return pack.WorkflowPackage{}, err
	}
	if authorizationErr := s.authorizeRecord(ctx, NamespacePackage, namespace, name, record, principal); authorizationErr != nil {
		return pack.WorkflowPackage{}, authorizationErr
	}
	_, suiteJSON, err := canonicalContractSuite(suite)
	if err != nil {
		return pack.WorkflowPackage{}, err
	}
	if !record.TestsPassed || record.ContractSuiteDigest != values.SHA256Digest(suiteJSON) {
		return pack.WorkflowPackage{}, fmt.Errorf("%w: package requires the registered passing test result", ErrContractTestFailed)
	}
	if reportErr := s.verifyReport(ctx, record.PlanDigest, record.ContractSuiteDigest, record.ContractTestDigest, report); reportErr != nil {
		return pack.WorkflowPackage{}, reportErr
	}
	reportJSON, err := canonicalJSON(report)
	if err != nil {
		return pack.WorkflowPackage{}, err
	}
	return pack.BuildWorkflowPackage(pack.WorkflowPackageRequest{
		Source: record.Source, Suite: suiteJSON, Report: reportJSON,
		Manifest: pack.WorkflowPackageManifest{
			Name: record.Name, Namespace: record.Namespace, Version: record.Version,
			SourceDigest: record.Digest, PlanDigest: record.PlanDigest,
			ContractSuiteDigest: record.ContractSuiteDigest, ContractTestDigest: record.ContractTestDigest, Authority: record.Authority,
			TrustClass: record.TrustClass, PublisherPrincipal: record.PublisherPrincipal,
			RegisteredAt: record.RegisteredAt, Provenance: record.Provenance,
		},
	})
}

func canonicalContractSuite(input WorkflowContractSuite) (WorkflowContractSuite, []byte, error) {
	if len(input.Cases) == 0 {
		return WorkflowContractSuite{}, nil, fmt.Errorf("%w: contract suite schema and cases are required", ErrInvalidContractService)
	}
	if err := validateContractSuiteAdmission(input); err != nil {
		return WorkflowContractSuite{}, nil, err
	}
	ownedJSON, cloneErr := canonicalJSON(input)
	if cloneErr != nil {
		return WorkflowContractSuite{}, nil, fmt.Errorf("%w: contract suite is not canonical JSON", ErrInvalidContractService)
	}
	if admissionErr := validateContractSuiteBytes(ownedJSON); admissionErr != nil {
		return WorkflowContractSuite{}, nil, admissionErr
	}
	var owned WorkflowContractSuite
	decoder := json.NewDecoder(bytes.NewReader(ownedJSON))
	decoder.UseNumber()
	if decodeErr := decoder.Decode(&owned); decodeErr != nil {
		return WorkflowContractSuite{}, nil, fmt.Errorf("%w: clone canonical contract suite", ErrInvalidContractService)
	}
	input = owned
	input.SchemaVersion = strings.TrimSpace(input.SchemaVersion)
	if input.SchemaVersion != ContractSuiteSchemaVersion {
		return WorkflowContractSuite{}, nil, fmt.Errorf("%w: contract suite schema and cases are required", ErrInvalidContractService)
	}
	seen := make(map[string]struct{}, len(input.Cases))
	for index := range input.Cases {
		name := strings.TrimSpace(input.Cases[index].Name)
		if name == "" {
			return WorkflowContractSuite{}, nil, fmt.Errorf("%w: contract case name is required", ErrInvalidContractService)
		}
		if _, duplicate := seen[name]; duplicate {
			return WorkflowContractSuite{}, nil, fmt.Errorf("%w: duplicate contract case %q", ErrInvalidContractService, name)
		}
		seen[name] = struct{}{}
		input.Cases[index].Name = name
		current := &input.Cases[index]
		if current.Editable {
			return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s is still an editable scaffold", ErrInvalidContractService, name)
		}
		if err := current.Inputs.Validate(); err != nil {
			return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s inputs: %w", ErrInvalidContractService, name, err)
		}
		hasOutputs := current.ExpectedOutputs != nil
		hasError := current.ExpectedError != nil
		if hasOutputs == hasError {
			return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s requires exactly one of expected_outputs or expected_error", ErrInvalidContractService, name)
		}
		if hasOutputs {
			if err := current.ExpectedOutputs.Validate(); err != nil {
				return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s outputs: %w", ErrInvalidContractService, name, err)
			}
		} else if err := validateExpectedContractError(*current.ExpectedError); err != nil {
			return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s expected error: %w", ErrInvalidContractService, name, err)
		}

		var effectErr error
		current.ExpectedEffects, effectErr = canonicalEffects(current.ExpectedEffects)
		if effectErr != nil {
			return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s effects: %w", ErrInvalidContractService, name, effectErr)
		}
		mockNodes := make(map[string]struct{}, len(current.Mocks))
		for mockIndex := range current.Mocks {
			mock := &current.Mocks[mockIndex]
			if err := graph.ValidateID(mock.NodeID); err != nil {
				return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s mock node: %w", ErrInvalidContractService, name, err)
			}
			if _, duplicate := mockNodes[mock.NodeID]; duplicate {
				return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s has duplicate mock node %q", ErrInvalidContractService, name, mock.NodeID)
			}
			mockNodes[mock.NodeID] = struct{}{}
			if err := validateContractIdentity("mock kind", mock.Kind); err != nil {
				return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s: %w", ErrInvalidContractService, name, mock.NodeID, err)
			}
			if err := validateContractIdentity("mock kind version", mock.KindVersion); err != nil {
				return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s: %w", ErrInvalidContractService, name, mock.NodeID, err)
			}
			if mock.ConfigSchema == nil || mock.InputSchema == nil || mock.OutputSchema == nil || mock.ExpectedConfig == nil {
				return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s requires exact schemas and config", ErrInvalidContractService, name, mock.NodeID)
			}
			for _, field := range []struct {
				name   string
				schema graph.Schema
			}{
				{name: "config", schema: mock.ConfigSchema},
				{name: "input", schema: mock.InputSchema},
				{name: "output", schema: mock.OutputSchema},
			} {
				if err := values.ValidateSchema(field.schema); err != nil {
					return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s has invalid %s schema: %w", ErrInvalidContractService, name, mock.NodeID, field.name, err)
				}
			}
			if _, err := canonicalJSON(mock.ExpectedConfig); err != nil {
				return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s has invalid config JSON: %w", ErrInvalidContractService, name, mock.NodeID, err)
			}
			if mock.ExpectedInputsEditable {
				return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s inputs are still editable", ErrInvalidContractService, name, mock.NodeID)
			}
			if err := mock.ExpectedInputs.Validate(); err != nil {
				return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s inputs: %w", ErrInvalidContractService, name, mock.NodeID, err)
			}
			if len(mock.Results) == 0 {
				return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s requires deterministic results", ErrInvalidContractService, name, mock.NodeID)
			}
			seenResults := make(map[string]struct{}, len(mock.Results))
			for resultIndex := range mock.Results {
				result := &mock.Results[resultIndex]
				if result.Attempt == 0 {
					result.Attempt = 1
				}
				if result.Attempt < 1 {
					return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s result attempt must be positive", ErrInvalidContractService, name, mock.NodeID)
				}
				identity := fmt.Sprintf("%d:%s", len(result.Iteration), result.Iteration) + fmt.Sprintf(":%d", result.Attempt)
				if _, duplicate := seenResults[identity]; duplicate {
					return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s has duplicate result identity", ErrInvalidContractService, name, mock.NodeID)
				}
				seenResults[identity] = struct{}{}
				if result.ExpectedInputs != nil {
					if err := result.ExpectedInputs.Validate(); err != nil {
						return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s result inputs: %w", ErrInvalidContractService, name, mock.NodeID, err)
					}
				}
				if (result.Failure == nil) == (result.Outputs == nil) {
					return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s result requires exactly one of outputs or failure", ErrInvalidContractService, name, mock.NodeID)
				}
				if result.Outputs != nil {
					if err := result.Outputs.Validate(); err != nil {
						return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s result outputs: %w", ErrInvalidContractService, name, mock.NodeID, err)
					}
				} else if result.Failure.Cause != nil {
					return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s result failure cannot carry a process-local cause", ErrInvalidContractService, name, mock.NodeID)
				} else if err := result.Failure.Validate(); err != nil {
					return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s result failure: %w", ErrInvalidContractService, name, mock.NodeID, err)
				}
				for callIndex := range result.Calls {
					if err := validateContractToolCall(result.Calls[callIndex], mockNodes, current.ExpectedEffects, mock.NodeID); err != nil {
						return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s node %s result call: %w", ErrInvalidContractService, name, mock.NodeID, err)
					}
				}
			}
			sort.Slice(mock.Results, func(i, j int) bool {
				if mock.Results[i].Iteration == mock.Results[j].Iteration {
					return mock.Results[i].Attempt < mock.Results[j].Attempt
				}
				return mock.Results[i].Iteration < mock.Results[j].Iteration
			})
		}
		sort.Slice(current.Mocks, func(i, j int) bool { return current.Mocks[i].NodeID < current.Mocks[j].NodeID })
		for callIndex := range current.ExpectedCalls {
			if err := validateContractToolCall(current.ExpectedCalls[callIndex], mockNodes, current.ExpectedEffects, ""); err != nil {
				return WorkflowContractSuite{}, nil, fmt.Errorf("%w: case %s expected call: %w", ErrInvalidContractService, name, err)
			}
		}
	}
	encoded, err := canonicalJSON(input)
	if err != nil {
		return WorkflowContractSuite{}, nil, err
	}
	if err := validateContractSuiteBytes(encoded); err != nil {
		return WorkflowContractSuite{}, nil, err
	}
	return input, encoded, nil
}

func validateContractSuiteAdmission(input WorkflowContractSuite) error {
	if len(input.Cases) > maxContractSuiteCases {
		return fmt.Errorf("%w: contract suite exceeds %d cases", ErrInvalidContractService, maxContractSuiteCases)
	}
	results, calls := 0, 0
	for caseIndex := range input.Cases {
		current := &input.Cases[caseIndex]
		if len(current.ExpectedCalls) > maxContractToolCalls-calls {
			return fmt.Errorf("%w: contract suite exceeds %d tool calls", ErrInvalidContractService, maxContractToolCalls)
		}
		calls += len(current.ExpectedCalls)
		for mockIndex := range current.Mocks {
			mock := &current.Mocks[mockIndex]
			if len(mock.Results) > maxContractMockResults-results {
				return fmt.Errorf("%w: contract suite exceeds %d mock results", ErrInvalidContractService, maxContractMockResults)
			}
			results += len(mock.Results)
			for resultIndex := range mock.Results {
				if len(mock.Results[resultIndex].Calls) > maxContractToolCalls-calls {
					return fmt.Errorf("%w: contract suite exceeds %d tool calls", ErrInvalidContractService, maxContractToolCalls)
				}
				calls += len(mock.Results[resultIndex].Calls)
			}
		}
	}
	return nil
}

func validateContractSuiteBytes(encoded []byte) error {
	if len(encoded) > maxContractSuiteBytes {
		return fmt.Errorf("%w: canonical contract suite exceeds %d bytes", ErrInvalidContractService, maxContractSuiteBytes)
	}
	return nil
}

func validateContractReportAdmission(report ContractTestReport) error {
	if len(report.Cases) > maxContractReportCases {
		return fmt.Errorf("%w: contract report exceeds the case limit", ErrContractTestFailed)
	}
	for _, field := range []struct {
		value string
		limit int
	}{
		{value: report.SchemaVersion, limit: maxContractReportIdentityBytes},
		{value: report.PlanDigest, limit: maxContractReportIdentityBytes},
		{value: report.SuiteDigest, limit: maxContractReportIdentityBytes},
		{value: report.Digest, limit: maxContractReportIdentityBytes},
		{value: report.Attestation, limit: maxContractAttestationBytes},
	} {
		if err := validateContractReportString(field.value, field.limit, false); err != nil {
			return err
		}
	}
	calls := 0
	effects := 0
	argumentBudget := maxContractReportBytes
	for caseIndex := range report.Cases {
		current := &report.Cases[caseIndex]
		if err := validateContractReportString(current.Name, maxContractReportIdentityBytes, true); err != nil {
			return err
		}
		if err := validateContractReportString(current.OutputDigest, maxContractReportIdentityBytes, false); err != nil {
			return err
		}
		if err := validateContractReportString(current.Message, maxContractReportMessageBytes, false); err != nil {
			return err
		}
		if current.Failure != nil {
			if err := validateContractReportString(current.Failure.Code, maxContractReportIdentityBytes, true); err != nil {
				return err
			}
			if err := validateContractReportString(current.Failure.Message, maxContractReportMessageBytes, false); err != nil {
				return err
			}
		}
		if len(current.Effects) > maxContractReportEffects-effects {
			return fmt.Errorf("%w: contract report exceeds the effect limit", ErrContractTestFailed)
		}
		effects += len(current.Effects)
		for _, effect := range current.Effects {
			if err := validateContractReportString(string(effect), maxContractReportIdentityBytes, true); err != nil {
				return err
			}
		}
		if len(current.Calls) > maxContractReportCalls-calls {
			return fmt.Errorf("%w: contract report exceeds the tool-call limit", ErrContractTestFailed)
		}
		calls += len(current.Calls)
		for callIndex := range current.Calls {
			call := &current.Calls[callIndex]
			for _, value := range []string{call.NodeID, call.Kind, call.Name, string(call.Effect), string(call.Outcome)} {
				if err := validateContractReportString(value, maxContractReportIdentityBytes, true); err != nil {
					return err
				}
			}
			if err := consumeBoundedContractJSON(call.Arguments, &argumentBudget, 0); err != nil {
				return fmt.Errorf("%w: contract report call arguments exceed safe JSON bounds", ErrContractTestFailed)
			}
		}
	}
	encoded, err := canonicalJSON(report)
	if err != nil {
		return fmt.Errorf("%w: contract report is not canonical JSON", ErrContractTestFailed)
	}
	return validateContractReportBytes(encoded)
}

func validateContractReportBytes(encoded []byte) error {
	if len(encoded) > maxContractReportBytes {
		return fmt.Errorf("%w: contract report exceeds the encoded byte limit", ErrContractTestFailed)
	}
	return nil
}

func validateContractReportString(value string, limit int, required bool) error {
	if len(value) > limit || !utf8.ValidString(value) || value != strings.TrimSpace(value) {
		return fmt.Errorf("%w: contract report contains an invalid or oversized string", ErrContractTestFailed)
	}
	if required && value == "" {
		return fmt.Errorf("%w: contract report requires a stable identity", ErrContractTestFailed)
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return fmt.Errorf("%w: contract report contains an invalid or oversized string", ErrContractTestFailed)
		}
	}
	return nil
}

func consumeBoundedContractJSON(value any, remaining *int, depth int) error {
	if depth > 64 || remaining == nil || *remaining < 0 {
		return errors.New("JSON value exceeds safe bounds")
	}
	consume := func(count int) error {
		if count < 0 || count > *remaining {
			return errors.New("JSON value exceeds safe bounds")
		}
		*remaining -= count
		return nil
	}
	switch current := value.(type) {
	case nil:
		return consume(4)
	case bool:
		if current {
			return consume(4)
		}
		return consume(5)
	case string:
		length, err := contractJSONStringBytes(current)
		if err != nil {
			return err
		}
		return consume(length)
	case json.Number:
		if len(current) > maxContractReportIdentityBytes {
			return errors.New("JSON number exceeds safe bounds")
		}
		encoded, err := json.Marshal(current)
		if err != nil {
			return err
		}
		return consume(len(encoded))
	case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		encoded, err := json.Marshal(current)
		if err != nil {
			return err
		}
		return consume(len(encoded))
	case []any:
		if err := consume(2); err != nil {
			return err
		}
		for index := range current {
			if index > 0 {
				if err := consume(1); err != nil {
					return err
				}
			}
			if err := consumeBoundedContractJSON(current[index], remaining, depth+1); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		if err := consume(2); err != nil {
			return err
		}
		index := 0
		for key, child := range current {
			if index > 0 {
				if err := consume(1); err != nil {
					return err
				}
			}
			keyBytes, err := contractJSONStringBytes(key)
			if err != nil {
				return err
			}
			if err := consume(keyBytes + 1); err != nil {
				return err
			}
			if err := consumeBoundedContractJSON(child, remaining, depth+1); err != nil {
				return err
			}
			index++
		}
		return nil
	default:
		return errors.New("value is not native JSON")
	}
}

func contractJSONStringBytes(value string) (int, error) {
	if !utf8.ValidString(value) {
		return 0, errors.New("JSON string is invalid UTF-8")
	}
	length := 2
	for _, current := range value {
		switch current {
		case '\\', '"', '\b', '\f', '\n', '\r', '\t':
			length += 2
		case '<', '>', '&', '\u2028', '\u2029':
			length += 6
		default:
			if current < 0x20 {
				length += 6
			} else {
				length += utf8.RuneLen(current)
			}
		}
		if length > maxContractReportBytes {
			return 0, errors.New("JSON string exceeds safe bounds")
		}
	}
	return length, nil
}

func (s *ContractRegistrationService) ready(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidContractService)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.kinds == nil || nilInterface(s.catalog) || nilInterface(s.definitions) ||
		nilInterface(s.authorizer) || nilInterface(s.attestor) {
		return fmt.Errorf("%w: service is not initialized", ErrInvalidContractService)
	}
	return nil
}

func (s *ContractRegistrationService) requireReport(ctx context.Context, plan *compile.ExecutionPlan, report ContractTestReport) error {
	if plan == nil {
		return fmt.Errorf("%w: execution plan is required", ErrContractTestFailed)
	}
	return s.verifyReport(ctx, plan.Digest, report.SuiteDigest, "", report)
}

func (s *ContractRegistrationService) verifyReport(ctx context.Context, planDigest, suiteDigest, testDigest string, report ContractTestReport) error {
	if admissionErr := validateContractReportAdmission(report); admissionErr != nil {
		return admissionErr
	}
	if report.SchemaVersion != ContractSuiteSchemaVersion || report.PlanDigest != planDigest || report.SuiteDigest != suiteDigest ||
		!report.Passed || report.Digest == "" || strings.TrimSpace(report.Attestation) == "" ||
		values.ValidateDigest(report.PlanDigest) != nil || values.ValidateDigest(report.SuiteDigest) != nil || values.ValidateDigest(report.Digest) != nil ||
		(testDigest != "" && report.Digest != testDigest) {
		return fmt.Errorf("%w: passing report for the exact plan is required", ErrContractTestFailed)
	}
	if len(report.Cases) < s.policy.MinimumCases {
		return fmt.Errorf("%w: policy requires at least %d cases", ErrContractTestFailed, s.policy.MinimumCases)
	}
	reportCopy := report
	reportCopy.Digest = ""
	reportCopy.Attestation = ""
	encoded, err := canonicalJSON(reportCopy)
	if err != nil || values.SHA256Digest(encoded) != report.Digest {
		return fmt.Errorf("%w: report digest mismatch", ErrContractTestFailed)
	}
	if err := s.attestor.VerifyContractReport(ctx, report.Digest, report.Attestation); err != nil {
		return errors.Join(ErrContractTestFailed, err)
	}
	return ctx.Err()
}

func validateContractSuiteForPlan(plan *compile.ExecutionPlan, kinds stepkind.Registry, suite WorkflowContractSuite, requireEffectCoverage bool) error {
	if plan == nil || plan.Graph.Nodes == nil {
		return fmt.Errorf("%w: execution plan is required", ErrInvalidContractService)
	}
	planNodes := make(map[string]graph.Node, len(plan.Graph.Nodes))
	nodeEffects := make(map[string]graph.EffectSet, len(plan.Graph.Nodes))
	var workflowEffects graph.EffectSet
	for _, node := range plan.Graph.Nodes {
		_, spec, err := stepkind.Resolve(kinds, node.Kind, node.KindVersion)
		if err != nil {
			return fmt.Errorf("%w: resolve node %s: %w", ErrInvalidContractService, node.ID, err)
		}
		effects, err := mergedContractEffects(append(append(graph.EffectSet(nil), spec.Effects...), node.Effects...))
		if err != nil {
			return fmt.Errorf("%w: node %s effects: %w", ErrInvalidContractService, node.ID, err)
		}
		planNodes[node.ID], nodeEffects[node.ID] = node, effects
		workflowEffects = append(workflowEffects, effects...)
	}
	wantedEffects, err := mergedContractEffects(workflowEffects)
	if err != nil {
		return fmt.Errorf("%w: workflow effects: %w", ErrInvalidContractService, err)
	}
	coveredEffects := make(map[graph.Effect]struct{}, len(wantedEffects))
	for _, contractCase := range suite.Cases {
		for _, effect := range contractCase.ExpectedEffects {
			if !containsEffect(wantedEffects, effect) {
				return fmt.Errorf("%w: case %s effect %q is not declared by the compiled plan", ErrInvalidContractService, contractCase.Name, effect)
			}
			coveredEffects[effect] = struct{}{}
		}
		if len(contractCase.Mocks) != len(planNodes) {
			return fmt.Errorf("%w: case %s requires exactly one mock for every compiled node", ErrInvalidContractService, contractCase.Name)
		}
		for _, mock := range contractCase.Mocks {
			node, ok := planNodes[mock.NodeID]
			if !ok {
				return fmt.Errorf("%w: case %s mocks unknown node %q", ErrInvalidContractService, contractCase.Name, mock.NodeID)
			}
			_, spec, resolveErr := stepkind.Resolve(kinds, node.Kind, node.KindVersion)
			if resolveErr != nil {
				return fmt.Errorf("%w: resolve node %s: %w", ErrInvalidContractService, node.ID, resolveErr)
			}
			if mock.Kind != spec.Name || mock.KindVersion != spec.Version ||
				!reflectContractJSONEqual(mock.ConfigSchema, spec.ConfigSchema) ||
				!reflectContractJSONEqual(mock.InputSchema, spec.InputSchema) ||
				!reflectContractJSONEqual(mock.OutputSchema, spec.OutputSchema) ||
				!reflectContractJSONEqual(mock.ExpectedConfig, node.Config) {
				return fmt.Errorf("%w: case %s mock %s does not match its exact compiled kind, schemas, or config", ErrInvalidContractService, contractCase.Name, mock.NodeID)
			}
			if err := values.ValidateValueSetSchema(spec.InputSchema, mock.ExpectedInputs); err != nil {
				return fmt.Errorf("%w: case %s mock %s inputs do not satisfy the registered schema: %w", ErrInvalidContractService, contractCase.Name, mock.NodeID, err)
			}
			for _, result := range mock.Results {
				maxAttempts := 1
				if node.Retry != nil {
					maxAttempts = node.Retry.Attempts
				}
				if result.Attempt > maxAttempts {
					return fmt.Errorf("%w: case %s mock %s result attempt %d exceeds the compiled retry limit %d", ErrInvalidContractService, contractCase.Name, mock.NodeID, result.Attempt, maxAttempts)
				}
				if node.ForEach == nil && result.Iteration != "" {
					return fmt.Errorf("%w: case %s mock %s declares a fan-out iteration for an ordinary node", ErrInvalidContractService, contractCase.Name, mock.NodeID)
				}
				if node.ForEach != nil && result.Iteration == "" {
					return fmt.Errorf("%w: case %s mock %s requires an exact fan-out iteration on every result", ErrInvalidContractService, contractCase.Name, mock.NodeID)
				}
				expectedInputs := mock.ExpectedInputs
				if result.ExpectedInputs != nil {
					expectedInputs = *result.ExpectedInputs
				}
				if err := values.ValidateValueSetSchema(spec.InputSchema, expectedInputs); err != nil {
					return fmt.Errorf("%w: case %s mock %s result inputs do not satisfy the registered schema: %w", ErrInvalidContractService, contractCase.Name, mock.NodeID, err)
				}
				if result.Outputs != nil {
					if err := values.ValidateValueSetSchema(spec.OutputSchema, result.Outputs); err != nil {
						return fmt.Errorf("%w: case %s mock %s outputs do not satisfy the registered schema: %w", ErrInvalidContractService, contractCase.Name, mock.NodeID, err)
					}
				}
				for _, call := range result.Calls {
					if !containsEffect(nodeEffects[mock.NodeID], call.Effect) {
						return fmt.Errorf("%w: case %s mock %s call declares effect %q outside that node", ErrInvalidContractService, contractCase.Name, mock.NodeID, call.Effect)
					}
				}
			}
		}
		for _, call := range contractCase.ExpectedCalls {
			if !containsEffect(nodeEffects[call.NodeID], call.Effect) {
				return fmt.Errorf("%w: case %s expected call declares effect %q outside node %s", ErrInvalidContractService, contractCase.Name, call.Effect, call.NodeID)
			}
		}
	}
	if requireEffectCoverage {
		for _, effect := range wantedEffects {
			if _, covered := coveredEffects[effect]; !covered {
				return fmt.Errorf("%w: contract suite does not cover declared effect %q", ErrInvalidContractService, effect)
			}
		}
	}
	return nil
}

func validateContractTestReport(suite WorkflowContractSuite, report ContractTestReport) error {
	if len(report.Cases) != len(suite.Cases) {
		return fmt.Errorf("%w: report must contain exactly one result for every contract case", ErrContractTestFailed)
	}
	allPassed := len(report.Cases) != 0
	seen := make(map[string]struct{}, len(report.Cases))
	for index, result := range report.Cases {
		expected := suite.Cases[index]
		if result.Name != expected.Name {
			return fmt.Errorf("%w: report case %d is %q, expected %q", ErrContractTestFailed, index, result.Name, expected.Name)
		}
		if _, duplicate := seen[result.Name]; duplicate {
			return fmt.Errorf("%w: report has duplicate case %q", ErrContractTestFailed, result.Name)
		}
		seen[result.Name] = struct{}{}
		if result.Message != "" {
			if err := validateContractIdentity("case result message", result.Message); err != nil {
				return fmt.Errorf("%w: case %s: %w", ErrContractTestFailed, result.Name, err)
			}
		}
		mockNodes := make(map[string]struct{}, len(expected.Mocks))
		for _, mock := range expected.Mocks {
			mockNodes[mock.NodeID] = struct{}{}
		}
		for _, call := range result.Calls {
			if err := validateContractToolCall(call, mockNodes, expected.ExpectedEffects, ""); err != nil {
				return fmt.Errorf("%w: case %s call: %w", ErrContractTestFailed, result.Name, err)
			}
		}
		observedEffects, effectErr := canonicalEffects(result.Effects)
		if effectErr != nil {
			return fmt.Errorf("%w: case %s observed effects: %w", ErrContractTestFailed, result.Name, effectErr)
		}
		if result.Passed && !reflectContractJSONEqual(observedEffects, expected.ExpectedEffects) {
			return fmt.Errorf("%w: case %s observed effects do not match the contract", ErrContractTestFailed, result.Name)
		}
		if !result.Passed {
			allPassed = false
			if strings.TrimSpace(result.Message) == "" {
				return fmt.Errorf("%w: failed case %s requires a safe message", ErrContractTestFailed, result.Name)
			}
			if result.Failure != nil {
				if err := validateExpectedContractError(*result.Failure); err != nil {
					return fmt.Errorf("%w: failed case %s error: %w", ErrContractTestFailed, result.Name, err)
				}
			}
			if result.OutputDigest != "" && values.ValidateDigest(result.OutputDigest) != nil {
				return fmt.Errorf("%w: failed case %s output digest is invalid", ErrContractTestFailed, result.Name)
			}
			continue
		}
		if result.Message != "" {
			return fmt.Errorf("%w: passed case %s must not carry a failure message", ErrContractTestFailed, result.Name)
		}
		if !reflectContractJSONEqual(result.Calls, expected.ExpectedCalls) {
			return fmt.Errorf("%w: case %s observed calls do not match the contract", ErrContractTestFailed, result.Name)
		}
		if expected.ExpectedError != nil {
			if result.OutputDigest != "" || result.Failure == nil || result.Failure.Code != expected.ExpectedError.Code ||
				(expected.ExpectedError.Message != "" && result.Failure.Message != expected.ExpectedError.Message) {
				return fmt.Errorf("%w: case %s observed error does not match the contract", ErrContractTestFailed, result.Name)
			}
			if err := validateExpectedContractError(*result.Failure); err != nil {
				return fmt.Errorf("%w: case %s observed error: %w", ErrContractTestFailed, result.Name, err)
			}
			continue
		}
		if result.Failure != nil {
			return fmt.Errorf("%w: case %s unexpectedly reports an error", ErrContractTestFailed, result.Name)
		}
		expectedDigest, err := values.DigestValueSet(expected.ExpectedOutputs)
		if err != nil || result.OutputDigest != expectedDigest {
			return fmt.Errorf("%w: case %s output digest does not match the contract", ErrContractTestFailed, result.Name)
		}
	}
	if report.Passed != allPassed {
		return fmt.Errorf("%w: aggregate pass state does not match case results", ErrContractTestFailed)
	}
	return nil
}

func canonicalEffects(input graph.EffectSet) (graph.EffectSet, error) {
	seen := make(map[graph.Effect]struct{}, len(input))
	result := make(graph.EffectSet, 0, len(input))
	for _, effect := range input {
		if !effect.Valid() {
			return nil, fmt.Errorf("unsupported effect %q", effect)
		}
		if _, duplicate := seen[effect]; duplicate {
			return nil, fmt.Errorf("duplicate effect %q", effect)
		}
		seen[effect] = struct{}{}
		result = append(result, effect)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func mergedContractEffects(input graph.EffectSet) (graph.EffectSet, error) {
	seen := make(map[graph.Effect]struct{}, len(input))
	result := make(graph.EffectSet, 0, len(input))
	for _, effect := range input {
		if !effect.Valid() {
			return nil, fmt.Errorf("unsupported effect %q", effect)
		}
		if _, duplicate := seen[effect]; duplicate {
			continue
		}
		seen[effect] = struct{}{}
		result = append(result, effect)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func validateExpectedContractError(input ContractExpectedError) error {
	if err := validateContractIdentity("error code", input.Code); err != nil {
		return err
	}
	if input.Message != "" {
		return validateContractIdentity("error message", input.Message)
	}
	return nil
}

func validateContractToolCall(input ContractToolCall, nodes map[string]struct{}, effects graph.EffectSet, requiredNode string) error {
	if err := graph.ValidateID(input.NodeID); err != nil {
		return err
	}
	if _, ok := nodes[input.NodeID]; !ok {
		return fmt.Errorf("unknown mock node %q", input.NodeID)
	}
	if requiredNode != "" && input.NodeID != requiredNode {
		return fmt.Errorf("call node %q does not match executing mock node %q", input.NodeID, requiredNode)
	}
	if err := validateContractIdentity("call kind", input.Kind); err != nil {
		return err
	}
	if err := validateContractIdentity("call name", input.Name); err != nil {
		return err
	}
	if !input.Effect.Valid() || !containsEffect(effects, input.Effect) {
		return fmt.Errorf("call effect %q is invalid or undeclared", input.Effect)
	}
	if !input.Outcome.Valid() {
		return fmt.Errorf("call outcome %q is invalid", input.Outcome)
	}
	if input.Arguments == nil {
		return errors.New("call arguments must be a JSON object")
	}
	_, err := canonicalJSON(input.Arguments)
	return err
}

func validateContractIdentity(name, value string) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s is required as stable UTF-8 without surrounding whitespace", name)
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return fmt.Errorf("%s must not contain control characters", name)
		}
	}
	return nil
}

func containsEffect(input graph.EffectSet, target graph.Effect) bool {
	for _, effect := range input {
		if effect == target {
			return true
		}
	}
	return false
}

func reflectContractJSONEqual(left, right any) bool {
	leftJSON, leftErr := canonicalJSON(left)
	rightJSON, rightErr := canonicalJSON(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func (s *ContractRegistrationService) sealReport(ctx context.Context, report ContractTestReport) (ContractTestReport, error) {
	if admissionErr := validateContractReportAdmission(report); admissionErr != nil {
		return ContractTestReport{}, admissionErr
	}
	report.Digest, report.Attestation = "", ""
	encoded, err := canonicalJSON(report)
	if err != nil {
		return ContractTestReport{}, err
	}
	report.Digest = values.SHA256Digest(encoded)
	report.Attestation, err = s.attestor.AttestContractReport(ctx, report.Digest)
	if err != nil {
		return ContractTestReport{}, fmt.Errorf("attest contract report: %w", err)
	}
	if admissionErr := validateContractReportAdmission(report); admissionErr != nil {
		return ContractTestReport{}, admissionErr
	}
	if strings.TrimSpace(report.Attestation) == "" {
		return ContractTestReport{}, errors.New("attest contract report: empty attestation")
	}
	if err := ctx.Err(); err != nil {
		return ContractTestReport{}, err
	}
	return report, nil
}

func (s *ContractRegistrationService) registerWorkflow(ctx context.Context, record hadronregistry.WorkflowRecord, makeCurrent bool) (hadronregistry.WorkflowRecord, error) {
	s.registrationMu.Lock()
	defer s.registrationMu.Unlock()
	registeredAt := s.now()
	if registeredAt.IsZero() {
		return hadronregistry.WorkflowRecord{}, fmt.Errorf("%w: registration clock returned zero time", ErrInvalidContractService)
	}
	registeredAt = registeredAt.UTC()
	if !s.lastRegistrationAt.IsZero() && !registeredAt.After(s.lastRegistrationAt) {
		return hadronregistry.WorkflowRecord{}, fmt.Errorf("%w: registration clock must advance monotonically", ErrInvalidContractService)
	}
	if err := ctx.Err(); err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	// Consume the validated instant before calling the catalog. This prevents a
	// post-commit catalog error from allowing a later write to reuse or regress
	// the timestamp while durable state may already contain the registration.
	s.lastRegistrationAt = registeredAt
	record.RegisteredAt = registeredAt
	return s.catalog.RegisterWorkflow(ctx, record, makeCurrent)
}

func (s *ContractRegistrationService) authorizeRequested(ctx context.Context, operation NamespaceOperation, namespace, name, principal string) error {
	return s.authorize(ctx, NamespaceAuthorization{
		Stage: NamespaceAuthorizationRequested, Operation: operation,
		Namespace: namespace, Name: name, Principal: principal,
	})
}

func (s *ContractRegistrationService) authorizeResolved(ctx context.Context, operation NamespaceOperation, namespace, name, principal, authority, digest string) error {
	return s.authorize(ctx, NamespaceAuthorization{
		Stage: NamespaceAuthorizationResolved, Operation: operation,
		Namespace: namespace, Name: name, Principal: principal,
		Authority: authority, Digest: digest,
	})
}

func (s *ContractRegistrationService) authorizeRecord(ctx context.Context, operation NamespaceOperation, requestedNamespace, requestedName string, record hadronregistry.WorkflowRecord, principal string) error {
	if record.Namespace != requestedNamespace || (requestedName != "" && record.Name != requestedName) {
		return fmt.Errorf("%w: resolved workflow identity differs from the authorized request", ErrNamespaceUnauthorized)
	}
	return s.authorize(ctx, NamespaceAuthorization{
		Stage: NamespaceAuthorizationResolved, Operation: operation, Namespace: record.Namespace, Name: record.Name,
		Principal: principal, Authority: record.Authority, Digest: record.Digest,
	})
}

func (s *ContractRegistrationService) authorize(ctx context.Context, request NamespaceAuthorization) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrNamespaceUnauthorized)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !request.Operation.valid() || !request.Stage.valid() {
		return fmt.Errorf("%w: authorization operation and stage are invalid", ErrNamespaceUnauthorized)
	}
	if err := hadronregistry.ValidateWorkflowNamespace(request.Namespace); err != nil {
		return fmt.Errorf("%w: invalid requested namespace", ErrNamespaceUnauthorized)
	}
	if request.Name == "" {
		if request.Operation != NamespaceSearch || request.Stage != NamespaceAuthorizationRequested {
			return fmt.Errorf("%w: workflow name is required", ErrNamespaceUnauthorized)
		}
	} else if _, _, _, err := canonicalRequestedIdentity(request.Namespace, request.Name, request.Principal); err != nil {
		return err
	}
	if err := validateContractIdentity("namespace principal", request.Principal); err != nil {
		return fmt.Errorf("%w: invalid principal", ErrNamespaceUnauthorized)
	}
	switch request.Stage {
	case NamespaceAuthorizationRequested:
		if request.Authority != "" || request.Digest != "" {
			return fmt.Errorf("%w: requested authorization must not claim resolved identity", ErrNamespaceUnauthorized)
		}
	case NamespaceAuthorizationResolved:
		if err := validateContractIdentity("resolved authority", request.Authority); err != nil {
			return fmt.Errorf("%w: invalid resolved authority", ErrNamespaceUnauthorized)
		}
		if err := values.ValidateDigest(request.Digest); err != nil {
			return fmt.Errorf("%w: invalid resolved digest", ErrNamespaceUnauthorized)
		}
	}
	if err := s.authorizer.AuthorizeNamespace(ctx, request); err != nil {
		return errors.Join(ErrNamespaceUnauthorized, err)
	}
	return ctx.Err()
}

func (o NamespaceOperation) valid() bool {
	switch o {
	case NamespaceRegister, NamespacePin, NamespaceUnpin, NamespacePublish,
		NamespaceInspect, NamespaceSearch, NamespaceResolve, NamespacePackage:
		return true
	default:
		return false
	}
}

func (s NamespaceAuthorizationStage) valid() bool {
	return s == NamespaceAuthorizationRequested || s == NamespaceAuthorizationResolved
}

func canonicalRequestedNamespace(namespace, principal string) (string, string, error) {
	namespace = strings.TrimSpace(namespace)
	principal = strings.TrimSpace(principal)
	if err := hadronregistry.ValidateWorkflowNamespace(namespace); err != nil {
		return "", "", fmt.Errorf("%w: invalid requested namespace", ErrNamespaceUnauthorized)
	}
	if err := validateContractIdentity("namespace principal", principal); err != nil {
		return "", "", fmt.Errorf("%w: invalid principal", ErrNamespaceUnauthorized)
	}
	return namespace, principal, nil
}

func canonicalRequestedIdentity(explicitNamespace, name, principal string) (string, string, string, error) {
	explicitNamespace = strings.TrimSpace(explicitNamespace)
	name = strings.TrimSpace(name)
	principal = strings.TrimSpace(principal)
	if err := hadronregistry.ValidateWorkflowName(name); err != nil {
		return "", "", "", fmt.Errorf("%w: invalid requested workflow name", ErrNamespaceUnauthorized)
	}
	separator := strings.LastIndexByte(name, '/')
	derivedNamespace := ""
	if separator > 0 {
		derivedNamespace = name[:separator]
	}
	if explicitNamespace == "" {
		explicitNamespace = derivedNamespace
	}
	if err := hadronregistry.ValidateWorkflowNamespace(explicitNamespace); err != nil || derivedNamespace == "" || explicitNamespace != derivedNamespace {
		return "", "", "", fmt.Errorf("%w: workflow name must be within the requested namespace", ErrNamespaceUnauthorized)
	}
	if err := validateContractIdentity("namespace principal", principal); err != nil {
		return "", "", "", fmt.Errorf("%w: invalid principal", ErrNamespaceUnauthorized)
	}
	return explicitNamespace, name, principal, nil
}

func canonicalJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var cloned any
	if err := decoder.Decode(&cloned); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("value has trailing JSON")
	}
	return json.Marshal(cloned)
}

func sortedEffects(input graph.EffectSet) graph.EffectSet {
	seen := make(map[graph.Effect]struct{}, len(input))
	for _, effect := range input {
		seen[effect] = struct{}{}
	}
	result := make(graph.EffectSet, 0, len(seen))
	for effect := range seen {
		result = append(result, effect)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}
