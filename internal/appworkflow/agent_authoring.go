package appworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/workflow/authoring"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	graphschema "github.com/hollis-labs/hadron/workflow/graph/schema"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	AgentAuthoringResultSchemaID       = "https://schemas.hollis-labs.dev/workflow/authoring/v1/result.schema.json"
	AgentAuthoringResultSchemaVersion  = "1"
	maximumAgentAuthoringIdentityBytes = 512
)

var ErrInvalidAgentAuthoring = errors.New("invalid agent workflow authoring request")

// AgentAuthoringRequest is the transport-neutral agent flywheel input. The
// envelope remains raw so DecodeEnvelope enforces byte/depth/unknown-field
// bounds before compiler allocation.
type AgentAuthoringRequest struct {
	Envelope    json.RawMessage        `json:"envelope"`
	ID          string                 `json:"id"`
	Version     string                 `json:"version"`
	Namespace   string                 `json:"namespace"`
	Suite       *WorkflowContractSuite `json:"suite,omitempty"`
	MakeCurrent bool                   `json:"make_current,omitempty"`
}

// AgentAuthoringHostIdentity is trusted host configuration, deliberately
// separate from AgentAuthoringRequest and every generated client DTO.
type AgentAuthoringHostIdentity struct {
	Authority  string
	TrustClass string
	Principal  string
}

// AgentRegistration is the safe immutable result; raw source and publisher
// identity do not cross this boundary.
type AgentRegistration struct {
	Name          string             `json:"name"`
	Namespace     string             `json:"namespace"`
	Version       string             `json:"version"`
	Digest        string             `json:"digest"`
	PlanDigest    string             `json:"plan_digest"`
	SourceFormat  graph.SourceFormat `json:"source_format"`
	SchemaID      string             `json:"schema_id"`
	SchemaVersion string             `json:"schema_version"`
}

type AgentAuthoringResult struct {
	SchemaID      string                        `json:"schema_id"`
	SchemaVersion string                        `json:"schema_version"`
	Definition    *graph.DefinitionRef          `json:"definition,omitempty"`
	Plan          *runtime.PlanRef              `json:"plan,omitempty"`
	Diagnostics   []authoring.CompactDiagnostic `json:"diagnostics"`
	Scaffold      *WorkflowContractSuite        `json:"scaffold,omitempty"`
	Report        *ContractTestReport           `json:"report,omitempty"`
	Registration  *AgentRegistration            `json:"registration,omitempty"`
}

type AgentAuthoringOptions struct {
	Stager       *AuthoringSourceStager
	Contracts    *ContractRegistrationService
	HostIdentity AgentAuthoringHostIdentity
	Limits       authoring.Limits
}

type AgentAuthoringService struct {
	stager    *AuthoringSourceStager
	contracts *ContractRegistrationService
	identity  AgentAuthoringHostIdentity
	limits    authoring.Limits
}

func NewAgentAuthoringService(options AgentAuthoringOptions) (*AgentAuthoringService, error) {
	if options.Stager == nil || options.Contracts == nil {
		return nil, fmt.Errorf("%w: stager and contract registration service are required", ErrInvalidAgentAuthoring)
	}
	if err := validateAgentAuthoringHostIdentity(options.HostIdentity); err != nil {
		return nil, err
	}
	return &AgentAuthoringService{stager: options.Stager, contracts: options.Contracts, identity: options.HostIdentity, limits: options.Limits}, nil
}

// Author validates, qualifies, and optionally registers one agent-authored
// definition through ContractRegistrationService. A nil suite returns the
// exact generated scaffold and performs no catalog mutation.
func (s *AgentAuthoringService) Author(ctx context.Context, request AgentAuthoringRequest) (AgentAuthoringResult, error) {
	return s.author(ctx, request, s.identity.Principal, true)
}

// author is the shared authoring path used by the authenticated lifecycle
// facade. Authority and trust remain frozen host configuration; only the
// already-bound caller principal may replace the configured service principal.
// register=false executes contract tests without mutating the catalog.
func (s *AgentAuthoringService) author(ctx context.Context, request AgentAuthoringRequest, principal string, register bool) (AgentAuthoringResult, error) {
	return s.authorWithRegistration(ctx, request, principal, register, nil)
}

type agentRegistrationFunc func(context.Context, RegisterWorkflowRequest, *workflowcompile.ExecutionPlan) (hadronregistry.WorkflowRecord, error)

// authorWithRegistration keeps validation/qualification single-sourced while
// allowing the lifecycle facade to attach Hadron-owned source-activation
// reconciliation to the exact current-version registration. The callback is
// internal authority and never comes from a transport request.
func (s *AgentAuthoringService) authorWithRegistration(ctx context.Context, request AgentAuthoringRequest, principal string, register bool, registerCurrent agentRegistrationFunc) (AgentAuthoringResult, error) {
	result := AgentAuthoringResult{SchemaID: AgentAuthoringResultSchemaID, SchemaVersion: AgentAuthoringResultSchemaVersion, Diagnostics: []authoring.CompactDiagnostic{}}
	if ctx == nil || s == nil || s.stager == nil || s.contracts == nil {
		return result, fmt.Errorf("%w: service is unavailable", ErrInvalidAgentAuthoring)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := validateAgentAuthoringRequest(request); err != nil {
		return result, err
	}
	identity := s.identity
	identity.Principal = strings.TrimSpace(principal)
	if err := validateAgentAuthoringHostIdentity(identity); err != nil {
		return result, err
	}
	if err := s.contracts.authorizeRequested(ctx, NamespaceRegister, request.Namespace, request.Namespace+"/"+request.ID, identity.Principal); err != nil {
		return result, err
	}
	envelope, findings := authoring.DecodeEnvelope(request.Envelope, s.limits)
	if len(findings) != 0 {
		result.Diagnostics = authoring.CompactDiagnostics(findings)
		return result, nil
	}

	source, ref, sourceErr := stagedAgentSource(envelope, request, identity)
	if sourceErr != nil {
		return result, sourceErr
	}
	if stageErr := s.stager.Stage(ctx, source); stageErr != nil {
		return result, stageErr
	}
	defer s.stager.Remove(ref)
	result.Definition = &ref

	validation, validationErr := s.contracts.Validate(ctx, ref)
	if validationErr != nil {
		return result, validationErr
	}
	if validation.Plan == nil || len(validation.Diagnostics) != 0 {
		result.Diagnostics = authoring.CompactDiagnostics(validation.Diagnostics)
		return result, nil
	}
	if findings := authoring.ValidatePlanLimits(validation.Plan, s.limits); len(findings) != 0 {
		result.Diagnostics = authoring.CompactDiagnostics(findings)
		return result, nil
	}
	if validation.Plan.Graph.Namespace != request.Namespace {
		return result, fmt.Errorf("%w: compiled graph namespace differs from the authorized registry namespace", ErrInvalidAgentAuthoring)
	}
	planRef := runtime.PlanRef{ID: validation.Plan.ID, Version: validation.Plan.Graph.Version, Digest: validation.Plan.Digest, SchemaVersion: validation.Plan.SchemaVersion}
	result.Plan = &planRef
	if request.Suite == nil {
		scaffold, scaffoldErr := s.contracts.GenerateContractScaffold(ctx, ref)
		if scaffoldErr != nil {
			return result, scaffoldErr
		}
		result.Scaffold = &scaffold
		return result, nil
	}
	report, reportErr := s.contracts.ExecuteContractTests(ctx, ref, *request.Suite)
	if reportErr != nil {
		return result, reportErr
	}
	result.Report = &report
	if !register {
		return result, nil
	}
	registrationRequest := RegisterWorkflowRequest{
		Definition: ref, Namespace: request.Namespace, Principal: identity.Principal,
		Report: report, MakeCurrent: request.MakeCurrent,
	}
	var record hadronregistry.WorkflowRecord
	var registerErr error
	if registerCurrent == nil {
		record, registerErr = s.contracts.Register(context.WithoutCancel(ctx), registrationRequest)
	} else {
		record, registerErr = registerCurrent(context.WithoutCancel(ctx), registrationRequest, validation.Plan)
	}
	if registerErr != nil {
		return result, registerErr
	}
	registration := safeAgentRegistration(record)
	result.Registration = &registration
	return result, nil
}

func stagedAgentSource(envelope authoring.Envelope, request AgentAuthoringRequest, identity AgentAuthoringHostIdentity) (ResolvedSource, graph.DefinitionRef, error) {
	var sourceBytes []byte
	format := graph.SourceWorkflow
	schemaID, schemaVersion := authoring.WorkflowSourceSchemaID, authoring.WorkflowSourceSchemaVersion
	envelopeDigest := values.SHA256Digest(request.Envelope)
	locator := "agent-" + strings.TrimPrefix(envelopeDigest, "sha256:") + ".workflow.yaml"
	hostProvenance := graph.Provenance{
		Authority: identity.Authority, Origin: "agent-authored", Locator: locator,
		Revision: request.Version,
		Parents:  []graph.ProvenanceRef{{Authority: "agent-envelope", Locator: "authoring-envelope", Digest: envelopeDigest}},
		Metadata: graph.Metadata{"trust_class": identity.TrustClass},
	}
	if envelope.Format == authoring.FormatGraphIR {
		if envelope.Graph == nil || envelope.Graph.ID != request.ID || envelope.Graph.Version != request.Version {
			return ResolvedSource{}, graph.DefinitionRef{}, fmt.Errorf("%w: graph identity differs from the request", ErrInvalidAgentAuthoring)
		}
		value := *envelope.Graph
		// Namespace is the authorized registry placement selected alongside the
		// source-local graph identity. Bind it before qualification so the
		// immutable source, plan, registry record, and later exposure all agree.
		value.Namespace = request.Namespace
		rebindAgentGraph(&value, locator, &hostProvenance)
		var err error
		sourceBytes, err = json.Marshal(value)
		if err != nil {
			return ResolvedSource{}, graph.DefinitionRef{}, fmt.Errorf("%w: canonical graph encoding: %w", ErrInvalidAgentAuthoring, err)
		}
		format, schemaID, schemaVersion = graph.SourceAgent, graphschema.ID, graphschema.Version
	} else {
		sourceBytes = []byte(envelope.Source)
	}
	digest := values.SHA256Digest(sourceBytes)
	provenance := hostProvenance
	provenance.Digest = digest
	ref := graph.DefinitionRef{
		Authority: identity.Authority, Kind: DefinitionKindAuthoring, ID: request.ID,
		Locator: locator, Version: request.Version, Digest: digest, Provenance: &provenance,
	}
	return ResolvedSource{
		Requested: ref, Definition: ref, Bytes: sourceBytes, Digest: digest,
		SourceFormat: format, SourceSchemaID: schemaID, SourceSchemaVersion: schemaVersion,
		TrustClass: identity.TrustClass,
	}, ref, nil
}

func validateAgentAuthoringRequest(request AgentAuthoringRequest) error {
	if err := graph.ValidateID(request.ID); err != nil || request.Version == "" {
		return fmt.Errorf("%w: normalized ID and version are required", ErrInvalidAgentAuthoring)
	}
	for _, value := range []string{request.Version, request.Namespace} {
		if value == "" || len(value) > maximumAgentAuthoringIdentityBytes || !utf8.ValidString(value) || strings.IndexFunc(value, unicode.IsControl) >= 0 || value != strings.TrimSpace(value) {
			return fmt.Errorf("%w: authoring request identity is invalid", ErrInvalidAgentAuthoring)
		}
	}
	if err := hadronregistry.ValidateWorkflowNamespace(request.Namespace); err != nil {
		return fmt.Errorf("%w: namespace: %w", ErrInvalidAgentAuthoring, err)
	}
	return nil
}

func validateAgentAuthoringHostIdentity(identity AgentAuthoringHostIdentity) error {
	for _, value := range []string{identity.Authority, identity.TrustClass, identity.Principal} {
		if value == "" || len(value) > maximumAgentAuthoringIdentityBytes || !utf8.ValidString(value) || strings.IndexFunc(value, unicode.IsControl) >= 0 || value != strings.TrimSpace(value) {
			return fmt.Errorf("%w: host authoring identity is invalid", ErrInvalidAgentAuthoring)
		}
	}
	return nil
}

func rebindAgentGraph(value *graph.Graph, locator string, provenance *graph.Provenance) {
	if value == nil {
		return
	}
	value.Source = agentSourceRef(value.Source, locator, true)
	value.SourceMap.Graph = agentSourceRef(value.SourceMap.Graph, locator, true)
	rebindAgentSourceMap(&value.SourceMap, locator)
	bindAgentProvenance(&value.Provenance, provenance)
	rebindAgentExtension(&value.Concurrency.Extension, locator)
	if value.Completion != nil {
		rebindAgentExtension(&value.Completion.Extension, locator)
	}
	if value.Durability != nil {
		rebindAgentExtension(&value.Durability.Extension, locator)
	}
	for name, extension := range value.Extensions {
		rebindAgentExtension(&extension, locator)
		value.Extensions[name] = extension
	}
	for index := range value.Inputs {
		value.Inputs[index].Source = agentSourceRef(value.Inputs[index].Source, locator, false)
		rebindAgentBinding(value.Inputs[index].Default, locator)
	}
	for index := range value.Outputs {
		rebindAgentOutput(&value.Outputs[index], locator)
	}
	for index := range value.Edges {
		value.Edges[index].Source = agentSourceRef(value.Edges[index].Source, locator, false)
	}
	for index := range value.Activations {
		activation := &value.Activations[index]
		activation.Source = agentSourceRef(activation.Source, locator, false)
		bindAgentProvenance(&activation.Provenance, provenance)
		for name, binding := range activation.Inputs {
			rebindAgentBinding(&binding, locator)
			activation.Inputs[name] = binding
		}
		rebindAgentExpression(activation.Policy.DeduplicationKey, locator)
	}
	for index := range value.Nodes {
		rebindAgentNode(&value.Nodes[index], locator, provenance)
	}
}

func rebindAgentSourceMap(value *graph.SourceMap, locator string) {
	for _, locations := range []map[string]graph.SourceRef{value.Inputs, value.Outputs, value.Nodes, value.Edges, value.Activations} {
		for name, source := range locations {
			source.Format, source.Locator = graph.SourceAgent, locator
			locations[name] = source
		}
	}
}

func rebindAgentNode(node *graph.Node, locator string, provenance *graph.Provenance) {
	node.Source = agentSourceRef(node.Source, locator, false)
	bindAgentProvenance(&node.Provenance, provenance)
	for index := range node.Needs {
		node.Needs[index].Source = agentSourceRef(node.Needs[index].Source, locator, false)
	}
	rebindAgentExpression(node.If, locator)
	if node.ForEach != nil {
		rebindAgentExpression(&node.ForEach.Items, locator)
	}
	for name, binding := range node.InputBindings {
		rebindAgentBinding(&binding, locator)
		node.InputBindings[name] = binding
	}
	for index := range node.Outputs {
		rebindAgentOutput(&node.Outputs[index], locator)
	}
	if node.Idempotency != nil {
		rebindAgentExpression(node.Idempotency.Key, locator)
	}
	for index := range node.Catch {
		node.Catch[index].Source = agentSourceRef(node.Catch[index].Source, locator, false)
		rebindAgentExpression(node.Catch[index].When, locator)
	}
	if node.Switch != nil {
		for index := range node.Switch.Arms {
			node.Switch.Arms[index].Source = agentSourceRef(node.Switch.Arms[index].Source, locator, false)
			rebindAgentExpression(&node.Switch.Arms[index].When, locator)
		}
	}
	if node.Call != nil {
		node.Call.Definition.Provenance = nil
	}
	if node.Verification != nil {
		rebindAgentVerification(node.Verification, locator)
	}
	if node.Memoization != nil {
		rebindAgentExpression(&node.Memoization.Key, locator)
		rebindAgentExtension(&node.Memoization.Extension, locator)
	}
	if node.Durability != nil {
		rebindAgentExtension(&node.Durability.Extension, locator)
	}
	if node.Service != nil {
		rebindAgentVerification(node.Service.ReadyCheck, locator)
		rebindAgentExtension(&node.Service.Extension, locator)
	}
	if node.Compensation != nil {
		rebindAgentExtension(&node.Compensation.Extension, locator)
	}
	for name, extension := range node.Extensions {
		rebindAgentExtension(&extension, locator)
		node.Extensions[name] = extension
	}
}

func rebindAgentOutput(output *graph.OutputSpec, locator string) {
	output.Source = agentSourceRef(output.Source, locator, false)
	rebindAgentBinding(output.Value, locator)
}

func rebindAgentBinding(binding *graph.Binding, locator string) {
	if binding == nil {
		return
	}
	binding.Source = agentSourceRef(binding.Source, locator, false)
	rebindAgentExpression(binding.Expression, locator)
}

func rebindAgentExpression(expression *graph.Expression, locator string) {
	if expression != nil {
		expression.Source = agentSourceRef(expression.Source, locator, false)
	}
}

func rebindAgentVerification(verification *graph.VerificationSpec, locator string) {
	if verification == nil {
		return
	}
	for index := range verification.Checks {
		verification.Checks[index].Source = agentSourceRef(verification.Checks[index].Source, locator, false)
	}
	rebindAgentExtension(&verification.Extension, locator)
}

func rebindAgentExtension(extension *graph.Extension, locator string) {
	if extension != nil {
		extension.Source = agentSourceRef(extension.Source, locator, false)
	}
}

func agentSourceRef(source *graph.SourceRef, locator string, required bool) *graph.SourceRef {
	if source == nil && !required {
		return nil
	}
	result := graph.SourceRef{Format: graph.SourceAgent, Locator: locator}
	if source != nil {
		result.StartLine, result.StartColumn = source.StartLine, source.StartColumn
		result.EndLine, result.EndColumn = source.EndLine, source.EndColumn
		result.Section, result.StepName, result.StageName = source.Section, source.StepName, source.StageName
		result.Path = append([]string(nil), source.Path...)
	}
	return &result
}

func bindAgentProvenance(destination *graph.Provenance, provenance *graph.Provenance) {
	if provenance == nil {
		*destination = graph.Provenance{}
		return
	}
	*destination = *provenance
	destination.Parents = append([]graph.ProvenanceRef(nil), provenance.Parents...)
}

func safeAgentRegistration(record hadronregistry.WorkflowRecord) AgentRegistration {
	return AgentRegistration{
		Name: record.Name, Namespace: record.Namespace, Version: record.Version,
		Digest: record.Digest, PlanDigest: record.PlanDigest, SourceFormat: record.SourceFormat,
		SchemaID: record.SourceSchemaID, SchemaVersion: record.SourceSchemaVersion,
	}
}
