package appworkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/pack"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/go-workflow/adapters/transform"
	"github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/go-workflow/values"
	"github.com/hollis-labs/go-workflow/verification"
)

type contractReversibleFixtureKind struct{ *stepkindtest.Kind }

func (*contractReversibleFixtureKind) DescribeReversibility(context.Context, stepkind.ReversibilityRequest) (stepkind.ReversibilityEvidence, error) {
	return stepkind.ReversibilityEvidence{Operation: "fixture.contract.effect", ReceiptSchema: graph.Schema{}}, nil
}

func TestContractRegistrationPathEndToEnd(t *testing.T) {
	service, ref, source := newContractServiceFixture(t, nil, func(_ context.Context, request NamespaceAuthorization) error {
		if request.Namespace != "team" || request.Principal != "principal:test" ||
			(request.Stage == NamespaceAuthorizationRequested && (request.Authority != "" || request.Digest != "")) ||
			(request.Stage == NamespaceAuthorizationResolved && request.Authority != "project") {
			return errors.New("namespace denied")
		}
		return nil
	})
	canonicalRunner, ok := service.runner.(*canonicalContractRunner)
	provider, exposesVerifiers := service.definitions.(definitionVerifierCatalog)
	if !ok || !exposesVerifiers || !reflect.DeepEqual(canonicalRunner.verifiers.List(), provider.Verifiers().List()) {
		t.Fatal("default contract runner did not freeze the definition resolver's exact verifier catalog")
	}
	validation, err := service.Validate(t.Context(), ref)
	if err != nil || validation.Plan == nil || len(validation.Diagnostics) != 0 {
		t.Fatalf("Validate() = %#v, %v", validation, err)
	}
	firstScaffold, err := service.GenerateContractScaffold(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	secondScaffold, err := service.GenerateContractScaffold(t.Context(), ref)
	if err != nil || !reflectContractJSONEqual(firstScaffold, secondScaffold) {
		t.Fatalf("scaffold determinism = %#v / %#v, %v", firstScaffold, secondScaffold, err)
	}
	if len(firstScaffold.Cases) != 1 || len(firstScaffold.Cases[0].Mocks) != 1 ||
		!firstScaffold.Cases[0].Editable || firstScaffold.Cases[0].Mocks[0].ExpectedInputsEditable ||
		!reflect.DeepEqual(firstScaffold.Cases[0].ExpectedEffects, graph.EffectSet{graph.EffectCompute}) ||
		firstScaffold.Cases[0].Mocks[0].ExpectedConfig["result"] != "inputs.message" ||
		!reflect.DeepEqual(firstScaffold.Cases[0].Mocks[0].ExpectedInputs, firstScaffold.Cases[0].Inputs) {
		t.Fatalf("scaffold = %#v", firstScaffold)
	}
	if _, executeErr := service.ExecuteContractTests(t.Context(), ref, firstScaffold); !errors.Is(executeErr, ErrInvalidContractService) {
		t.Fatalf("ExecuteContractTests accepted editable scaffold = %v", executeErr)
	}

	suite := passingContractSuite(t, service, validation.Plan)
	report, err := service.ExecuteContractTests(t.Context(), ref, suite)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed || report.Digest == "" || report.Attestation == "" {
		t.Fatalf("canonical contract report = %#v", report)
	}
	record, err := service.Register(t.Context(), RegisterWorkflowRequest{
		Definition: ref, Namespace: "team", Principal: "principal:test", Report: report, MakeCurrent: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.Name != "team/contract-demo" || record.Digest != values.SHA256Digest(source) ||
		record.PlanDigest != validation.Plan.Digest || record.ContractSuiteDigest != report.SuiteDigest ||
		record.ContractTestDigest != report.Digest || record.PublisherPrincipal != "principal:test" {
		t.Fatalf("registered record = %#v", record)
	}
	service.now = func() time.Time { return time.Date(2026, time.August, 24, 13, 0, 0, 0, time.UTC) }
	replayed, err := service.Register(t.Context(), RegisterWorkflowRequest{
		Definition: ref, Namespace: "team", Principal: "principal:test", Report: report, MakeCurrent: true,
	})
	if err != nil || !replayed.RegisteredAt.Equal(record.RegisteredAt) {
		t.Fatalf("idempotent registration replay = %#v, %v", replayed, err)
	}
	query := hadronregistry.WorkflowQuery{Name: record.Name, Version: record.Version, Digest: record.Digest}
	if _, pinErr := service.Pin(t.Context(), query, "principal:test"); pinErr != nil {
		t.Fatal(pinErr)
	}
	archive, err := service.Package(t.Context(), query, suite, report, "principal:test")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := pack.ReadWorkflowSource(archive.Bytes, "workflow.yaml", pack.WorkflowArchiveLimits{})
	if err != nil || !bytes.Equal(selected.Source, source) {
		t.Fatalf("packaged source = %#v, %v", selected, err)
	}
	tamperedReport := report
	tamperedReport.Cases = append([]ContractCaseResult(nil), report.Cases...)
	tamperedReport.Cases[0].OutputDigest = values.SHA256Digest([]byte("tampered"))
	if _, packageErr := service.Package(t.Context(), query, suite, tamperedReport, "principal:test"); !errors.Is(packageErr, ErrContractTestFailed) {
		t.Fatalf("Package accepted report content with stale digest/attestation: %v", packageErr)
	}
	tamperedAttestation := report
	tamperedAttestation.Attestation = "forged-attestation"
	if _, packageErr := service.Package(t.Context(), query, suite, tamperedAttestation, "principal:test"); !errors.Is(packageErr, ErrContractTestFailed) {
		t.Fatalf("Package accepted forged attestation: %v", packageErr)
	}
	if _, publishErr := service.Publish(t.Context(), query, "principal:test"); publishErr != nil {
		t.Fatal(publishErr)
	}
	search, err := service.Search(t.Context(), "team", "contract", "principal:test")
	if err != nil || len(search) != 1 || !search[0].Published {
		t.Fatalf("Search() = %#v, %v", search, err)
	}
	resolved, err := service.Resolve(t.Context(), hadronregistry.WorkflowQuery{Name: record.Name}, "principal:test")
	if err != nil || !resolved.Movable || resolved.Record.Digest != record.Digest {
		t.Fatalf("Resolve(current) = %#v, %v", resolved, err)
	}
	resolved.Record.Source[0] = 'x'
	inspected, err := service.Inspect(t.Context(), query, "principal:test")
	if err != nil || !bytes.Equal(inspected.Source, source) {
		t.Fatalf("Inspect defensive copy = %#v, %v", inspected, err)
	}
	if err := service.Unpin(t.Context(), record.Name, "principal:test"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Publish(t.Context(), query, "principal:test"); err == nil {
		t.Fatal("Publish accepted an unpinned version")
	}
}

func TestCanonicalContractRunnerExecutesCompensationBeforeReportingOriginalFailure(t *testing.T) {
	plan := compileContractPlan(t, `workflow:
  name: compensation-contract
  version: v1
steps:
  - id: placeholder
    transform: {result: "'placeholder'"}
    outputs:
      result: {type: string}
`)
	plan.Graph.Nodes = []graph.Node{
		{ID: "a-effect", Kind: "contract-effect", KindVersion: "v1", Config: graph.Config{}, Compensation: &graph.CompensationSpec{Handler: "undo"}},
		{ID: "z-fail", Kind: "contract-fail", KindVersion: "v1", Config: graph.Config{}, Needs: []graph.Need{{Node: "a-effect", Kind: graph.EdgeControl}}},
		{ID: "undo", Kind: "contract-undo", KindVersion: "v1", Config: graph.Config{}},
	}
	plan.Graph.Edges = nil
	plan.Graph.Outputs = nil
	plan.Graph.Compensation = &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}, Mode: graph.CompensationBestEffort}
	plan.Graph.Digest, plan.Digest = "", ""
	inferred := compile.InferValueDependencies(plan, compile.DependencyOptions{})
	if inferred.Plan == nil || len(inferred.Diagnostics) != 0 {
		t.Fatalf("InferValueDependencies = %#v", inferred.Diagnostics)
	}
	plan = inferred.Plan
	for index := range plan.Graph.Nodes {
		plan.Graph.Nodes[index].Config = graph.Config{}
	}

	effect := &contractReversibleFixtureKind{Kind: stepkindtest.NewNoopKind("contract-effect", "v1")}
	effect.SpecValue.Effects = graph.EffectSet{graph.EffectMaterialize}
	effect.SpecValue.Compensation = stepkind.CompensationReceiptRequired
	failing := stepkindtest.NewNoopKind("contract-fail", "v1")
	undo := stepkindtest.NewNoopKind("contract-undo", "v1")
	undo.SpecValue.Effects = graph.EffectSet{graph.EffectMaterialize}
	kinds := stepkind.NewRegistry()
	for _, kind := range []stepkind.StepKind{effect, failing, undo} {
		if err := kinds.Register(kind); err != nil {
			t.Fatal(err)
		}
	}
	if findings := compile.ValidatePlan(t.Context(), plan, compile.ValidationOptions{StepKinds: kinds}); len(findings) != 0 {
		t.Fatalf("ValidatePlan = %#v", findings)
	}

	suite := executableContractSuite(t, plan, kinds, values.ValueSet{}, values.ValueSet{})
	contractCase := &suite.Cases[0]
	contractCase.Name = "compensation-before-terminal-failure"
	contractCase.ExpectedOutputs = nil
	contractCase.ExpectedError = &ContractExpectedError{Code: "fixture_failed", Message: "forward failure"}
	for index := range contractCase.Mocks {
		mock := &contractCase.Mocks[index]
		mock.ExpectedInputs = values.ValueSet{}
		switch mock.NodeID {
		case "a-effect":
			mock.Results = []ContractMockResult{{Outputs: values.ValueSet{}, Compensation: &stepkind.CompensationReceipt{Operation: "fixture.contract.effect", Values: values.ValueSet{}}}}
		case "z-fail":
			mock.Results = []ContractMockResult{{Failure: &stepkind.ExecutionError{Code: "fixture_failed", Message: "forward failure", Classification: stepkind.RetryPermanent}}}
		case "undo":
			mock.Results = []ContractMockResult{{Outputs: values.ValueSet{}}}
		}
	}
	canonical, _, err := canonicalContractSuite(suite)
	if err != nil {
		t.Fatal(err)
	}
	if validationErr := validateContractSuiteForPlan(plan, kinds, canonical, false); validationErr != nil {
		t.Fatal(validationErr)
	}
	report, err := newCanonicalContractRunner(compile.DependencyOptions{}, verification.NewDefaultRegistry()).Execute(t.Context(), plan, kinds, canonical, 2)
	if err != nil || !report.Passed || len(report.Cases) != 1 || report.Cases[0].Failure == nil || report.Cases[0].Failure.Code != "fixture_failed" || !reflect.DeepEqual(report.Cases[0].Effects, graph.EffectSet{graph.EffectCompute, graph.EffectMaterialize}) {
		t.Fatalf("compensation contract report = %#v, %v", report, err)
	}
}

func TestContractRegistrationFailsClosedForMutableFailedForgedAndUnauthorizedInputs(t *testing.T) {
	service, ref, _ := newContractServiceFixture(t, nil, func(_ context.Context, request NamespaceAuthorization) error {
		if request.Namespace != "allowed" {
			return errors.New("denied")
		}
		return nil
	})
	validation, err := service.Validate(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	suite := passingContractSuite(t, service, validation.Plan)
	_, suiteJSON, _ := canonicalContractSuite(suite)
	expectedOutputDigest, err := values.DigestValueSet(suite.Cases[0].ExpectedOutputs)
	if err != nil {
		t.Fatal(err)
	}
	report, err := service.sealReport(t.Context(), ContractTestReport{
		SchemaVersion: ContractSuiteSchemaVersion, PlanDigest: validation.Plan.Digest,
		SuiteDigest: values.SHA256Digest(suiteJSON), Passed: true,
		Cases: []ContractCaseResult{{Name: "echo", Passed: true, OutputDigest: expectedOutputDigest, Effects: graph.EffectSet{graph.EffectCompute}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	mutable := ref
	mutable.Digest = ""
	if _, err := service.Register(t.Context(), RegisterWorkflowRequest{Definition: mutable, Namespace: "allowed", Principal: "p", Report: report}); !errors.Is(err, ErrInvalidContractService) {
		t.Fatalf("mutable registration = %v", err)
	}
	failed := report
	failed.Passed = false
	if _, err := service.Register(t.Context(), RegisterWorkflowRequest{Definition: ref, Namespace: "allowed", Principal: "p", Report: failed}); !errors.Is(err, ErrContractTestFailed) {
		t.Fatalf("failed report registration = %v", err)
	}
	forged := report
	forged.PlanDigest = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if _, err := service.Register(t.Context(), RegisterWorkflowRequest{Definition: ref, Namespace: "allowed", Principal: "p", Report: forged}); !errors.Is(err, ErrContractTestFailed) {
		t.Fatalf("forged report registration = %v", err)
	}
	if _, err := service.Register(t.Context(), RegisterWorkflowRequest{Definition: ref, Namespace: "denied", Principal: "p", Report: report}); !errors.Is(err, ErrNamespaceUnauthorized) {
		t.Fatalf("unauthorized registration = %v", err)
	}
}

func TestNamespaceRequestedAuthorizationPrecedesSensitiveWork(t *testing.T) {
	service, ref, _ := newContractServiceFixture(t, nil, func(context.Context, NamespaceAuthorization) error { return nil })
	suite, report, record := registerQualifiedContractFixture(t, service, ref)
	definitions := &contractDefinitionSpy{delegate: service.definitions}
	catalog := &workflowCatalogSpy{delegate: service.catalog}
	attestor := &contractAttestorSpy{delegate: service.attestor}
	service.definitions, service.catalog, service.attestor = definitions, catalog, attestor
	denied := errors.New("requested scope denied")
	var requests []NamespaceAuthorization
	service.authorizer = NamespaceAuthorizerFunc(func(_ context.Context, request NamespaceAuthorization) error {
		requests = append(requests, request)
		if request.Stage != NamespaceAuthorizationRequested || request.Authority != "" || request.Digest != "" {
			t.Fatalf("authorization request = %#v", request)
		}
		return denied
	})
	query := hadronregistry.WorkflowQuery{Name: record.Name, Version: record.Version, Digest: record.Digest}
	operations := []struct {
		name string
		op   NamespaceOperation
		run  func() error
	}{
		{name: "register", op: NamespaceRegister, run: func() error {
			_, err := service.Register(t.Context(), RegisterWorkflowRequest{Definition: ref, Namespace: "team", Principal: "principal:test", Report: report})
			return err
		}},
		{name: "pin", op: NamespacePin, run: func() error { _, err := service.Pin(t.Context(), query, "principal:test"); return err }},
		{name: "unpin", op: NamespaceUnpin, run: func() error { return service.Unpin(t.Context(), record.Name, "principal:test") }},
		{name: "publish", op: NamespacePublish, run: func() error { _, err := service.Publish(t.Context(), query, "principal:test"); return err }},
		{name: "inspect", op: NamespaceInspect, run: func() error { _, err := service.Inspect(t.Context(), query, "principal:test"); return err }},
		{name: "search", op: NamespaceSearch, run: func() error { _, err := service.Search(t.Context(), "team", "contract", "principal:test"); return err }},
		{name: "resolve", op: NamespaceResolve, run: func() error { _, err := service.Resolve(t.Context(), query, "principal:test"); return err }},
		{name: "package", op: NamespacePackage, run: func() error {
			_, err := service.Package(t.Context(), query, suite, report, "principal:test")
			return err
		}},
	}
	for _, test := range operations {
		t.Run(test.name, func(t *testing.T) {
			definitions.reset()
			catalog.reset()
			attestor.reset()
			requests = nil
			if err := test.run(); !errors.Is(err, ErrNamespaceUnauthorized) || !errors.Is(err, denied) {
				t.Fatalf("operation error = %v", err)
			}
			if definitions.calls != 0 || catalog.calls != 0 || attestor.calls != 0 {
				t.Fatalf("work before requested authorization: definitions=%d catalog=%d attestor=%d", definitions.calls, catalog.calls, attestor.calls)
			}
			if len(requests) != 1 || requests[0].Operation != test.op || requests[0].Stage != NamespaceAuthorizationRequested {
				t.Fatalf("authorization requests = %#v", requests)
			}
		})
	}

	requests = nil
	_, existingErr := service.Inspect(t.Context(), query, "principal:test")
	_, missingErr := service.Inspect(t.Context(), hadronregistry.WorkflowQuery{Name: "team/missing", Version: "v1"}, "principal:test")
	if !errors.Is(existingErr, ErrNamespaceUnauthorized) || !errors.Is(missingErr, ErrNamespaceUnauthorized) || existingErr.Error() != missingErr.Error() {
		t.Fatalf("existing/missing denial = %v / %v", existingErr, missingErr)
	}
}

func TestNamespaceResolvedAuthorizationPrecedesMutationAndDataReturn(t *testing.T) {
	service, ref, _ := newContractServiceFixture(t, nil, func(context.Context, NamespaceAuthorization) error { return nil })
	suite, report, record := registerQualifiedContractFixture(t, service, ref)
	query := hadronregistry.WorkflowQuery{Name: record.Name, Version: record.Version, Digest: record.Digest}
	if _, err := service.catalog.PinWorkflow(t.Context(), query); err != nil {
		t.Fatal(err)
	}
	definitions := &contractDefinitionSpy{delegate: service.definitions}
	catalog := &workflowCatalogSpy{delegate: service.catalog}
	attestor := &contractAttestorSpy{delegate: service.attestor}
	service.definitions, service.catalog, service.attestor = definitions, catalog, attestor
	denied := errors.New("resolved definition denied")
	service.authorizer = NamespaceAuthorizerFunc(func(_ context.Context, request NamespaceAuthorization) error {
		if request.Stage == NamespaceAuthorizationRequested {
			return nil
		}
		if request.Stage != NamespaceAuthorizationResolved || request.Namespace != record.Namespace || request.Name != record.Name ||
			request.Authority != record.Authority || request.Digest != record.Digest {
			t.Fatalf("resolved authorization request = %#v", request)
		}
		return denied
	})

	assertDenied := func(name string, result any, err error) {
		t.Helper()
		if !errors.Is(err, ErrNamespaceUnauthorized) || !errors.Is(err, denied) {
			t.Fatalf("%s error = %v", name, err)
		}
		if !reflect.ValueOf(result).IsZero() {
			t.Fatalf("%s returned data = %#v", name, result)
		}
		if catalog.mutations != 0 {
			t.Fatalf("%s mutations = %d", name, catalog.mutations)
		}
	}

	returnedRecord, err := service.Pin(t.Context(), query, "principal:test")
	assertDenied("pin", returnedRecord, err)
	catalog.reset()
	if unpinErr := service.Unpin(t.Context(), record.Name, "principal:test"); !errors.Is(unpinErr, ErrNamespaceUnauthorized) || catalog.mutations != 0 {
		t.Fatalf("unpin error/mutations = %v / %d", unpinErr, catalog.mutations)
	}
	catalog.reset()
	returnedRecord, err = service.Publish(t.Context(), query, "principal:test")
	assertDenied("publish", returnedRecord, err)
	catalog.reset()
	returnedRecord, err = service.Inspect(t.Context(), query, "principal:test")
	assertDenied("inspect", returnedRecord, err)
	catalog.reset()
	resolution, err := service.Resolve(t.Context(), query, "principal:test")
	assertDenied("resolve", resolution, err)
	catalog.reset()
	search, err := service.Search(t.Context(), "team", "contract", "principal:test")
	assertDenied("search", search, err)
	catalog.reset()
	archive, err := service.Package(t.Context(), query, suite, report, "principal:test")
	assertDenied("package", archive, err)
	if attestor.calls != 0 {
		t.Fatalf("package verified a report before resolved authorization: %d calls", attestor.calls)
	}
	catalog.reset()
	attestor.reset()
	definitions.reset()
	returnedRecord, err = service.Register(t.Context(), RegisterWorkflowRequest{
		Definition: ref, Namespace: "team", Principal: "principal:test", Report: report,
	})
	assertDenied("register", returnedRecord, err)
	if attestor.calls != 0 {
		t.Fatalf("register verified a report before resolved authorization: %d calls", attestor.calls)
	}
}

func TestNamespaceRequestedIdentityValidationPrecedesAuthorizer(t *testing.T) {
	service, _, _ := newContractServiceFixture(t, nil, func(context.Context, NamespaceAuthorization) error { return nil })
	catalog := &workflowCatalogSpy{delegate: service.catalog}
	service.catalog = catalog
	authorizerCalls := 0
	service.authorizer = NamespaceAuthorizerFunc(func(context.Context, NamespaceAuthorization) error {
		authorizerCalls++
		return nil
	})
	for _, test := range []struct {
		name string
		run  func() error
	}{
		{name: "invalid name", run: func() error {
			_, err := service.Inspect(t.Context(), hadronregistry.WorkflowQuery{Name: "team/../secret"}, "principal:test")
			return err
		}},
		{name: "missing namespace", run: func() error {
			_, err := service.Resolve(t.Context(), hadronregistry.WorkflowQuery{Name: "unscoped"}, "principal:test")
			return err
		}},
		{name: "invalid search namespace", run: func() error {
			_, err := service.Search(t.Context(), "team/../secret", "", "principal:test")
			return err
		}},
		{name: "invalid principal", run: func() error {
			_, err := service.Inspect(t.Context(), hadronregistry.WorkflowQuery{Name: "team/contract-demo"}, "principal:\nattacker")
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); !errors.Is(err, ErrNamespaceUnauthorized) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if authorizerCalls != 0 || catalog.calls != 0 {
		t.Fatalf("invalid identity reached authorizer/catalog: %d / %d", authorizerCalls, catalog.calls)
	}
}

func TestPinAndPublishFreezeResolvedIdentityAcrossAliasChange(t *testing.T) {
	service, ref, _ := newContractServiceFixture(t, nil, func(context.Context, NamespaceAuthorization) error { return nil })
	_, _, first := registerQualifiedContractFixture(t, service, ref)
	baseCatalog := service.catalog
	second := first
	second.Version = "v2"
	second.Source = bytes.Replace(first.Source, []byte("version: v1"), []byte("version: v2"), 1)
	second.Digest = values.SHA256Digest(second.Source)
	second.PlanDigest = values.SHA256Digest([]byte("plan-v2"))
	second.Provenance.Revision = second.Version
	second.Provenance.Digest = second.Digest
	second.RegisteredAt = first.RegisteredAt.Add(time.Minute)
	second.Published = false
	if _, err := baseCatalog.RegisterWorkflow(t.Context(), second, true); err != nil {
		t.Fatal(err)
	}
	catalog := &workflowCatalogSpy{delegate: baseCatalog}
	service.catalog = catalog

	catalog.inspectHook = func() {
		if _, err := baseCatalog.RegisterWorkflow(t.Context(), first, true); err != nil {
			t.Fatalf("move current alias during pin: %v", err)
		}
	}
	pinned, err := service.Pin(t.Context(), hadronregistry.WorkflowQuery{Name: first.Name}, "principal:test")
	if err != nil || pinned.Version != second.Version || pinned.Digest != second.Digest {
		t.Fatalf("Pin(alias changed) = %#v, %v", pinned, err)
	}
	if len(catalog.pinQueries) != 1 || catalog.pinQueries[0] != (hadronregistry.WorkflowQuery{Name: second.Name, Version: second.Version, Digest: second.Digest}) {
		t.Fatalf("pin mutation query = %#v", catalog.pinQueries)
	}
	if resolved, resolveErr := baseCatalog.ResolvePinnedWorkflow(t.Context(), first.Name); resolveErr != nil || resolved.Record.Digest != second.Digest {
		t.Fatalf("resolved pin = %#v, %v", resolved, resolveErr)
	}

	if _, pinErr := baseCatalog.PinWorkflow(t.Context(), hadronregistry.WorkflowQuery{Name: first.Name, Version: first.Version, Digest: first.Digest}); pinErr != nil {
		t.Fatal(pinErr)
	}
	catalog.inspectHook = func() {
		if _, registerErr := baseCatalog.RegisterWorkflow(t.Context(), second, true); registerErr != nil {
			t.Fatalf("move current alias during publish: %v", registerErr)
		}
	}
	published, err := service.Publish(t.Context(), hadronregistry.WorkflowQuery{Name: first.Name}, "principal:test")
	if err != nil || published.Version != first.Version || published.Digest != first.Digest || !published.Published {
		t.Fatalf("Publish(alias changed) = %#v, %v", published, err)
	}
	if len(catalog.publishQueries) != 1 || catalog.publishQueries[0] != (hadronregistry.WorkflowQuery{Name: first.Name, Version: first.Version, Digest: first.Digest}) {
		t.Fatalf("publish mutation query = %#v", catalog.publishQueries)
	}
	if inspected, inspectErr := baseCatalog.InspectWorkflow(t.Context(), hadronregistry.WorkflowQuery{Name: second.Name, Version: second.Version, Digest: second.Digest}); inspectErr != nil || inspected.Published {
		t.Fatalf("unauthorized alias target was published = %#v, %v", inspected, inspectErr)
	}
}

func TestRegistrationClockFailsClosedBeforeCatalogMutation(t *testing.T) {
	service, ref, _ := newContractServiceFixture(t, nil, func(context.Context, NamespaceAuthorization) error { return nil })
	validation, err := service.Validate(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	suite := passingContractSuite(t, service, validation.Plan)
	report, err := service.ExecuteContractTests(t.Context(), ref, suite)
	if err != nil {
		t.Fatal(err)
	}
	catalog := &workflowCatalogSpy{delegate: service.catalog}
	service.catalog = catalog
	request := RegisterWorkflowRequest{Definition: ref, Namespace: "team", Principal: "principal:test", Report: report}
	service.now = func() time.Time { return time.Time{} }
	if _, err := service.Register(t.Context(), request); !errors.Is(err, ErrInvalidContractService) || catalog.mutations != 0 {
		t.Fatalf("zero clock error/mutations = %v / %d", err, catalog.mutations)
	}
	registeredAt := time.Date(2026, time.August, 24, 14, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return registeredAt }
	if _, err := service.Register(t.Context(), request); err != nil || catalog.mutations != 1 {
		t.Fatalf("first registration error/mutations = %v / %d", err, catalog.mutations)
	}
	for _, invalid := range []time.Time{registeredAt, registeredAt.Add(-time.Nanosecond)} {
		service.now = func() time.Time { return invalid }
		if _, err := service.Register(t.Context(), request); !errors.Is(err, ErrInvalidContractService) || catalog.mutations != 1 {
			t.Fatalf("non-monotonic clock %s error/mutations = %v / %d", invalid, err, catalog.mutations)
		}
	}
}

func TestContractSuiteValidationFailsClosed(t *testing.T) {
	service, ref, _ := newContractServiceFixture(t, nil, func(context.Context, NamespaceAuthorization) error { return nil })
	validation, err := service.Validate(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	base := passingContractSuite(t, service, validation.Plan)

	tests := map[string]func(*WorkflowContractSuite){
		"outputs and error": func(suite *WorkflowContractSuite) {
			suite.Cases[0].ExpectedError = &ContractExpectedError{Code: "unexpected"}
		},
		"neither outputs nor error": func(suite *WorkflowContractSuite) {
			suite.Cases[0].ExpectedOutputs = nil
		},
		"missing error code": func(suite *WorkflowContractSuite) {
			suite.Cases[0].ExpectedOutputs = nil
			suite.Cases[0].ExpectedError = &ContractExpectedError{}
		},
		"duplicate effect": func(suite *WorkflowContractSuite) {
			suite.Cases[0].ExpectedEffects = graph.EffectSet{graph.EffectCompute, graph.EffectCompute}
		},
		"undeclared effect": func(suite *WorkflowContractSuite) {
			suite.Cases[0].ExpectedEffects = graph.EffectSet{graph.EffectRead}
		},
		"duplicate mock": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks = append(suite.Cases[0].Mocks, suite.Cases[0].Mocks[0])
		},
		"unknown mock node": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].NodeID = "unknown"
		},
		"wrong kind": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].Kind = "cmd"
		},
		"wrong kind version": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].KindVersion = "v2"
		},
		"wrong schema": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].InputSchema = graph.Schema{"type": "array"}
		},
		"wrong config": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].ExpectedConfig = graph.Config{"result": "inputs.other"}
		},
		"no mock result": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].Results = nil
		},
		"multiple mock results": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].Results = append(suite.Cases[0].Mocks[0].Results, suite.Cases[0].Mocks[0].Results[0])
		},
		"attempt beyond retry limit": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].Results[0].Attempt = 2
		},
		"iteration on ordinary node": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].Results[0].Iteration = runtime.FanOutIteration(0)
		},
		"call from wrong node": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].Results[0].Calls = []ContractToolCall{{
				NodeID: "other", Kind: "tool", Name: "literal", Arguments: map[string]any{}, Effect: graph.EffectCompute, Outcome: verification.ActivitySucceeded,
			}}
		},
		"undeclared call effect": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].Results[0].Calls = []ContractToolCall{{
				NodeID: "echo", Kind: "tool", Name: "literal", Arguments: map[string]any{}, Effect: graph.EffectMutate, Outcome: verification.ActivitySucceeded,
			}}
		},
		"invalid call name": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].Results[0].Calls = []ContractToolCall{{
				NodeID: "echo", Kind: "tool", Arguments: map[string]any{}, Effect: graph.EffectCompute, Outcome: verification.ActivitySucceeded,
			}}
		},
		"invalid call arguments": func(suite *WorkflowContractSuite) {
			suite.Cases[0].Mocks[0].Results[0].Calls = []ContractToolCall{{
				NodeID: "echo", Kind: "tool", Name: "literal", Arguments: map[string]any{"invalid": make(chan int)}, Effect: graph.EffectCompute, Outcome: verification.ActivitySucceeded,
			}}
		},
		"unknown expected call node": func(suite *WorkflowContractSuite) {
			suite.Cases[0].ExpectedCalls = []ContractToolCall{{
				NodeID: "unknown", Kind: "tool", Name: "literal", Arguments: map[string]any{}, Effect: graph.EffectCompute, Outcome: verification.ActivitySucceeded,
			}}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			suite := cloneContractSuiteForTest(t, base)
			mutate(&suite)
			if _, executeErr := service.ExecuteContractTests(t.Context(), ref, suite); !errors.Is(executeErr, ErrInvalidContractService) {
				t.Fatalf("ExecuteContractTests() = %v", executeErr)
			}
		})
	}

	schemaFailure := cloneContractSuiteForTest(t, base)
	schemaFailure.Cases[0].Mocks[0].Results[0].Outputs["result"] = contractInline(t, "mock", "result", true)
	schemaFailure.Cases[0].ExpectedOutputs = nil
	schemaFailure.Cases[0].ExpectedError = &ContractExpectedError{Code: "step_result_invalid"}
	if report, executeErr := service.ExecuteContractTests(t.Context(), ref, schemaFailure); executeErr != nil || !report.Passed {
		t.Fatalf("runtime node-output schema failure = %#v, %v", report, executeErr)
	}

	subset := cloneContractSuiteForTest(t, base)
	subset.Cases[0].ExpectedEffects = graph.EffectSet{}
	if report, executeErr := service.ExecuteContractTests(t.Context(), ref, subset); executeErr != nil || report.Passed {
		t.Fatalf("effect subset report = %#v, %v", report, executeErr)
	}
	service.policy.RequireEffectCoverage = true
	if _, executeErr := service.ExecuteContractTests(t.Context(), ref, subset); !errors.Is(executeErr, ErrInvalidContractService) {
		t.Fatalf("required effect coverage = %v", executeErr)
	}

	plan := *validation.Plan
	plan.Graph.Nodes = append([]graph.Node(nil), validation.Plan.Graph.Nodes...)
	plan.Graph.Nodes[0].Effects = append(plan.Graph.Nodes[0].Effects, graph.EffectRead)
	second := validation.Plan.Graph.Nodes[0]
	second.ID = "summary"
	plan.Graph.Nodes = append(plan.Graph.Nodes, second)
	nodeSuite, err := scaffoldFor(&plan, service.kinds)
	if err != nil {
		t.Fatal(err)
	}
	nodeSuite.Cases[0].Editable = false
	for index := range nodeSuite.Cases[0].Mocks {
		nodeSuite.Cases[0].Mocks[index].ExpectedInputsEditable = false
		if nodeSuite.Cases[0].Mocks[index].NodeID == "summary" {
			nodeSuite.Cases[0].Mocks[index].Results[0].Calls = []ContractToolCall{{
				NodeID: "summary", Kind: "tool", Name: "literal", Arguments: map[string]any{}, Effect: graph.EffectRead, Outcome: verification.ActivitySucceeded,
			}}
		}
	}
	canonical, _, err := canonicalContractSuite(nodeSuite)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateContractSuiteForPlan(&plan, service.kinds, canonical, false); !errors.Is(err, ErrInvalidContractService) {
		t.Fatalf("node-specific effect validation = %v", err)
	}
}

func TestContractSuiteAdmissionBoundsBeforeRunnerAndAttestor(t *testing.T) {
	runnerCalls := 0
	service, ref, _ := newContractServiceFixture(t, contractRunnerFunc(func(context.Context, *compile.ExecutionPlan, stepkind.Registry, WorkflowContractSuite, int) (ContractTestReport, error) {
		runnerCalls++
		return ContractTestReport{}, errors.New("runner must not be called")
	}), func(context.Context, NamespaceAuthorization) error { return nil })
	validation, err := service.Validate(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	base := passingContractSuite(t, service, validation.Plan)
	attestor := &contractAttestorSpy{delegate: service.attestor}
	service.attestor = attestor

	overCases := cloneContractSuiteForTest(t, base)
	overCases.Cases = make([]WorkflowContractCase, maxContractSuiteCases+1)
	for index := range overCases.Cases {
		overCases.Cases[index] = base.Cases[0]
		overCases.Cases[index].Name = fmt.Sprintf("case-%d", index)
	}
	overResults := cloneContractSuiteForTest(t, base)
	overResults.Cases[0].Mocks[0].Results = make([]ContractMockResult, maxContractMockResults+1)
	overCalls := cloneContractSuiteForTest(t, base)
	overCalls.Cases[0].ExpectedCalls = make([]ContractToolCall, maxContractToolCalls+1)
	overBytes := cloneContractSuiteForTest(t, base)
	overBytes.Cases[0].Name = strings.Repeat("x", maxContractSuiteBytes)

	for _, test := range []struct {
		name  string
		suite WorkflowContractSuite
	}{
		{name: "cases", suite: overCases},
		{name: "mock results", suite: overResults},
		{name: "tool calls", suite: overCalls},
		{name: "encoded bytes", suite: overBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, executeErr := service.ExecuteContractTests(t.Context(), ref, test.suite); !errors.Is(executeErr, ErrInvalidContractService) {
				t.Fatalf("ExecuteContractTests() = %v", executeErr)
			}
		})
	}
	if runnerCalls != 0 || attestor.calls != 0 {
		t.Fatalf("oversized suites reached runner/attestor: %d / %d", runnerCalls, attestor.calls)
	}

	boundary := WorkflowContractSuite{Cases: make([]WorkflowContractCase, maxContractSuiteCases)}
	boundary.Cases[0].Mocks = []ContractExecutorMock{{Results: make([]ContractMockResult, maxContractMockResults)}}
	boundary.Cases[0].ExpectedCalls = make([]ContractToolCall, maxContractToolCalls)
	if err := validateContractSuiteAdmission(boundary); err != nil {
		t.Fatalf("exact structural limits rejected: %v", err)
	}
	mixedCalls := WorkflowContractSuite{Cases: []WorkflowContractCase{{
		ExpectedCalls: make([]ContractToolCall, maxContractToolCalls-1),
		Mocks:         []ContractExecutorMock{{Results: []ContractMockResult{{Calls: make([]ContractToolCall, 2)}}}},
	}}}
	if err := validateContractSuiteAdmission(mixedCalls); !errors.Is(err, ErrInvalidContractService) {
		t.Fatalf("combined tool-call over-limit accepted: %v", err)
	}
	if err := validateContractSuiteBytes(make([]byte, maxContractSuiteBytes)); err != nil {
		t.Fatalf("exact byte limit rejected: %v", err)
	}
	if err := validateContractSuiteBytes(make([]byte, maxContractSuiteBytes+1)); !errors.Is(err, ErrInvalidContractService) {
		t.Fatalf("byte over-limit accepted: %v", err)
	}

	if _, err := NewContractRegistrationService(ContractRegistrationOptions{
		Definitions: service.definitions, StepKinds: service.kinds, Catalog: service.catalog,
		Authorizer: service.authorizer, Attestor: service.attestor, Runner: service.runner,
		Policy: ContractTestPolicy{MinimumCases: maxContractSuiteCases + 1, Repetitions: 1},
	}); !errors.Is(err, ErrInvalidContractService) {
		t.Fatalf("MinimumCases above admission cap = %v", err)
	}
}

func TestContractSuiteCanonicalizationNeverMutatesCallerOwnership(t *testing.T) {
	service, ref, _ := newContractServiceFixture(t, nil, func(context.Context, NamespaceAuthorization) error { return nil })
	validation, err := service.Validate(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	base := passingContractSuite(t, service, validation.Plan)

	unsorted := cloneContractSuiteForTest(t, base)
	late := unsorted.Cases[0].Mocks[0]
	late.NodeID = "z-node"
	late.Results = append([]ContractMockResult(nil), late.Results[0], late.Results[0])
	late.Results[0].Attempt = 2
	late.Results[1].Attempt = 0
	early := late
	early.NodeID = "a-node"
	early.Results = append([]ContractMockResult(nil), late.Results...)
	unsorted.Cases[0].Mocks = []ContractExecutorMock{late, early}
	snapshot, err := canonicalJSON(unsorted)
	if err != nil {
		t.Fatal(err)
	}
	canonical, _, err := canonicalContractSuite(unsorted)
	if err != nil {
		t.Fatal(err)
	}
	if canonical.Cases[0].Mocks[0].NodeID != "a-node" || canonical.Cases[0].Mocks[0].Results[0].Attempt != 1 ||
		unsorted.Cases[0].Mocks[0].NodeID != "z-node" || unsorted.Cases[0].Mocks[0].Results[1].Attempt != 0 {
		t.Fatalf("canonical/caller ordering and defaults = %#v / %#v", canonical.Cases[0].Mocks, unsorted.Cases[0].Mocks)
	}
	assertContractSuiteBytesEqual(t, unsorted, snapshot)

	rejected := cloneContractSuiteForTest(t, unsorted)
	rejected.Cases[0].ExpectedCalls = []ContractToolCall{{
		NodeID: "missing", Kind: "tool", Name: "literal", Arguments: map[string]any{},
		Effect: graph.EffectCompute, Outcome: verification.ActivitySucceeded,
	}}
	rejectedSnapshot, err := canonicalJSON(rejected)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, canonicalErr := canonicalContractSuite(rejected); !errors.Is(canonicalErr, ErrInvalidContractService) {
		t.Fatalf("rejected canonicalization = %v", canonicalErr)
	}
	assertContractSuiteBytesEqual(t, rejected, rejectedSnapshot)
	if _, executeErr := service.ExecuteContractTests(t.Context(), ref, rejected); !errors.Is(executeErr, ErrInvalidContractService) {
		t.Fatalf("rejected ExecuteContractTests = %v", executeErr)
	}
	assertContractSuiteBytesEqual(t, rejected, rejectedSnapshot)

	executable := cloneContractSuiteForTest(t, base)
	executeSnapshot, err := canonicalJSON(executable)
	if err != nil {
		t.Fatal(err)
	}
	if executable.Cases[0].Mocks[0].Results[0].Attempt != 0 {
		t.Fatal("fixture does not exercise private attempt defaulting")
	}
	if _, err := service.ExecuteContractTests(t.Context(), ref, executable); err != nil {
		t.Fatal(err)
	}
	assertContractSuiteBytesEqual(t, executable, executeSnapshot)

	var wait sync.WaitGroup
	errorsSeen := make(chan error, 16)
	for range 16 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, _, canonicalErr := canonicalContractSuite(unsorted)
			errorsSeen <- canonicalErr
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for canonicalErr := range errorsSeen {
		if canonicalErr != nil {
			t.Fatal(canonicalErr)
		}
	}
	assertContractSuiteBytesEqual(t, unsorted, snapshot)
}

func TestContractReportAdmissionBoundsBeforeSensitiveWork(t *testing.T) {
	service, ref, _ := newContractServiceFixture(t, nil, func(context.Context, NamespaceAuthorization) error { return nil })
	suite, report, record := registerQualifiedContractFixture(t, service, ref)
	query := hadronregistry.WorkflowQuery{Name: record.Name, Version: record.Version, Digest: record.Digest}
	definitions := &contractDefinitionSpy{delegate: service.definitions}
	catalog := &workflowCatalogSpy{delegate: service.catalog}
	attestor := &contractAttestorSpy{delegate: service.attestor}
	service.definitions, service.catalog, service.attestor = definitions, catalog, attestor

	overLimit := report
	overLimit.Attestation = strings.Repeat("TOP_SECRET_ATTESTATION", maxContractAttestationBytes)
	if _, err := service.Register(t.Context(), RegisterWorkflowRequest{
		Definition: ref, Namespace: "team", Principal: "principal:test", Report: overLimit,
	}); !errors.Is(err, ErrContractTestFailed) || strings.Contains(err.Error(), "TOP_SECRET") {
		t.Fatalf("oversized registration report = %v", err)
	}
	if definitions.calls != 0 || catalog.calls != 0 || attestor.calls != 0 {
		t.Fatalf("oversized registration reached sensitive work: definitions=%d catalog=%d attestor=%d", definitions.calls, catalog.calls, attestor.calls)
	}
	definitions.reset()
	catalog.reset()
	attestor.reset()
	archive, err := service.Package(t.Context(), query, suite, overLimit, "principal:test")
	if !errors.Is(err, ErrContractTestFailed) || !reflect.ValueOf(archive).IsZero() || strings.Contains(err.Error(), "TOP_SECRET") {
		t.Fatalf("oversized package report = %#v, %v", archive, err)
	}
	if definitions.calls != 0 || catalog.calls != 0 || attestor.calls != 0 {
		t.Fatalf("oversized package reached sensitive work: definitions=%d catalog=%d attestor=%d", definitions.calls, catalog.calls, attestor.calls)
	}

	trustedOversize := report
	trustedOversize.Digest, trustedOversize.Attestation = "", ""
	trustedOversize.Cases = append([]ContractCaseResult(nil), report.Cases...)
	trustedOversize.Cases[0].Calls = make([]ContractToolCall, maxContractReportCalls+1)
	if _, err := service.sealReport(t.Context(), trustedOversize); !errors.Is(err, ErrContractTestFailed) {
		t.Fatalf("sealReport(oversized runner result) = %v", err)
	}
	if attestor.calls != 0 {
		t.Fatalf("oversized trusted report reached attestor: %d calls", attestor.calls)
	}

	boundaryAttestation := report
	boundaryAttestation.Attestation = strings.Repeat("a", maxContractAttestationBytes)
	if err := validateContractReportAdmission(boundaryAttestation); err != nil {
		t.Fatalf("exact attestation limit rejected: %v", err)
	}
	boundaryIdentity := report
	boundaryIdentity.Cases = append([]ContractCaseResult(nil), report.Cases...)
	boundaryIdentity.Cases[0].Name = strings.Repeat("n", maxContractReportIdentityBytes)
	if err := validateContractReportAdmission(boundaryIdentity); err != nil {
		t.Fatalf("exact identity limit rejected: %v", err)
	}
	boundaryMessage := report
	boundaryMessage.Cases = append([]ContractCaseResult(nil), report.Cases...)
	boundaryMessage.Cases[0].Message = strings.Repeat("m", maxContractReportMessageBytes)
	if err := validateContractReportAdmission(boundaryMessage); err != nil {
		t.Fatalf("exact message limit rejected: %v", err)
	}
	boundaryCases := report
	boundaryCases.Cases = make([]ContractCaseResult, maxContractReportCases)
	for index := range boundaryCases.Cases {
		boundaryCases.Cases[index] = report.Cases[0]
		boundaryCases.Cases[index].Name = fmt.Sprintf("case-%d", index)
	}
	if err := validateContractReportAdmission(boundaryCases); err != nil {
		t.Fatalf("exact case limit rejected: %v", err)
	}
	boundaryCalls := report
	boundaryCalls.Cases = append([]ContractCaseResult(nil), report.Cases...)
	call := ContractToolCall{
		NodeID: "echo", Kind: "tool", Name: "literal", Arguments: map[string]any{},
		Effect: graph.EffectCompute, Outcome: verification.ActivitySucceeded,
	}
	boundaryCalls.Cases[0].Calls = make([]ContractToolCall, maxContractReportCalls)
	for index := range boundaryCalls.Cases[0].Calls {
		boundaryCalls.Cases[0].Calls[index] = call
	}
	if err := validateContractReportAdmission(boundaryCalls); err != nil {
		t.Fatalf("exact call limit rejected: %v", err)
	}
	boundaryEffects := report
	boundaryEffects.Cases = append([]ContractCaseResult(nil), report.Cases...)
	boundaryEffects.Cases[0].Effects = make(graph.EffectSet, maxContractReportEffects)
	for index := range boundaryEffects.Cases[0].Effects {
		boundaryEffects.Cases[0].Effects[index] = graph.EffectCompute
	}
	if err := validateContractReportAdmission(boundaryEffects); err != nil {
		t.Fatalf("exact effect limit rejected: %v", err)
	}
	if err := validateContractReportBytes(make([]byte, maxContractReportBytes)); err != nil {
		t.Fatalf("exact report byte limit rejected: %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*ContractTestReport)
	}{
		{name: "cases", mutate: func(input *ContractTestReport) { input.Cases = make([]ContractCaseResult, maxContractReportCases+1) }},
		{name: "calls", mutate: func(input *ContractTestReport) {
			input.Cases = append([]ContractCaseResult(nil), report.Cases...)
			input.Cases[0].Calls = make([]ContractToolCall, maxContractReportCalls+1)
		}},
		{name: "effects", mutate: func(input *ContractTestReport) {
			input.Cases = append([]ContractCaseResult(nil), report.Cases...)
			input.Cases[0].Effects = make(graph.EffectSet, maxContractReportEffects+1)
		}},
		{name: "attestation", mutate: func(input *ContractTestReport) {
			input.Attestation = strings.Repeat("a", maxContractAttestationBytes+1)
		}},
		{name: "identity", mutate: func(input *ContractTestReport) {
			input.Cases = append([]ContractCaseResult(nil), report.Cases...)
			input.Cases[0].Name = strings.Repeat("n", maxContractReportIdentityBytes+1)
		}},
		{name: "message", mutate: func(input *ContractTestReport) {
			input.Cases = append([]ContractCaseResult(nil), report.Cases...)
			input.Cases[0].Message = strings.Repeat("m", maxContractReportMessageBytes+1)
		}},
		{name: "encoded bytes", mutate: func(input *ContractTestReport) {
			input.Cases = append([]ContractCaseResult(nil), report.Cases...)
			oversizedCall := call
			oversizedCall.Arguments = map[string]any{"payload": strings.Repeat("x", maxContractReportBytes)}
			input.Cases[0].Calls = []ContractToolCall{oversizedCall}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := report
			test.mutate(&candidate)
			if admissionErr := validateContractReportAdmission(candidate); !errors.Is(admissionErr, ErrContractTestFailed) {
				t.Fatalf("over-limit report accepted: %v", admissionErr)
			}
		})
	}
	if err := validateContractReportBytes(make([]byte, maxContractReportBytes+1)); !errors.Is(err, ErrContractTestFailed) {
		t.Fatalf("report byte over-limit accepted: %v", err)
	}
}

func TestSealReportBoundsAttestorOutput(t *testing.T) {
	service, ref, _ := newContractServiceFixture(t, nil, func(context.Context, NamespaceAuthorization) error { return nil })
	validation, err := service.Validate(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	suite := passingContractSuite(t, service, validation.Plan)

	exact := strings.Repeat("a", maxContractAttestationBytes)
	service.attestor = fixedContractAttestor{attestation: exact}
	report, err := service.ExecuteContractTests(t.Context(), ref, suite)
	if err != nil {
		t.Fatalf("exact-limit attestation rejected: %v", err)
	}
	if report.Attestation != exact {
		t.Fatalf("exact-limit attestation length = %d", len(report.Attestation))
	}

	const marker = "RAW_ATTESTOR_SECRET"
	service.attestor = fixedContractAttestor{attestation: strings.Repeat(marker, maxContractAttestationBytes)}
	report, err = service.ExecuteContractTests(t.Context(), ref, suite)
	if !errors.Is(err, ErrContractTestFailed) || !reflect.ValueOf(report).IsZero() {
		t.Fatalf("over-limit attestation result = %#v, %v", report, err)
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("over-limit attestation leaked through error: %v", err)
	}
}

func TestContractReportValidationPrecedesAttestation(t *testing.T) {
	service, ref, _ := newContractServiceFixture(t, nil, func(context.Context, NamespaceAuthorization) error { return nil })
	validation, err := service.Validate(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	suite := passingContractSuite(t, service, validation.Plan)
	digest, err := values.DigestValueSet(suite.Cases[0].ExpectedOutputs)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]ContractTestReport{
		"missing case": {Passed: true},
		"aggregate mismatch": {Passed: true, Cases: []ContractCaseResult{{
			Name: "echo", Passed: false, Message: "mismatch",
		}}},
		"wrong output digest": {Passed: true, Cases: []ContractCaseResult{{
			Name: "echo", Passed: true, OutputDigest: values.SHA256Digest([]byte("wrong")), Effects: graph.EffectSet{graph.EffectCompute},
		}}},
		"missing observed effects": {Passed: true, Cases: []ContractCaseResult{{
			Name: "echo", Passed: true, OutputDigest: digest,
		}}},
		"extra call": {Passed: true, Cases: []ContractCaseResult{{
			Name: "echo", Passed: true, OutputDigest: digest, Effects: graph.EffectSet{graph.EffectCompute},
			Calls: []ContractToolCall{{NodeID: "echo", Kind: "tool", Name: "literal", Arguments: map[string]any{}, Effect: graph.EffectCompute, Outcome: verification.ActivitySucceeded}},
		}}},
	}
	for name, returned := range tests {
		t.Run(name, func(t *testing.T) {
			service.runner = contractRunnerFunc(func(context.Context, *compile.ExecutionPlan, stepkind.Registry, WorkflowContractSuite, int) (ContractTestReport, error) {
				return returned, nil
			})
			if _, executeErr := service.ExecuteContractTests(t.Context(), ref, suite); !errors.Is(executeErr, ErrContractTestFailed) {
				t.Fatalf("ExecuteContractTests() = %v", executeErr)
			}
		})
	}

	service.runner = contractRunnerFunc(func(context.Context, *compile.ExecutionPlan, stepkind.Registry, WorkflowContractSuite, int) (ContractTestReport, error) {
		return ContractTestReport{Passed: true, Cases: []ContractCaseResult{{Name: "echo", Passed: true, OutputDigest: digest, Effects: graph.EffectSet{graph.EffectCompute}}}}, nil
	})
	report, err := service.ExecuteContractTests(t.Context(), ref, suite)
	if err != nil || report.Digest == "" || report.Attestation == "" {
		t.Fatalf("valid report = %#v, %v", report, err)
	}

	errorSuite := cloneContractSuiteForTest(t, suite)
	errorSuite.Cases[0].ExpectedOutputs = nil
	errorSuite.Cases[0].ExpectedError = &ContractExpectedError{Code: "expected"}
	errorSuite.Cases[0].Mocks[0].Results = []ContractMockResult{{Failure: &stepkind.ExecutionError{
		Code: "expected", Message: "expected failure", Classification: stepkind.RetryPermanent,
	}}}
	service.runner = contractRunnerFunc(func(context.Context, *compile.ExecutionPlan, stepkind.Registry, WorkflowContractSuite, int) (ContractTestReport, error) {
		return ContractTestReport{Passed: true, Cases: []ContractCaseResult{{
			Name: "echo", Passed: true, Failure: &ContractExpectedError{Code: "wrong"}, Effects: graph.EffectSet{graph.EffectCompute},
		}}}, nil
	})
	if _, err := service.ExecuteContractTests(t.Context(), ref, errorSuite); !errors.Is(err, ErrContractTestFailed) {
		t.Fatalf("wrong expected error code = %v", err)
	}
}

func TestContractScaffoldMarksDependentInputsEditable(t *testing.T) {
	service, ref, _ := newContractServiceFixture(t, nil, func(context.Context, NamespaceAuthorization) error { return nil })
	validation, err := service.Validate(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	plan := *validation.Plan
	plan.Graph.Nodes = append([]graph.Node(nil), validation.Plan.Graph.Nodes...)
	dependent := plan.Graph.Nodes[0]
	dependent.ID = "summary"
	dependent.Needs = []graph.Need{{Node: "echo", Kind: graph.EdgeData}}
	dependent.InputBindings = map[string]graph.Binding{"message": {
		Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "steps.echo.outputs.result"},
	}}
	plan.Graph.Nodes = append(plan.Graph.Nodes, dependent)
	scaffold, err := scaffoldFor(&plan, service.kinds)
	if err != nil {
		t.Fatal(err)
	}
	if len(scaffold.Cases[0].Mocks) != 2 || scaffold.Cases[0].Mocks[0].ExpectedInputsEditable ||
		!scaffold.Cases[0].Mocks[1].ExpectedInputsEditable || len(scaffold.Cases[0].Mocks[1].ExpectedInputs) != 0 {
		t.Fatalf("dependent scaffold = %#v", scaffold.Cases[0].Mocks)
	}
}

func TestCanonicalContractRunnerDrivesRetryCatchSwitchFinallyAndFanOut(t *testing.T) {
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	value := contractInline(t, "input", "value", "hello")
	output := contractInline(t, "mock", "result", "done")

	t.Run("retry", func(t *testing.T) {
		plan := compileContractPlan(t, `workflow:
  name: retry-contract
  version: v1
inputs:
  - name: value
    type: string
    required: true
steps:
  - id: flaky
    transform:
      result: inputs.value
    with:
      value: inputs.value
    outputs:
      result: {type: string}
    retry:
      attempts: 2
      backoff:
        strategy: fixed
        initial_delay: 1ms
      on: [temporary]
outputs:
  result:
    type: string
    value: steps.flaky.outputs.result
`)
		suite := executableContractSuite(t, plan, kinds, values.ValueSet{"value": value}, values.ValueSet{"result": output})
		mock := &suite.Cases[0].Mocks[0]
		mock.ExpectedInputs = values.ValueSet{"value": value}
		mock.Results = []ContractMockResult{
			{Attempt: 1, Failure: &stepkind.ExecutionError{Code: "temporary", Message: "retry", Classification: stepkind.Retryable}},
			{Attempt: 2, Outputs: values.ValueSet{"result": output}},
		}
		assertCanonicalContractPass(t, plan, kinds, suite)
	})

	t.Run("catch", func(t *testing.T) {
		plan := compileContractPlan(t, `workflow:
  name: catch-contract
  version: v1
steps:
  - id: primary
    transform: {result: "'unused'"}
    outputs:
      result: {type: string}
    catch:
      - errors: [expected]
        targets: [recover]
  - id: recover
    transform: {result: "'done'"}
    outputs:
      result: {type: string}
outputs:
  result:
    type: string
    value: steps.recover.outputs.result
`)
		suite := executableContractSuite(t, plan, kinds, values.ValueSet{}, values.ValueSet{"result": output})
		for index := range suite.Cases[0].Mocks {
			mock := &suite.Cases[0].Mocks[index]
			mock.ExpectedInputs = values.ValueSet{}
			if mock.NodeID == "primary" {
				mock.Results = []ContractMockResult{{Failure: &stepkind.ExecutionError{Code: "expected", Message: "expected failure", Classification: stepkind.RetryPermanent}}}
			} else {
				mock.Results = []ContractMockResult{{Outputs: values.ValueSet{"result": output}}}
			}
		}
		assertCanonicalContractPass(t, plan, kinds, suite)
	})

	t.Run("switch and finally", func(t *testing.T) {
		plan := compileContractPlan(t, `workflow:
  name: control-contract
  version: v1
inputs:
  - name: choose
    type: boolean
    required: true
steps:
  - id: choose
    transform: {result: "'selected'"}
    outputs:
      result: {type: string}
    switch:
      arms:
        - when: inputs.choose
          targets: [selected]
      default: [other]
  - id: selected
    transform: {result: "'done'"}
    outputs:
      result: {type: string}
  - id: other
    transform: {result: "'other'"}
    outputs:
      result: {type: string}
finally:
  - id: cleanup
    transform: {result: "'clean'"}
    outputs:
      result: {type: string}
`)
		choose := contractInline(t, "input", "choose", true)
		suite := executableContractSuite(t, plan, kinds, values.ValueSet{"choose": choose}, values.ValueSet{})
		for index := range suite.Cases[0].Mocks {
			mock := &suite.Cases[0].Mocks[index]
			mock.ExpectedInputs = values.ValueSet{}
			mock.Results = []ContractMockResult{{Outputs: values.ValueSet{"result": contractInline(t, "mock", mock.NodeID, mock.NodeID)}}}
		}
		assertCanonicalContractPass(t, plan, kinds, suite)
	})

	t.Run("finalizer with declared outputs publishes successful result", func(t *testing.T) {
		plan := compileContractPlan(t, `workflow:
  name: finalizer-output-contract
  version: v1
steps:
  - id: work
    transform: {result: "'done'"}
    outputs:
      result: {type: string}
finally:
  - id: cleanup
    transform: {result: "'clean'"}
    outputs:
      result: {type: string}
outputs:
  result:
    type: string
    value: steps.work.outputs.result
`)
		if len(plan.Graph.Outputs) != 1 {
			t.Fatalf("compiled outputs = %#v", plan.Graph.Outputs)
		}
		suite := executableContractSuite(t, plan, kinds, values.ValueSet{}, values.ValueSet{"result": output})
		for index := range suite.Cases[0].Mocks {
			mock := &suite.Cases[0].Mocks[index]
			mock.ExpectedInputs = values.ValueSet{}
			result := output
			if mock.NodeID == "cleanup" {
				result = contractInline(t, "mock", mock.NodeID, "clean")
			}
			mock.Results = []ContractMockResult{{Outputs: values.ValueSet{"result": result}}}
		}
		assertCanonicalContractPass(t, plan, kinds, suite)
	})

	t.Run("fan-out", func(t *testing.T) {
		plan := compileContractPlan(t, `workflow:
  name: fanout-contract
  version: v1
inputs:
  - name: items
    schema:
      type: array
      items: {type: string}
    required: true
steps:
  - id: fan
    for_each: inputs.items
    concurrency: 1
    transform: {result: item}
    outputs:
      result: {type: string}
`)
		items := contractInline(t, "input", "items", []any{"a", "b"})
		suite := executableContractSuite(t, plan, kinds, values.ValueSet{"items": items}, values.ValueSet{})
		mock := &suite.Cases[0].Mocks[0]
		mock.ExpectedInputs = values.ValueSet{}
		runID := runtime.RunID("contract-" + values.SHA256Digest([]byte(plan.Digest + "\x00" + suite.Cases[0].Name))[7:31])
		for index, item := range []string{"a", "b"} {
			metadata := values.Metadata{Producer: values.Producer{Kind: "fanout-item", Reference: string(runID) + "/fan"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
			itemValue, err := values.NewInline(item, metadata)
			if err != nil {
				t.Fatal(err)
			}
			indexValue, err := values.NewInline(json.Number(fmt.Sprint(index)), metadata)
			if err != nil {
				t.Fatal(err)
			}
			expectedInputs := values.ValueSet{"item": itemValue, "index": indexValue}
			mock.Results = append(mock.Results, ContractMockResult{
				Iteration: runtime.FanOutIteration(index), Attempt: 1, ExpectedInputs: &expectedInputs,
				Outputs: values.ValueSet{"result": contractInline(t, "mock", fmt.Sprintf("item-%d", index), item)},
			})
		}
		assertCanonicalContractPass(t, plan, kinds, suite)
	})
}

func TestCanonicalContractRunnerUsesRuntimeVerificationEvidence(t *testing.T) {
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	plan := compileContractPlan(t, `workflow:
  name: verified-contract
  version: v1
steps:
  - id: inspect
    transform: {result: "'ok'"}
    outputs:
      result: {type: string}
    verify:
      - type: expected_tool_call
        config:
          server: github
          tool: issues.get
          outcome: succeeded
outputs:
  result:
    type: string
    value: steps.inspect.outputs.result
`)
	output := contractInline(t, "mock", "result", "ok")
	base := executableContractSuite(t, plan, kinds, values.ValueSet{}, values.ValueSet{"result": output})
	call := ContractToolCall{NodeID: "inspect", Kind: "github", Name: "issues.get", Arguments: map[string]any{}, Effect: graph.EffectCompute, Outcome: verification.ActivitySucceeded}
	base.Cases[0].Mocks[0].ExpectedInputs = values.ValueSet{}
	base.Cases[0].Mocks[0].Results = []ContractMockResult{{Outputs: values.ValueSet{"result": output}, Calls: []ContractToolCall{call}}}
	base.Cases[0].ExpectedCalls = []ContractToolCall{call}
	assertCanonicalContractPass(t, plan, kinds, base)

	failedEvidence := cloneContractSuiteForTest(t, base)
	failedEvidence.Cases[0].ExpectedOutputs = nil
	failedEvidence.Cases[0].ExpectedError = &ContractExpectedError{Code: "verification_failed"}
	failedEvidence.Cases[0].Mocks[0].Results[0].Calls[0].Outcome = verification.ActivityFailed
	failedEvidence.Cases[0].ExpectedCalls[0].Outcome = verification.ActivityFailed
	assertCanonicalContractPass(t, plan, kinds, failedEvidence)

	missingEvidence := cloneContractSuiteForTest(t, base)
	missingEvidence.Cases[0].ExpectedOutputs = nil
	missingEvidence.Cases[0].ExpectedError = &ContractExpectedError{Code: "verification_failed"}
	missingEvidence.Cases[0].Mocks[0].Results[0].Calls = nil
	missingEvidence.Cases[0].ExpectedCalls = nil
	assertCanonicalContractPass(t, plan, kinds, missingEvidence)

	canonical, _, err := canonicalContractSuite(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newCanonicalContractRunner(compile.DependencyOptions{}, nil).Execute(t.Context(), plan, kinds, canonical, 1); !errors.Is(err, ErrInvalidContractService) {
		t.Fatalf("verified plan without catalog = %v", err)
	}
}

func TestCanonicalContractRunnerPreservesExactJSONNumbers(t *testing.T) {
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	plan := compileContractPlan(t, `workflow:
  name: exact-number-contract
  version: v1
inputs:
  - name: value
    type: integer
    required: true
steps:
  - id: preserve
    transform: {result: inputs.value}
    with:
      value: inputs.value
    outputs:
      result: {type: integer}
outputs:
  result:
    type: integer
    value: steps.preserve.outputs.result
`)
	input := contractInline(t, "contract-input", "value", json.Number("9007199254740993"))
	output := contractInline(t, "mock", "result", json.Number("9007199254740993"))
	suite := executableContractSuite(t, plan, kinds, values.ValueSet{"value": input}, values.ValueSet{"result": output})
	suite.Cases[0].Mocks[0].ExpectedInputs = values.ValueSet{"value": input}
	suite.Cases[0].Mocks[0].Results = []ContractMockResult{{Outputs: values.ValueSet{"result": output}}}
	assertCanonicalContractPass(t, plan, kinds, suite)

	adjacent := cloneContractSuiteForTest(t, suite)
	adjacent.Cases[0].ExpectedOutputs["result"] = contractInline(t, "mock", "result", json.Number("9007199254740992"))
	canonical, _, err := canonicalContractSuite(adjacent)
	if err != nil {
		t.Fatal(err)
	}
	report, err := newCanonicalContractRunner(compile.DependencyOptions{}, verification.NewDefaultRegistry()).Execute(t.Context(), plan, kinds, canonical, 1)
	if err != nil || report.Passed || report.Cases[0].OutputDigest == "" {
		t.Fatalf("adjacent exact-number mismatch = %#v, %v", report, err)
	}
}

func compileContractPlan(t *testing.T, source string) *compile.ExecutionPlan {
	t.Helper()
	loaded := compile.LoadBytes("contract.workflow.yaml", []byte(source))
	if loaded.Source == nil || len(loaded.Diagnostics) != 0 {
		t.Fatalf("LoadBytes() = %#v", loaded.Diagnostics)
	}
	compiled := compile.Compile(loaded.Source)
	if compiled.Plan == nil || len(compiled.Diagnostics) != 0 {
		t.Fatalf("Compile() = %#v", compiled.Diagnostics)
	}
	inferred := compile.InferValueDependencies(compiled.Plan, compile.DependencyOptions{})
	if inferred.Plan == nil || len(inferred.Diagnostics) != 0 {
		t.Fatalf("InferValueDependencies() = %#v", inferred.Diagnostics)
	}
	return inferred.Plan
}

func executableContractSuite(t *testing.T, plan *compile.ExecutionPlan, kinds stepkind.Registry, inputs, outputs values.ValueSet) WorkflowContractSuite {
	t.Helper()
	suite, err := scaffoldFor(plan, kinds)
	if err != nil {
		t.Fatal(err)
	}
	suite.Cases[0].Name, suite.Cases[0].Editable = "case", false
	suite.Cases[0].Inputs, suite.Cases[0].ExpectedOutputs = inputs, outputs
	for index := range suite.Cases[0].Mocks {
		suite.Cases[0].Mocks[index].ExpectedInputsEditable = false
		suite.Cases[0].Mocks[index].ExpectedInputs = values.ValueSet{}
		suite.Cases[0].Mocks[index].Results = nil
	}
	return suite
}

func assertCanonicalContractPass(t *testing.T, plan *compile.ExecutionPlan, kinds stepkind.Registry, suite WorkflowContractSuite) {
	t.Helper()
	canonical, _, err := canonicalContractSuite(suite)
	if err != nil {
		t.Fatal(err)
	}
	if validationErr := validateContractSuiteForPlan(plan, kinds, canonical, false); validationErr != nil {
		t.Fatal(validationErr)
	}
	report, err := newCanonicalContractRunner(compile.DependencyOptions{}, verification.NewDefaultRegistry()).Execute(t.Context(), plan, kinds, canonical, 2)
	if err != nil || !report.Passed {
		t.Fatalf("canonical report = %#v, %v", report, err)
	}
}

func contractInline(t *testing.T, kind, output string, raw any) values.Value {
	t.Helper()
	value, err := values.NewInline(raw, values.Metadata{Producer: values.Producer{Kind: kind, Reference: "contract", Output: output}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func newContractServiceFixture(t *testing.T, runner ContractRunner, authorize func(context.Context, NamespaceAuthorization) error) (*ContractRegistrationService, graph.DefinitionRef, []byte) {
	t.Helper()
	root := t.TempDir()
	source := []byte(`workflow:
  name: contract-demo
  namespace: team
  version: v1
inputs:

  - name: message
    type: string
    required: true
steps:
  - id: echo
    transform:
      result: inputs.message
    with:
      message: inputs.message
    outputs:
      result:
        type: string
    effects: [compute]
outputs:
  result:
    type: string
    value: steps.echo.outputs.result
`)
	if err := os.WriteFile(filepath.Join(root, "contract-demo.workflow.yaml"), source, 0o600); err != nil {
		t.Fatal(err)
	}
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	resolver, err := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{root}, Authorizer: DefinitionAuthorizerFunc(allowDefinitions),
		Compile: DefinitionCompileOptions{StepKinds: kinds, SemanticRevision: "contract-service-test-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewContractRegistrationService(ContractRegistrationOptions{
		Definitions: resolver, StepKinds: kinds, Catalog: hadronregistry.NewWorkflowIndex(),
		Authorizer: NamespaceAuthorizerFunc(authorize), Attestor: testContractAttestor{}, Runner: runner,
		Policy: ContractTestPolicy{MinimumCases: 1, Repetitions: 2},
		Now:    func() time.Time { return time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := graph.DefinitionRef{
		Authority: "project", Kind: DefinitionKindFile, ID: "contract-demo",
		Locator: "contract-demo.workflow.yaml", Version: "v1", Digest: values.SHA256Digest(source),
	}
	return service, ref, source
}

func passingContractSuite(t *testing.T, service *ContractRegistrationService, plan *compile.ExecutionPlan) WorkflowContractSuite {
	t.Helper()
	suite, err := scaffoldFor(plan, service.kinds)
	if err != nil {
		t.Fatal(err)
	}
	input, err := values.NewInline("hello", values.Metadata{
		Producer:  values.Producer{Kind: "contract-input", Reference: plan.ID, Output: "message"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := values.NewInline("hello", values.Metadata{
		Producer:  values.Producer{Kind: "contract-output", Reference: plan.ID, Output: "result"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	contractCase := &suite.Cases[0]
	contractCase.Name, contractCase.Editable = "echo", false
	contractCase.Inputs = values.ValueSet{"message": input}
	contractCase.ExpectedOutputs = values.ValueSet{"result": output}
	contractCase.Mocks[0].ExpectedInputs = values.ValueSet{"message": input}
	contractCase.Mocks[0].ExpectedInputsEditable = false
	contractCase.Mocks[0].Results = []ContractMockResult{{Outputs: values.ValueSet{"result": output}}}
	return suite
}

func registerQualifiedContractFixture(t *testing.T, service *ContractRegistrationService, ref graph.DefinitionRef) (WorkflowContractSuite, ContractTestReport, hadronregistry.WorkflowRecord) {
	t.Helper()
	validation, err := service.Validate(t.Context(), ref)
	if err != nil || validation.Plan == nil {
		t.Fatalf("Validate() = %#v, %v", validation, err)
	}
	suite := passingContractSuite(t, service, validation.Plan)
	report, err := service.ExecuteContractTests(t.Context(), ref, suite)
	if err != nil {
		t.Fatal(err)
	}
	record, err := service.Register(t.Context(), RegisterWorkflowRequest{
		Definition: ref, Namespace: "team", Principal: "principal:test", Report: report, MakeCurrent: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return suite, report, record
}

func cloneContractSuiteForTest(t *testing.T, input WorkflowContractSuite) WorkflowContractSuite {
	t.Helper()
	encoded, err := canonicalJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	var result WorkflowContractSuite
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertContractSuiteBytesEqual(t *testing.T, input WorkflowContractSuite, expected []byte) {
	t.Helper()
	actual, err := canonicalJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatal("contract suite caller-owned bytes changed")
	}
}

type contractRunnerFunc func(context.Context, *compile.ExecutionPlan, stepkind.Registry, WorkflowContractSuite, int) (ContractTestReport, error)

func (f contractRunnerFunc) Execute(ctx context.Context, plan *compile.ExecutionPlan, kinds stepkind.Registry, suite WorkflowContractSuite, repetitions int) (ContractTestReport, error) {
	return f(ctx, plan, kinds, suite, repetitions)
}

type testContractAttestor struct{}

func (testContractAttestor) AttestContractReport(_ context.Context, digest string) (string, error) {
	return "test-attestation:" + digest, nil
}

func (testContractAttestor) VerifyContractReport(_ context.Context, digest, attestation string) error {
	if attestation != "test-attestation:"+digest || strings.TrimSpace(digest) == "" {
		return errors.New("invalid test attestation")
	}
	return nil
}

type fixedContractAttestor struct {
	attestation string
}

func (a fixedContractAttestor) AttestContractReport(context.Context, string) (string, error) {
	return a.attestation, nil
}

func (a fixedContractAttestor) VerifyContractReport(_ context.Context, _ string, attestation string) error {
	if attestation != a.attestation {
		return errors.New("invalid fixed attestation")
	}
	return nil
}

type contractDefinitionSpy struct {
	delegate contractDefinitionResolver
	calls    int
}

func (s *contractDefinitionSpy) ResolvePlan(ctx context.Context, ref graph.DefinitionRef) (*compile.ExecutionPlan, error) {
	s.calls++
	return s.delegate.ResolvePlan(ctx, ref)
}

func (s *contractDefinitionSpy) LoadPlan(ctx context.Context, digest string) (*compile.ExecutionPlan, error) {
	s.calls++
	return s.delegate.LoadPlan(ctx, digest)
}

func (s *contractDefinitionSpy) ResolveSource(ctx context.Context, ref graph.DefinitionRef) (ResolvedSource, error) {
	s.calls++
	return s.delegate.ResolveSource(ctx, ref)
}

func (s *contractDefinitionSpy) reset() { s.calls = 0 }

type workflowCatalogSpy struct {
	delegate       WorkflowContractCatalog
	calls          int
	mutations      int
	inspectHook    func()
	pinQueries     []hadronregistry.WorkflowQuery
	publishQueries []hadronregistry.WorkflowQuery
}

func (s *workflowCatalogSpy) RegisterWorkflow(ctx context.Context, record hadronregistry.WorkflowRecord, current bool) (hadronregistry.WorkflowRecord, error) {
	s.calls++
	s.mutations++
	return s.delegate.RegisterWorkflow(ctx, record, current)
}

func (s *workflowCatalogSpy) PinWorkflow(ctx context.Context, query hadronregistry.WorkflowQuery) (hadronregistry.WorkflowRecord, error) {
	s.calls++
	s.mutations++
	s.pinQueries = append(s.pinQueries, query)
	return s.delegate.PinWorkflow(ctx, query)
}

func (s *workflowCatalogSpy) UnpinWorkflowExact(ctx context.Context, query hadronregistry.WorkflowQuery) error {
	s.calls++
	s.mutations++
	return s.delegate.UnpinWorkflowExact(ctx, query)
}

func (s *workflowCatalogSpy) ResolvePinnedWorkflow(ctx context.Context, name string) (hadronregistry.WorkflowResolution, error) {
	s.calls++
	return s.delegate.ResolvePinnedWorkflow(ctx, name)
}

func (s *workflowCatalogSpy) PublishWorkflow(ctx context.Context, query hadronregistry.WorkflowQuery) (hadronregistry.WorkflowRecord, error) {
	s.calls++
	s.mutations++
	s.publishQueries = append(s.publishQueries, query)
	return s.delegate.PublishWorkflow(ctx, query)
}

func (s *workflowCatalogSpy) InspectWorkflow(ctx context.Context, query hadronregistry.WorkflowQuery) (hadronregistry.WorkflowRecord, error) {
	s.calls++
	record, err := s.delegate.InspectWorkflow(ctx, query)
	if err == nil && s.inspectHook != nil {
		s.inspectHook()
	}
	return record, err
}

func (s *workflowCatalogSpy) SearchWorkflows(ctx context.Context, namespace, text string) ([]hadronregistry.WorkflowRecord, error) {
	s.calls++
	return s.delegate.SearchWorkflows(ctx, namespace, text)
}

func (s *workflowCatalogSpy) ResolveWorkflow(ctx context.Context, query hadronregistry.WorkflowQuery) (hadronregistry.WorkflowResolution, error) {
	s.calls++
	return s.delegate.ResolveWorkflow(ctx, query)
}

func (s *workflowCatalogSpy) reset() {
	s.calls, s.mutations = 0, 0
	s.pinQueries = nil
	s.publishQueries = nil
}

type contractAttestorSpy struct {
	delegate ContractReportAttestor
	calls    int
}

func (s *contractAttestorSpy) AttestContractReport(ctx context.Context, digest string) (string, error) {
	s.calls++
	return s.delegate.AttestContractReport(ctx, digest)
}

func (s *contractAttestorSpy) VerifyContractReport(ctx context.Context, digest, attestation string) error {
	s.calls++
	return s.delegate.VerifyContractReport(ctx, digest, attestation)
}

func (s *contractAttestorSpy) reset() { s.calls = 0 }
