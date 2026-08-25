package appworkflow

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

func TestDefinitionResolverFreezesNodeExpanderNamesInSemanticIdentity(t *testing.T) {
	first := &mutableNodeExpander{name: "zeta"}
	second := &mutableNodeExpander{name: "alpha"}
	input := []workflowcompile.NodeExpander{first, second}
	normalized, names, err := normalizeNodeExpanders(input)
	if err != nil {
		t.Fatal(err)
	}
	first.name = "changed"
	input[0] = &mutableNodeExpander{name: "replacement"}
	if !reflect.DeepEqual(names, []string{"alpha", "zeta"}) || normalized[0].Name() != "alpha" || normalized[1].Name() != "zeta" {
		t.Fatalf("frozen expanders = %#v / %#v", names, normalized)
	}
	without, err := semanticDefinitionKey("revision", 8, nil, nil, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	with, err := semanticDefinitionKey("revision", 8, nil, nil, 0, nil, names)
	if err != nil {
		t.Fatal(err)
	}
	if with == without {
		t.Fatal("node expander names did not change the semantic cache identity")
	}
	if _, _, err := normalizeNodeExpanders([]workflowcompile.NodeExpander{second, second}); err == nil {
		t.Fatal("duplicate node expander name was accepted")
	}
	var typedNil *mutableNodeExpander
	if _, _, err := normalizeNodeExpanders([]workflowcompile.NodeExpander{typedNil}); err == nil {
		t.Fatal("typed-nil node expander was accepted")
	}
}

func TestDefinitionResolverFreezesVerifierCatalogAndKeysFullSpecs(t *testing.T) {
	permissiveSpec := verification.VerifierSpec{
		Kind: "custom_review", Version: "v1", Mode: verification.ModeReviewer,
		ConfigSchema: graph.Schema{"type": "object", "additionalProperties": false},
	}
	strictSpec := permissiveSpec
	strictSpec.ConfigSchema = graph.Schema{
		"type": "object", "required": []any{"approval"},
		"properties":           map[string]any{"approval": map[string]any{"type": "boolean"}},
		"additionalProperties": false,
	}
	evidenceSpec := permissiveSpec
	evidenceSpec.RequiredEvidence = []verification.ActivityKind{verification.ActivityToolCall}
	otherSpec := verification.VerifierSpec{Kind: "alpha", Version: "v2", Mode: verification.ModeDeterministic, ConfigSchema: graph.Schema{"type": "object"}}

	firstKey, keyErr := semanticDefinitionKey("verifier-test-v1", 8, nil, []verification.VerifierSpec{permissiveSpec, otherSpec}, 0, nil, nil)
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	reorderedKey, keyErr := semanticDefinitionKey("verifier-test-v1", 8, nil, []verification.VerifierSpec{otherSpec, permissiveSpec}, 0, nil, nil)
	if keyErr != nil || reorderedKey != firstKey {
		t.Fatalf("canonical verifier key = %q, %v; want %q", reorderedKey, keyErr, firstKey)
	}
	nilKey, keyErr := semanticDefinitionKey("verifier-test-v1", 8, nil, nil, 0, nil, nil)
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	emptyKey, keyErr := semanticDefinitionKey("verifier-test-v1", 8, nil, []verification.VerifierSpec{}, 0, nil, nil)
	if keyErr != nil || emptyKey != nilKey {
		t.Fatalf("nil/empty verifier catalogs are not canonical: %q / %q, %v", nilKey, emptyKey, keyErr)
	}
	strictKey, keyErr := semanticDefinitionKey("verifier-test-v1", 8, nil, []verification.VerifierSpec{strictSpec, otherSpec}, 0, nil, nil)
	if keyErr != nil || strictKey == firstKey {
		t.Fatalf("config schema did not change semantic key: %q / %q, %v", firstKey, strictKey, keyErr)
	}
	evidenceKey, keyErr := semanticDefinitionKey("verifier-test-v1", 8, nil, []verification.VerifierSpec{evidenceSpec, otherSpec}, 0, nil, nil)
	if keyErr != nil || evidenceKey == firstKey {
		t.Fatalf("required evidence did not change semantic key: %q / %q, %v", firstKey, evidenceKey, keyErr)
	}
	revisedKey, keyErr := semanticDefinitionKey("verifier-test-v2", 8, nil, []verification.VerifierSpec{permissiveSpec, otherSpec}, 0, nil, nil)
	if keyErr != nil || revisedKey == firstKey {
		t.Fatalf("semantic revision did not change verifier behavior key: %q / %q, %v", firstKey, revisedKey, keyErr)
	}

	root := t.TempDir()
	if writeErr := os.WriteFile(filepath.Join(root, "workflow.yaml"), testVerifiedWorkflowSource("verified", "custom_review"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	kinds := stepkind.NewRegistry()
	if registerErr := kinds.Register(stepkindtest.NewNoopKind("noop", "v1")); registerErr != nil {
		t.Fatal(registerErr)
	}
	permissiveResolverKey, keyErr := semanticDefinitionKey("verifier-test-v1", workflowcompile.DefaultMaxCallDepth, kinds.List(), []verification.VerifierSpec{permissiveSpec}, 0, []string{}, []string{})
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	strictResolverKey, keyErr := semanticDefinitionKey("verifier-test-v1", workflowcompile.DefaultMaxCallDepth, kinds.List(), []verification.VerifierSpec{strictSpec}, 0, []string{}, []string{})
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	permissiveCatalog := verification.NewRegistry()
	permissive := &resolverTestVerifier{spec: permissiveSpec}
	if registerErr := permissiveCatalog.Register(permissive); registerErr != nil {
		t.Fatal(registerErr)
	}
	permissiveResolver, resolverErr := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{root}, Authorizer: DefinitionAuthorizerFunc(allowDefinitions),
		Compile: DefinitionCompileOptions{StepKinds: kinds, Verifiers: permissiveCatalog, SemanticRevision: "verifier-test-v1"},
	})
	if resolverErr != nil {
		t.Fatal(resolverErr)
	}
	if permissiveResolver.semanticKey != permissiveResolverKey {
		t.Fatalf("resolver semantic key = %q, want %q", permissiveResolver.semanticKey, permissiveResolverKey)
	}
	request := graph.DefinitionRef{Kind: DefinitionKindFile, ID: "verified", Locator: root, Version: "1.0.0"}
	if _, resolveErr := permissiveResolver.ResolvePlan(t.Context(), request); resolveErr != nil {
		t.Fatalf("permissive verifier ResolvePlan() = %v", resolveErr)
	}

	strictCatalog := verification.NewRegistry()
	strict := &resolverTestVerifier{spec: strictSpec}
	if registerErr := strictCatalog.Register(strict); registerErr != nil {
		t.Fatal(registerErr)
	}
	strictResolver, resolverErr := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{root}, Authorizer: DefinitionAuthorizerFunc(allowDefinitions),
		Compile: DefinitionCompileOptions{StepKinds: kinds, Verifiers: strictCatalog, SemanticRevision: "verifier-test-v1"},
	})
	if resolverErr != nil {
		t.Fatal(resolverErr)
	}
	if strictResolver.semanticKey != strictResolverKey {
		t.Fatalf("strict resolver semantic key = %q, want %q", strictResolver.semanticKey, strictResolverKey)
	}
	if _, err := strictResolver.ResolvePlan(t.Context(), request); err == nil {
		t.Fatal("strict verifier schema accepted missing config")
	} else {
		var diagnosticErr *DefinitionDiagnosticError
		if !errors.As(err, &diagnosticErr) || len(diagnosticErr.Diagnostics()) == 0 || diagnosticErr.Diagnostics()[0].Code != verification.CodeInvalidCheck {
			t.Fatalf("strict verifier diagnostics = %v", err)
		}
	}
	if strict.validationCalls.Load() != 0 {
		t.Fatalf("schema-invalid config reached verifier implementation %d times", strict.validationCalls.Load())
	}

	if err := permissiveCatalog.Register(&resolverTestVerifier{spec: verification.VerifierSpec{
		Kind: "late_review", Version: "v1", Mode: verification.ModeReviewer,
		ConfigSchema: graph.Schema{"type": "object"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := permissiveResolver.Verifiers().Lookup("late_review"); ok {
		t.Fatal("late verifier registration changed resolver snapshot")
	}
	if err := os.WriteFile(filepath.Join(root, "late.workflow.yaml"), testVerifiedWorkflowSource("late", "late_review"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := permissiveResolver.ResolvePlan(t.Context(), graph.DefinitionRef{Kind: DefinitionKindFile, ID: "late", Locator: "late.workflow.yaml", Version: "1.0.0"}); err == nil {
		t.Fatal("late verifier changed compiled validation behavior")
	}

	var typedNil *verification.MemoryRegistry
	if _, err := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{root}, Authorizer: DefinitionAuthorizerFunc(allowDefinitions),
		Compile: DefinitionCompileOptions{StepKinds: kinds, Verifiers: typedNil, SemanticRevision: "verifier-test-v1"},
	}); !errors.Is(err, ErrInvalidDefinitionOptions) {
		t.Fatalf("typed-nil verifier registry error = %v", err)
	}
}

type resolverTestVerifier struct {
	spec            verification.VerifierSpec
	validationCalls atomic.Int64
}

func (v *resolverTestVerifier) Spec() verification.VerifierSpec { return v.spec }

func (v *resolverTestVerifier) ValidateConfig(context.Context, graph.VerificationCheck) []diagnostic.Diagnostic {
	v.validationCalls.Add(1)
	return nil
}

func (v *resolverTestVerifier) Verify(context.Context, verification.Request) (verification.CheckResult, error) {
	return verification.CheckResult{Kind: v.spec.Kind, Version: v.spec.Version, Outcome: verification.CheckPassed, Code: "custom_passed", Message: "custom verifier passed"}, nil
}

func testVerifiedWorkflowSource(name, verifier string) []byte {
	return []byte(fmt.Sprintf(`workflow:
  name: %s
  version: 1.0.0
steps:
  - id: root
    kind: noop
    kind_version: v1
    verify:
      - type: %s
`, name, verifier))
}

type mutableNodeExpander struct{ name string }

func (e *mutableNodeExpander) Name() string { return e.name }

func (*mutableNodeExpander) ExpandNode(workflowcompile.NodeExpansionRequest) (workflowcompile.NodeExpansion, bool, []diagnostic.Diagnostic) {
	return workflowcompile.NodeExpansion{}, false, nil
}

func TestDefinitionResolverSupportsFileDirectoryRegistryAndPackageSources(t *testing.T) {
	root := t.TempDir()
	fileSource := testWorkflowSource("file-root", "1.0.0", "noop")
	if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), fileSource, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "named.workflow.yaml"), testWorkflowSource("named", "1.0.0", "noop"), 0o600); err != nil {
		t.Fatal(err)
	}
	archive := appWorkflowArchive(t, map[string][]byte{"nested/packaged.workflow.yaml": testWorkflowSource("packaged", "1.0.0", "noop")})
	if err := os.WriteFile(filepath.Join(root, "bundle.tar.gz"), archive, 0o600); err != nil {
		t.Fatal(err)
	}
	index := hadronregistry.NewWorkflowIndex()
	registrySource := testWorkflowSource("registered", "2.0.0", "noop")
	registered, err := index.RegisterWorkflow(t.Context(), hadronregistry.WorkflowRecord{
		Name: "registered", Version: "2.0.0", Source: registrySource,
		Authority: "registry.test", TrustClass: "signed",
		Provenance: graph.Provenance{
			Origin: "publisher", Locator: "registry://registered/2.0.0/registered.workflow.yaml",
			Parents:  []graph.ProvenanceRef{{Authority: "git", Locator: "git://repo", Digest: values.SHA256Digest([]byte("commit"))}},
			Metadata: graph.Metadata{"publisher": "test", "attestations": []any{"signed"}},
		},
	}, true)
	if err != nil {
		t.Fatal(err)
	}

	resolver := newTestDefinitionResolver(t, root, index, DefinitionAuthorizerFunc(allowDefinitions))
	tests := []struct {
		name      string
		ref       graph.DefinitionRef
		wantID    string
		wantTrust string
	}{
		{name: "directory default", ref: graph.DefinitionRef{Kind: DefinitionKindFile, ID: "file-root", Locator: root, Version: "1.0.0"}, wantID: "file-root", wantTrust: "project"},
		{name: "named file", ref: graph.DefinitionRef{Kind: DefinitionKindFile, ID: "named", Locator: "named.workflow.yaml", Version: "1.0.0"}, wantID: "named", wantTrust: "project"},
		{name: "registry version", ref: graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: "registered", Version: "2.0.0", Digest: registered.Digest}, wantID: "registered", wantTrust: "signed"},
		{name: "package", ref: graph.DefinitionRef{Kind: DefinitionKindPackage, ID: "packaged", Locator: "bundle.tar.gz#nested/packaged.workflow.yaml", Version: "1.0.0"}, wantID: "packaged", wantTrust: "packaged"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := resolver.ResolveSource(t.Context(), test.ref)
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Definition.ID != test.wantID || resolved.TrustClass != test.wantTrust || resolved.Digest != values.SHA256Digest(resolved.Bytes) {
				t.Fatalf("resolved = %+v", resolved)
			}
			plan, err := resolver.ResolvePlan(t.Context(), test.ref)
			if err != nil {
				var diagnostics *DefinitionDiagnosticError
				_ = errors.As(err, &diagnostics)
				var findings any
				if diagnostics != nil {
					findings = diagnostics.Diagnostics()
				}
				t.Fatalf("resolve plan: %v; diagnostics=%+v", err, findings)
			}
			if plan.ID != test.wantID || len(plan.SourceDigests) != 1 || plan.SourceDigests[0].Digest != resolved.Digest {
				t.Fatalf("plan = %+v", plan)
			}
			if test.name == "registry version" {
				if !reflect.DeepEqual(plan.Provenance.Parents, registered.Provenance.Parents) || !reflect.DeepEqual(plan.Provenance.Metadata["attestations"], []any{"signed"}) {
					t.Fatalf("registry provenance = %+v", plan.Provenance)
				}
			}
			if test.name == "package" {
				if got := plan.Provenance.Metadata["package_digest"]; got != values.SHA256Digest(archive) || got == resolved.Digest {
					t.Fatalf("package/source digests were conflated: %+v", plan.Provenance.Metadata)
				}
			}
		})
	}
}

func TestDefinitionResolverAcceptsMaximumFileAndPackageBounds(t *testing.T) {
	root := t.TempDir()
	fileSource := testWorkflowSource("maximum-file", "1.0.0", "noop")
	if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), fileSource, 0o600); err != nil {
		t.Fatal(err)
	}
	packageSource := testWorkflowSource("maximum-package", "1.0.0", "noop")
	archive := appWorkflowArchive(t, map[string][]byte{"workflow.yaml": packageSource})
	if err := os.WriteFile(filepath.Join(root, "bundle.tar.gz"), archive, 0o600); err != nil {
		t.Fatal(err)
	}
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(stepkindtest.NewNoopKind("noop", "v1")); err != nil {
		t.Fatal(err)
	}
	resolver, err := NewDefinitionResolver(DefinitionResolverOptions{
		Roots:           []string{root},
		Authorizer:      DefinitionAuthorizerFunc(allowDefinitions),
		Compile:         DefinitionCompileOptions{StepKinds: kinds, SemanticRevision: "maximum-bound-tests-v1"},
		MaxSourceBytes:  math.MaxInt64,
		MaxArchiveBytes: math.MaxInt64,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		ref  graph.DefinitionRef
		want []byte
	}{
		{name: "file", ref: graph.DefinitionRef{Kind: DefinitionKindFile, ID: "maximum-file", Locator: "workflow.yaml", Version: "1.0.0"}, want: fileSource},
		{name: "package", ref: graph.DefinitionRef{Kind: DefinitionKindPackage, ID: "maximum-package", Locator: "bundle.tar.gz#workflow.yaml", Version: "1.0.0"}, want: packageSource},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := resolver.ResolveSource(t.Context(), test.ref)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(resolved.Bytes, test.want) {
				t.Fatalf("resolved bytes = %q, want %q", resolved.Bytes, test.want)
			}
		})
	}
}

func TestDefinitionResolverReauthorizesMovableRefsPinsExactBytesAndDefensivelyCopies(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "orders.workflow.yaml")
	first := testWorkflowSource("orders", "1.0.0", "noop")
	second := bytes.Replace(first, []byte("kind: noop"), []byte("kind: alternative"), 1)
	if err := os.WriteFile(path, first, 0o600); err != nil {
		t.Fatal(err)
	}
	var authorizations atomic.Int64
	authorizer := DefinitionAuthorizerFunc(func(context.Context, DefinitionAuthorization) error {
		authorizations.Add(1)
		return nil
	})
	resolver := newTestDefinitionResolverWithKinds(t, root, nil, authorizer, "noop", "alternative")
	movable := graph.DefinitionRef{Kind: DefinitionKindFile, ID: "orders", Locator: "orders.workflow.yaml", Version: "1.0.0"}
	firstResolved, err := resolver.ResolveSource(t.Context(), movable)
	if err != nil {
		t.Fatal(err)
	}
	pinned := movable
	pinned.Digest = firstResolved.Digest
	if _, pinErr := resolver.ResolveSource(t.Context(), pinned); pinErr != nil {
		t.Fatal(pinErr)
	}
	if writeErr := os.WriteFile(path, second, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	exactReplay, err := resolver.ResolveSource(t.Context(), pinned)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(exactReplay.Bytes, first) {
		t.Fatal("exact digest did not preserve previously resolved source bytes")
	}
	refreshed, err := resolver.ResolveSource(t.Context(), movable)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.Digest == firstResolved.Digest || !bytes.Equal(refreshed.Bytes, second) {
		t.Fatalf("movable reference did not re-resolve: %+v", refreshed)
	}
	if authorizations.Load() != 8 { // requested+resolved for each of four resolutions
		t.Fatalf("authorization calls = %d", authorizations.Load())
	}

	plan, err := resolver.ResolvePlan(t.Context(), movable)
	if err != nil {
		t.Fatal(err)
	}
	plan.Graph.Nodes[0].ID = "mutated"
	loaded, err := resolver.LoadPlan(t.Context(), plan.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Graph.Nodes[0].ID == "mutated" {
		t.Fatal("caller mutation changed cached plan")
	}
	loaded.Graph.Nodes[0].ID = "mutated-again"
	reloaded, err := resolver.LoadPlan(t.Context(), plan.Digest)
	if err != nil || reloaded.Graph.Nodes[0].ID == "mutated-again" {
		t.Fatalf("loaded plan was not defensively copied: %v", err)
	}
}

func TestDefinitionResolverCapturesPlanSourceAndCompileSnapshotInOneResolution(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "snapshot.workflow.yaml")
	first := testWorkflowSource("snapshot", "1.0.0", "noop")
	second := bytes.Replace(first, []byte("kind: noop"), []byte("kind: alternative"), 1)
	if err := os.WriteFile(path, first, 0o600); err != nil {
		t.Fatal(err)
	}
	resolver := newTestDefinitionResolverWithKinds(t, root, nil, DefinitionAuthorizerFunc(allowDefinitions), "noop", "alternative")
	requested := graph.DefinitionRef{Kind: DefinitionKindFile, ID: "snapshot", Locator: "snapshot.workflow.yaml", Version: "1.0.0"}
	initial, err := resolver.ResolvePlanSnapshot(t.Context(), requested)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Source == nil || !bytes.Equal(initial.Source.Content, first) || initial.Source.Digest != values.SHA256Digest(first) || !initial.Compile.Available || initial.Compile.SemanticRevision != "definition-tests-v1" || initial.Digest == initial.Plan.Digest {
		t.Fatalf("resolved snapshot = %#v", initial)
	}
	initial.Source.Content[0] = 'X'
	initial.Compile.StepKinds[0].Name = "mutated"
	if writeErr := os.WriteFile(path, second, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	refreshed, err := resolver.ResolvePlanSnapshot(t.Context(), requested)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(refreshed.Source.Content, second) || refreshed.Source.Content[0] == 'X' || refreshed.Compile.StepKinds[0].Name == "mutated" || refreshed.Digest == initial.Digest {
		t.Fatalf("snapshot was not exact and defensive: %#v", refreshed)
	}
	tampered := refreshed
	tampered.Digest = initial.Digest
	if tampered.Validate() == nil {
		t.Fatal("snapshot accepted a digest from different exact material")
	}
}

func TestDefinitionResolverReresolvesRegistryAliasWhileExactDigestStaysReproducible(t *testing.T) {
	root := t.TempDir()
	index := hadronregistry.NewWorkflowIndex()
	register := func(version, kind string, current bool) hadronregistry.WorkflowRecord {
		source := testWorkflowSource("service", version, kind)
		record, err := index.RegisterWorkflow(t.Context(), hadronregistry.WorkflowRecord{
			Name: "service", Version: version, Source: source,
			Authority: "registry.test", TrustClass: "signed",
			Provenance: graph.Provenance{Origin: "publisher", Locator: "registry://service/" + version + "/service.workflow.yaml"},
		}, current)
		if err != nil {
			t.Fatal(err)
		}
		return record
	}
	first := register("1.0.0", "noop", true)
	resolver := newTestDefinitionResolverWithKinds(t, root, index, DefinitionAuthorizerFunc(allowDefinitions), "noop", "alternative")
	alias := graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: "service"}
	resolvedFirst, err := resolver.ResolveSource(t.Context(), alias)
	if err != nil || resolvedFirst.Digest != first.Digest || !resolvedFirst.Movable {
		t.Fatalf("first alias = %+v, %v", resolvedFirst, err)
	}
	pinned := graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: "service", Digest: first.Digest}
	if _, pinErr := resolver.ResolveSource(t.Context(), pinned); pinErr != nil {
		t.Fatal(pinErr)
	}
	second := register("2.0.0", "alternative", true)
	resolvedSecond, err := resolver.ResolveSource(t.Context(), alias)
	if err != nil || resolvedSecond.Digest != second.Digest || resolvedSecond.Digest == resolvedFirst.Digest {
		t.Fatalf("moved alias = %+v, %v", resolvedSecond, err)
	}
	exact, err := resolver.ResolveSource(t.Context(), pinned)
	if err != nil || exact.Digest != first.Digest || !bytes.Equal(exact.Bytes, first.Source) {
		t.Fatalf("exact replay = %+v, %v", exact, err)
	}
	conflict := graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: "service", Version: second.Version, Digest: first.Digest}
	if _, err := resolver.ResolveSource(t.Context(), conflict); !errors.Is(err, ErrDefinitionPinConflict) {
		t.Fatalf("version/digest conflict = %v", err)
	}
}

func TestDefinitionResolverIsolatesMutatingAuthorizerInputs(t *testing.T) {
	root := t.TempDir()
	source := testWorkflowSource("immutable-auth", "1.0.0", "noop")
	if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), source, 0o600); err != nil {
		t.Fatal(err)
	}
	requestedProvenance := &graph.Provenance{Authority: "project", Metadata: graph.Metadata{"caller": "original"}}
	authorizer := DefinitionAuthorizerFunc(func(_ context.Context, request DefinitionAuthorization) error {
		request.Requested.ID = "mutated"
		if request.Requested.Provenance != nil {
			request.Requested.Provenance.Authority = "mutated"
			request.Requested.Provenance.Metadata["caller"] = "mutated"
		}
		if request.Resolved != nil {
			request.Resolved.ID = "mutated"
			request.Resolved.Locator = "mutated.workflow.yaml"
			if request.Resolved.Provenance != nil {
				request.Resolved.Provenance.Locator = "mutated.workflow.yaml"
				request.Resolved.Provenance.Metadata["trust_class"] = "mutated"
			}
		}
		return nil
	})
	resolver := newTestDefinitionResolver(t, root, nil, authorizer)
	ref := graph.DefinitionRef{
		Authority: "project", Kind: DefinitionKindFile, ID: "immutable-auth",
		Locator: root, Version: "1.0.0", Provenance: requestedProvenance,
	}
	resolved, err := resolver.ResolveSource(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Requested.ID != "immutable-auth" || resolved.Definition.ID != "immutable-auth" ||
		resolved.Definition.Provenance.Locator == "mutated.workflow.yaml" || requestedProvenance.Metadata["caller"] != "original" {
		t.Fatalf("authorizer mutated resolver state: requested=%+v definition=%+v original=%+v", resolved.Requested, resolved.Definition, requestedProvenance)
	}
	plan, err := resolver.ResolvePlan(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := resolver.LoadPlan(t.Context(), plan.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ID != "immutable-auth" || loaded.Provenance.Locator == "mutated.workflow.yaml" || loaded.Provenance.Metadata["trust_class"] != "project" {
		t.Fatalf("authorizer mutated loaded plan: %+v", loaded)
	}
}

func TestDefinitionResolverConcurrentExactSourceDivergenceConflicts(t *testing.T) {
	root := t.TempDir()
	source := testWorkflowSource("divergent", "1.0.0", "noop")
	digest := values.SHA256Digest(source)
	registry := &divergentWorkflowResolver{source: source, digest: digest, release: make(chan struct{})}
	resolver := newTestDefinitionResolver(t, root, registry, DefinitionAuthorizerFunc(allowDefinitions))
	refs := []graph.DefinitionRef{
		{Kind: DefinitionKindRegistry, ID: "divergent", Version: "1.0.0", Provenance: &graph.Provenance{Metadata: graph.Metadata{"caller": "first"}}},
		{Kind: DefinitionKindRegistry, ID: "divergent", Version: "1.0.0", Provenance: &graph.Provenance{Metadata: graph.Metadata{"caller": "second"}}},
	}
	results := make(chan error, 2)
	for _, ref := range refs {
		go func() {
			_, resolveErr := resolver.ResolveSource(context.Background(), ref)
			results <- resolveErr
		}()
	}
	var successes, conflicts int
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrDefinitionPinConflict):
			conflicts++
		default:
			t.Fatalf("unexpected resolution error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 || resolver.CacheStats().ExactSources != 1 || registry.calls.Load() != 2 {
		t.Fatalf("successes=%d conflicts=%d stats=%+v", successes, conflicts, resolver.CacheStats())
	}
}

func TestDefinitionResolverExactCacheIgnoresCallerProvenanceWithoutHidingItFromAuthorization(t *testing.T) {
	root := t.TempDir()
	index := hadronregistry.NewWorkflowIndex()
	record, err := index.RegisterWorkflow(t.Context(), hadronregistry.WorkflowRecord{
		Name: "provenance-cache", Version: "1.0.0", Source: testWorkflowSource("provenance-cache", "1.0.0", "noop"),
		Authority: "registry.test", TrustClass: "signed",
		Provenance: graph.Provenance{Origin: "publisher", Locator: "registry://provenance-cache/1.0.0/provenance-cache.workflow.yaml"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	registry := &countingWorkflowResolver{delegate: index}
	var authorizedMu sync.Mutex
	authorized := make(map[string]int)
	authorizer := DefinitionAuthorizerFunc(func(_ context.Context, request DefinitionAuthorization) error {
		if request.Requested.Provenance != nil {
			caller, _ := request.Requested.Provenance.Metadata["caller"].(string)
			authorizedMu.Lock()
			authorized[caller]++
			authorizedMu.Unlock()
		}
		return nil
	})
	resolver := newTestDefinitionResolver(t, root, registry, authorizer)
	resolve := func(caller string) ResolvedSource {
		t.Helper()
		resolved, resolveErr := resolver.ResolveSource(t.Context(), graph.DefinitionRef{
			Kind: DefinitionKindRegistry, ID: record.Name, Version: record.Version,
			Provenance: &graph.Provenance{Metadata: graph.Metadata{"caller": caller}},
		})
		if resolveErr != nil {
			t.Fatal(resolveErr)
		}
		return resolved
	}
	first := resolve("first")
	second := resolve("second")
	if registry.calls.Load() != 1 || resolver.CacheStats().ExactSources != 1 || first.Digest != second.Digest {
		t.Fatalf("registry calls=%d stats=%+v digests=%q/%q", registry.calls.Load(), resolver.CacheStats(), first.Digest, second.Digest)
	}
	if first.Requested.Provenance.Metadata["caller"] != "first" || second.Requested.Provenance.Metadata["caller"] != "second" {
		t.Fatalf("caller provenance was leaked across exact cache: first=%+v second=%+v", first.Requested, second.Requested)
	}
	authorizedMu.Lock()
	firstAuthorizations, secondAuthorizations := authorized["first"], authorized["second"]
	authorizedMu.Unlock()
	if firstAuthorizations != 2 || secondAuthorizations != 2 {
		t.Fatalf("authorization calls by caller = %+v", authorized)
	}
}

func TestDefinitionResolverRejectsEmptyAndOversizedRegistrySourcesBeforeResolvedAuthorization(t *testing.T) {
	for _, test := range []struct {
		name   string
		source []byte
	}{
		{name: "empty"},
		{name: "oversized", source: bytes.Repeat([]byte("x"), 33)},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			digest := values.SHA256Digest(test.source)
			registry := staticWorkflowResolver{resolution: hadronregistry.WorkflowResolution{Record: hadronregistry.WorkflowRecord{
				Name: "bounded", Version: "1.0.0", Digest: digest, Source: bytes.Clone(test.source),
				Authority: "registry.test", TrustClass: "signed",
				Provenance: graph.Provenance{Authority: "registry.test", Origin: "publisher", Locator: "registry://bounded/1.0.0/bounded.workflow.yaml", Revision: "1.0.0", Digest: digest},
			}}}
			var resolvedAuthorizations atomic.Int64
			kinds := stepkind.NewRegistry()
			if err := kinds.Register(stepkindtest.NewNoopKind("noop", "v1")); err != nil {
				t.Fatal(err)
			}
			resolver, err := NewDefinitionResolver(DefinitionResolverOptions{
				Roots: []string{root}, Registry: registry,
				Authorizer: DefinitionAuthorizerFunc(func(_ context.Context, request DefinitionAuthorization) error {
					if request.Stage == AuthorizationResolved {
						resolvedAuthorizations.Add(1)
					}
					return nil
				}),
				Compile:        DefinitionCompileOptions{StepKinds: kinds, SemanticRevision: "bounded-registry-v1"},
				MaxSourceBytes: 32,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, resolveErr := resolver.ResolveSource(t.Context(), graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: "bounded", Version: "1.0.0"})
			if !errors.Is(resolveErr, ErrUnsafeDefinitionSource) || resolvedAuthorizations.Load() != 0 || resolver.CacheStats().ExactSources != 0 {
				t.Fatalf("ResolveSource error=%v resolved_auth=%d stats=%+v", resolveErr, resolvedAuthorizations.Load(), resolver.CacheStats())
			}
			var diagnosticErr *DefinitionDiagnosticError
			if !errors.As(resolveErr, &diagnosticErr) || len(diagnosticErr.Diagnostics()) != 1 || diagnosticErr.Diagnostics()[0].Code != CodeDefinitionUnsafe {
				t.Fatalf("structured error = %T %v", resolveErr, resolveErr)
			}
		})
	}
}

func TestDefinitionResolverCacheIsConcurrentAndDoesNotCacheAuthorizationFailures(t *testing.T) {
	root := t.TempDir()
	source := testWorkflowSource("concurrent", "1.0.0", "noop")
	if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), source, 0o600); err != nil {
		t.Fatal(err)
	}
	var deny atomic.Bool
	deny.Store(true)
	resolver := newTestDefinitionResolver(t, root, nil, DefinitionAuthorizerFunc(func(_ context.Context, request DefinitionAuthorization) error {
		if deny.Load() && request.Stage == AuthorizationResolved {
			return errors.New("denied")
		}
		return nil
	}))
	ref := graph.DefinitionRef{Kind: DefinitionKindFile, ID: "concurrent", Locator: root, Version: "1.0.0", Digest: values.SHA256Digest(source)}
	if _, err := resolver.ResolvePlan(t.Context(), ref); !errors.Is(err, ErrDefinitionUnauthorized) {
		t.Fatalf("denied error = %v", err)
	}
	if stats := resolver.CacheStats(); stats.Plans != 0 || stats.ExactSources != 0 || stats.Compilations != 0 {
		t.Fatalf("denied resolution populated cache: %+v", stats)
	}
	deny.Store(false)

	const callers = 32
	start := make(chan struct{})
	errorsByCaller := make(chan error, callers)
	var plans sync.WaitGroup
	plans.Add(callers)
	for index := 0; index < callers; index++ {
		go func() {
			defer plans.Done()
			<-start
			plan, err := resolver.ResolvePlan(context.Background(), ref)
			if err == nil && plan.ID != "concurrent" {
				err = fmt.Errorf("plan id = %q", plan.ID)
			}
			errorsByCaller <- err
		}()
	}
	close(start)
	plans.Wait()
	close(errorsByCaller)
	for err := range errorsByCaller {
		if err != nil {
			t.Fatal(err)
		}
	}
	if stats := resolver.CacheStats(); stats.Plans != 1 || stats.ExactSources != 1 || stats.Compilations != 1 {
		t.Fatalf("cache stats = %+v", stats)
	}
}

func TestDefinitionResolverCanceledFlightOwnerDoesNotPoisonOtherCaller(t *testing.T) {
	root := t.TempDir()
	if writeErr := os.WriteFile(filepath.Join(root, "workflow.yaml"), testWorkflowSource("flight", "1.0.0", "blocking"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	kind := stepkindtest.NewNoopKind("blocking", "v1")
	kind.ValidateConfigFunc = func(context.Context, graph.Config) []diagnostic.Diagnostic {
		once.Do(func() { close(entered) })
		<-release
		return nil
	}
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(kind); err != nil {
		t.Fatal(err)
	}
	resolver, err := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{root}, Authorizer: DefinitionAuthorizerFunc(allowDefinitions),
		Compile: DefinitionCompileOptions{StepKinds: kinds, SemanticRevision: "flight-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := graph.DefinitionRef{Kind: DefinitionKindFile, ID: "flight", Locator: root, Version: "1.0.0"}
	ownerContext, cancelOwner := context.WithCancel(t.Context())
	ownerResult := make(chan error, 1)
	go func() {
		_, resolveErr := resolver.ResolvePlan(ownerContext, ref)
		ownerResult <- resolveErr
	}()
	<-entered
	cancelOwner()
	waiterResult := make(chan error, 1)
	go func() {
		plan, resolveErr := resolver.ResolvePlan(context.Background(), ref)
		if resolveErr == nil && plan.ID != "flight" {
			resolveErr = fmt.Errorf("plan id = %q", plan.ID)
		}
		waiterResult <- resolveErr
	}()
	close(release)
	if ownerErr := <-ownerResult; !errors.Is(ownerErr, context.Canceled) {
		t.Fatalf("owner error = %v", ownerErr)
	}
	if waiterErr := <-waiterResult; waiterErr != nil {
		t.Fatalf("waiter error = %v", waiterErr)
	}
	if stats := resolver.CacheStats(); stats.Compilations != 1 || stats.Plans != 1 {
		t.Fatalf("flight cache stats = %+v", stats)
	}
}

func TestDefinitionResolverRejectsAmbiguousPinsLegacyAndUnsafeSourceRaces(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "safe"), 0o700); err != nil {
		t.Fatal(err)
	}
	insideSource := testWorkflowSource("inside", "1.0.0", "noop")
	outsideSource := testWorkflowSource("outside", "1.0.0", "noop")
	if err := os.WriteFile(filepath.Join(root, "safe", "workflow.yaml"), insideSource, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "workflow.yaml"), outsideSource, 0o600); err != nil {
		t.Fatal(err)
	}
	resolver := newTestDefinitionResolver(t, root, nil, DefinitionAuthorizerFunc(allowDefinitions))
	tests := []graph.DefinitionRef{
		{Kind: "workflow", ID: "inside", Locator: "safe/workflow.yaml"},
		{Kind: DefinitionKindRegistry, ID: "inside", Locator: "safe/workflow.yaml"},
		{Kind: DefinitionKindFile, ID: "inside"},
		{Kind: "blueprint", Locator: "safe/workflow.yaml"},
	}
	for _, ref := range tests {
		if _, err := resolver.ResolveSource(t.Context(), ref); !errors.Is(err, ErrDefinitionUnresolved) {
			t.Fatalf("ambiguous/legacy ref %+v error = %v", ref, err)
		}
	}
	if _, err := resolver.ResolveSource(t.Context(), graph.DefinitionRef{Kind: DefinitionKindFile, ID: "inside", Locator: "../workflow.yaml"}); !errors.Is(err, ErrUnsafeDefinitionSource) {
		t.Fatalf("traversal error = %v", err)
	}

	var swapped atomic.Bool
	resolver.sources.afterFirstRead = func() {
		if !swapped.CompareAndSwap(false, true) {
			return
		}
		if err := os.Rename(filepath.Join(root, "safe"), filepath.Join(root, "safe-original")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "safe")); err != nil {
			t.Fatal(err)
		}
	}
	_, err := resolver.ResolveSource(t.Context(), graph.DefinitionRef{Kind: DefinitionKindFile, ID: "inside", Locator: "safe/workflow.yaml", Version: "1.0.0"})
	if !errors.Is(err, ErrUnsafeDefinitionSource) {
		t.Fatalf("ancestor swap error = %v", err)
	}
}

func TestDefinitionResolverFailsClosedOnIdentityPinAndLoadAuthorization(t *testing.T) {
	root := t.TempDir()
	source := testWorkflowSource("actual", "1.0.0", "noop")
	if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), source, 0o600); err != nil {
		t.Fatal(err)
	}
	var denyLoad atomic.Bool
	var stagesMu sync.Mutex
	var stages []DefinitionAuthorizationStage
	resolver := newTestDefinitionResolver(t, root, nil, DefinitionAuthorizerFunc(func(_ context.Context, request DefinitionAuthorization) error {
		stagesMu.Lock()
		stages = append(stages, request.Stage)
		stagesMu.Unlock()
		if denyLoad.Load() && request.Stage == AuthorizationResolved && request.Requested.Digest != "" && request.Requested.ID == "" {
			return errors.New("exact authority denied")
		}
		return nil
	}))
	wrong := graph.DefinitionRef{Kind: DefinitionKindFile, ID: "expected", Locator: root, Version: "1.0.0"}
	if _, err := resolver.ResolvePlan(t.Context(), wrong); !errors.Is(err, ErrDefinitionPinConflict) {
		t.Fatalf("id mismatch error = %v", err)
	}
	plan, err := resolver.ResolvePlan(t.Context(), graph.DefinitionRef{Kind: DefinitionKindFile, ID: "actual", Locator: root, Version: "1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	denyLoad.Store(true)
	if _, err := resolver.LoadPlan(t.Context(), plan.Digest); !errors.Is(err, ErrDefinitionUnauthorized) {
		t.Fatalf("load authorization error = %v", err)
	}
	stagesMu.Lock()
	gotStages := append([]DefinitionAuthorizationStage(nil), stages...)
	stagesMu.Unlock()
	if !containsStage(gotStages, AuthorizationPlanLoad) || !containsStage(gotStages, AuthorizationResolved) {
		t.Fatalf("authorization stages = %v", gotStages)
	}
}

func TestDefinitionResolverReplacesAuthoredProvenanceWithHostAuthority(t *testing.T) {
	root := t.TempDir()
	parentDigest := values.SHA256Digest([]byte("forged-parent"))
	source := []byte(fmt.Sprintf(`workflow:
  name: hostile
  version: 1.0.0
  provenance:
    authority: attacker
    origin: forged
    revision: forged-revision
    parents:
      - authority: attacker
        locator: forged://parent
        digest: %s
    metadata:
      trust_class: forged
      package_digest: forged
      attacker: survives
steps:
  - id: root
    kind: noop
    kind_version: v1
`, parentDigest))
	if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), source, 0o600); err != nil {
		t.Fatal(err)
	}
	resolver := newTestDefinitionResolver(t, root, nil, DefinitionAuthorizerFunc(allowDefinitions))
	plan, err := resolver.ResolvePlan(t.Context(), graph.DefinitionRef{Kind: DefinitionKindFile, ID: "hostile", Locator: root, Version: "1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Provenance.Authority != "project" || plan.Provenance.Origin != "workflow-file" || plan.Provenance.Revision != "" || len(plan.Provenance.Parents) != 0 {
		t.Fatalf("host provenance = %+v", plan.Provenance)
	}
	if !reflect.DeepEqual(plan.Provenance.Metadata, graph.Metadata{"trust_class": "project"}) {
		t.Fatalf("host metadata retained authored claims: %+v", plan.Provenance.Metadata)
	}
}

func TestDefinitionResolverKeepsRelocationVariantsExplicitAtDigestLoad(t *testing.T) {
	root := t.TempDir()
	contents := testWorkflowSource("relocatable", "1.0.0", "noop")
	for _, directory := range []string{"first", "second"} {
		if err := os.Mkdir(filepath.Join(root, directory), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, directory, "workflow.yaml"), contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var allowedLocator atomic.Value
	allowedLocator.Store("")
	authorizer := DefinitionAuthorizerFunc(func(_ context.Context, request DefinitionAuthorization) error {
		allowed := allowedLocator.Load().(string)
		if request.Stage == AuthorizationResolved && request.Requested.ID == "" && allowed != "" &&
			(request.Resolved == nil || request.Resolved.Provenance == nil || request.Resolved.Provenance.Locator != allowed) {
			return errors.New("provenance is not authorized")
		}
		return nil
	})
	resolver := newTestDefinitionResolver(t, root, nil, authorizer)
	first, err := resolver.ResolvePlan(t.Context(), graph.DefinitionRef{Kind: DefinitionKindFile, ID: "relocatable", Locator: "first", Version: "1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolver.ResolvePlan(t.Context(), graph.DefinitionRef{Kind: DefinitionKindFile, ID: "relocatable", Locator: "second", Version: "1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest || first.Provenance.Locator == second.Provenance.Locator {
		t.Fatalf("relocated plans = %q/%q, %q/%q", first.Digest, second.Digest, first.Provenance.Locator, second.Provenance.Locator)
	}
	if _, loadErr := resolver.LoadPlan(t.Context(), first.Digest); !errors.Is(loadErr, ErrDefinitionPinConflict) {
		t.Fatalf("ambiguous digest load = %v", loadErr)
	}
	allowedLocator.Store(second.Provenance.Locator)
	loaded, err := resolver.LoadPlan(t.Context(), first.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Provenance.Locator != second.Provenance.Locator {
		t.Fatalf("authorized variant locator = %q", loaded.Provenance.Locator)
	}
	allowedLocator.Store(filepath.Join(root, "absent", "workflow.yaml"))
	if _, err := resolver.LoadPlan(t.Context(), first.Digest); !errors.Is(err, ErrDefinitionUnauthorized) {
		t.Fatalf("unauthorized variants error = %v", err)
	}
}

func TestDefinitionResolverRecursivelyUsesSameGraphNativeProvider(t *testing.T) {
	root := t.TempDir()
	rootSource := []byte(`workflow:
  name: root
  version: 1.0.0
steps:
  - id: invoke-child
    kind: call
    kind_version: v1
    call:
      definition:
        kind: registry
        id: child
        version: 1.0.0
      mode: inline
`)
	if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), rootSource, 0o600); err != nil {
		t.Fatal(err)
	}
	index := hadronregistry.NewWorkflowIndex()
	if _, err := index.RegisterWorkflow(t.Context(), hadronregistry.WorkflowRecord{
		Name: "child", Version: "1.0.0", Source: testWorkflowSource("child", "1.0.0", "noop"),
		Authority: "registry.test", TrustClass: "signed",
		Provenance: graph.Provenance{Origin: "publisher", Locator: "registry://child/1.0.0/child.workflow.yaml"},
	}, false); err != nil {
		t.Fatal(err)
	}
	resolver := newTestDefinitionResolver(t, root, index, DefinitionAuthorizerFunc(allowDefinitions))
	plan, err := resolver.ResolvePlan(t.Context(), graph.DefinitionRef{Kind: DefinitionKindFile, ID: "root", Locator: root, Version: "1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.ID != "root" || resolver.CacheStats().Plans != 2 {
		t.Fatalf("root/cache = %q %+v", plan.ID, resolver.CacheStats())
	}
	resolvedChild, err := resolver.ResolveDefinition(t.Context(), graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: "child", Version: "1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	if resolvedChild.Definition.Digest != resolvedChild.Graph.Digest || resolvedChild.Graph.Provenance.Metadata["source_digest"] == resolvedChild.Graph.Digest {
		t.Fatalf("call resolution identity = %+v / %+v", resolvedChild.Definition, resolvedChild.Graph.Provenance)
	}
}

func newTestDefinitionResolver(t *testing.T, root string, registry hadronregistry.WorkflowResolver, authorizer DefinitionAuthorizer) *DefinitionResolver {
	t.Helper()
	return newTestDefinitionResolverWithKinds(t, root, registry, authorizer, "noop")
}

func newTestDefinitionResolverWithKinds(t *testing.T, root string, registry hadronregistry.WorkflowResolver, authorizer DefinitionAuthorizer, kinds ...string) *DefinitionResolver {
	t.Helper()
	kindRegistry := stepkind.NewRegistry()
	allKinds := append([]string(nil), kinds...)
	allKinds = append(allKinds, "call")
	sort.Strings(allKinds)
	for index, name := range allKinds {
		if index != 0 && name == allKinds[index-1] {
			continue
		}
		if err := kindRegistry.Register(stepkindtest.NewNoopKind(name, "v1")); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{root}, Registry: registry, Authorizer: authorizer,
		Compile: DefinitionCompileOptions{StepKinds: kindRegistry, SemanticRevision: "definition-tests-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func allowDefinitions(context.Context, DefinitionAuthorization) error { return nil }

func testWorkflowSource(name, version, kind string) []byte {
	return []byte(fmt.Sprintf(`workflow:
  name: %s
  version: %s
steps:
  - id: root
    kind: %s
    kind_version: v1
`, name, version, kind))
}

func appWorkflowArchive(t *testing.T, entries map[string][]byte) []byte {
	t.Helper()
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	tarWriter := tar.NewWriter(gzipWriter)
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		contents := entries[name]
		if err := tarWriter.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(contents))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write(contents); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func containsStage(stages []DefinitionAuthorizationStage, wanted DefinitionAuthorizationStage) bool {
	for _, stage := range stages {
		if stage == wanted {
			return true
		}
	}
	return false
}

type divergentWorkflowResolver struct {
	source  []byte
	digest  string
	release chan struct{}
	calls   atomic.Int64
}

func (r *divergentWorkflowResolver) ResolveWorkflow(ctx context.Context, _ hadronregistry.WorkflowQuery) (hadronregistry.WorkflowResolution, error) {
	call := r.calls.Add(1)
	if call == 2 {
		close(r.release)
	}
	select {
	case <-r.release:
	case <-ctx.Done():
		return hadronregistry.WorkflowResolution{}, ctx.Err()
	}
	authority := fmt.Sprintf("registry-%d", call)
	return hadronregistry.WorkflowResolution{Record: hadronregistry.WorkflowRecord{
		Name: "divergent", Version: "1.0.0", Digest: r.digest, Source: bytes.Clone(r.source),
		Authority: authority, TrustClass: "signed",
		Provenance: graph.Provenance{
			Authority: authority, Origin: "publisher",
			Locator:  fmt.Sprintf("registry://divergent/%d/divergent.workflow.yaml", call),
			Revision: "1.0.0", Digest: r.digest, Metadata: graph.Metadata{"publisher": authority},
		},
	}}, nil
}

var _ hadronregistry.WorkflowResolver = (*divergentWorkflowResolver)(nil)

type countingWorkflowResolver struct {
	delegate hadronregistry.WorkflowResolver
	calls    atomic.Int64
}

func (r *countingWorkflowResolver) ResolveWorkflow(ctx context.Context, query hadronregistry.WorkflowQuery) (hadronregistry.WorkflowResolution, error) {
	r.calls.Add(1)
	return r.delegate.ResolveWorkflow(ctx, query)
}

type staticWorkflowResolver struct {
	resolution hadronregistry.WorkflowResolution
	err        error
}

func (r staticWorkflowResolver) ResolveWorkflow(context.Context, hadronregistry.WorkflowQuery) (hadronregistry.WorkflowResolution, error) {
	return r.resolution, r.err
}

var (
	_ hadronregistry.WorkflowResolver = (*countingWorkflowResolver)(nil)
	_ hadronregistry.WorkflowResolver = staticWorkflowResolver{}
)

func TestDefinitionResolverDiagnosticsRemainGraphNative(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "legacy.workflow.yaml"), []byte("blueprint:\n  name: forbidden\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	resolver := newTestDefinitionResolver(t, root, nil, DefinitionAuthorizerFunc(allowDefinitions))
	_, err := resolver.ResolvePlan(t.Context(), graph.DefinitionRef{Kind: DefinitionKindFile, Locator: "legacy.workflow.yaml"})
	var diagnosticErr *DefinitionDiagnosticError
	if !errors.As(err, &diagnosticErr) || len(diagnosticErr.Diagnostics()) == 0 {
		t.Fatalf("legacy error = %T %v", err, err)
	}
	for _, finding := range diagnosticErr.Diagnostics() {
		if !strings.HasPrefix(string(finding.Code), "HADR-") || finding.Source == nil {
			t.Fatalf("diagnostic = %+v", finding)
		}
	}
}

func TestDefinitionResolverRejectsBareWorkflowSuffix(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".workflow.yaml"), testWorkflowSource("hidden", "1.0.0", "noop"), 0o600); err != nil {
		t.Fatal(err)
	}
	resolver := newTestDefinitionResolver(t, root, nil, DefinitionAuthorizerFunc(allowDefinitions))
	if _, err := resolver.ResolveSource(t.Context(), graph.DefinitionRef{Kind: DefinitionKindFile, Locator: ".workflow.yaml"}); !errors.Is(err, ErrDefinitionUnresolved) {
		t.Fatalf("bare suffix error = %v", err)
	}
}
