package stepkind_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
)

func TestRegistryLookupListAndDefensiveCopies(t *testing.T) {
	t.Parallel()

	registry := stepkind.NewRegistry()
	var _ stepkind.Registry = registry

	zeta := stepkindtest.NewNoopKind("zeta", "v1")
	alphaV2 := stepkindtest.NewNoopKind("alpha", "v2")
	alphaV2.SpecValue.Effects = graph.EffectSet{graph.EffectRead, graph.EffectCompute}
	alphaV2.SpecValue.RequiredCapabilities = []string{"network", "filesystem"}
	alphaV2.SpecValue.ConfigSchema = graph.Schema{
		"properties": map[string]any{"enabled": map[string]any{"type": "boolean"}},
	}
	alphaV1 := stepkindtest.NewNoopKind("alpha", "v1")
	for _, kind := range []stepkind.StepKind{zeta, alphaV2, alphaV1} {
		if err := registry.Register(kind); err != nil {
			t.Fatalf("Register(%s) error = %v", kind.Spec().Name, err)
		}
	}

	got, ok := registry.Lookup("alpha", "v2")
	if !ok || got != alphaV2 {
		t.Fatalf("Lookup(alpha, v2) = %T, %t; want registered kind, true", got, ok)
	}
	if got, ok := registry.Lookup("alpha", "v3"); ok || got != nil {
		t.Fatalf("Lookup(alpha, v3) = %T, %t; want nil, false", got, ok)
	}

	wantOrder := []string{"alpha@v1", "alpha@v2", "zeta@v1"}
	if gotOrder := specOrder(registry.List()); !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Fatalf("List() order = %v, want %v", gotOrder, wantOrder)
	}
	listed := registry.List()
	if got, want := listed[1].Effects, (graph.EffectSet{graph.EffectCompute, graph.EffectRead}); !reflect.DeepEqual(got, want) {
		t.Errorf("normalized effects = %v, want %v", got, want)
	}
	if got, want := listed[1].RequiredCapabilities, []string{"filesystem", "network"}; !reflect.DeepEqual(got, want) {
		t.Errorf("normalized capabilities = %v, want %v", got, want)
	}

	alphaV2.SpecValue.ConfigSchema["mutated"] = true
	listed[1].ConfigSchema["mutated-through-list"] = true
	nested := listed[1].ConfigSchema["properties"].(map[string]any)
	nested["enabled"] = map[string]any{"type": "string"}
	again := registry.List()[1].ConfigSchema
	if _, mutated := again["mutated"]; mutated {
		t.Error("registration metadata changed through executor-owned schema")
	}
	if _, mutated := again["mutated-through-list"]; mutated {
		t.Error("registration metadata changed through List result")
	}
	if got := again["properties"].(map[string]any)["enabled"].(map[string]any)["type"]; got != "boolean" {
		t.Errorf("nested registered schema type = %v, want boolean", got)
	}
}

func TestRegistryDeepCopiesTypedSchemaContainers(t *testing.T) {
	t.Parallel()

	type namedMap map[string]string
	type namedSlice []map[string]any
	type namedArray [1]map[string]string

	typedMap := namedMap{"type": "string"}
	typedSlice := namedSlice{{"type": "boolean"}}
	typedArray := namedArray{{"type": "number"}}
	kind := stepkindtest.NewNoopKind("typed-schema", "v1")
	kind.SpecValue.ConfigSchema = graph.Schema{
		"typed_map":   typedMap,
		"typed_slice": typedSlice,
		"typed_array": typedArray,
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	typedMap["type"] = "integer"
	typedSlice[0]["type"] = "integer"
	typedArray[0]["type"] = "integer"
	listed := registry.List()[0].ConfigSchema
	if got := listed["typed_map"].(namedMap)["type"]; got != "string" {
		t.Errorf("typed map value = %q, want string", got)
	}
	if got := listed["typed_slice"].(namedSlice)[0]["type"]; got != "boolean" {
		t.Errorf("typed slice value = %q, want boolean", got)
	}
	if got := listed["typed_array"].(namedArray)[0]["type"]; got != "number" {
		t.Errorf("typed array value = %q, want number", got)
	}

	listed["typed_map"].(namedMap)["type"] = "null"
	listed["typed_slice"].(namedSlice)[0]["type"] = "null"
	arrayCopy := listed["typed_array"].(namedArray)
	arrayCopy[0]["type"] = "null"
	again := registry.List()[0].ConfigSchema
	if got := again["typed_map"].(namedMap)["type"]; got != "string" {
		t.Errorf("typed map changed through List() = %q", got)
	}
	if got := again["typed_slice"].(namedSlice)[0]["type"]; got != "boolean" {
		t.Errorf("typed slice changed through List() = %q", got)
	}
	if got := again["typed_array"].(namedArray)[0]["type"]; got != "number" {
		t.Errorf("typed array changed through List() = %q", got)
	}
}

func TestRegistryRejectsDuplicateRegistration(t *testing.T) {
	t.Parallel()

	registry := stepkind.NewRegistry()
	if err := registry.Register(stepkindtest.NewNoopKind("noop", "v1")); err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	err := registry.Register(stepkindtest.NewNoopKind("noop", "v1"))
	var duplicate *stepkind.DuplicateRegistrationError
	if !errors.As(err, &duplicate) {
		t.Fatalf("second Register() error = %T %v, want *DuplicateRegistrationError", err, err)
	}
	if duplicate.Name != "noop" || duplicate.Version != "v1" {
		t.Fatalf("duplicate identity = %s@%s, want noop@v1", duplicate.Name, duplicate.Version)
	}
}

func TestRegistryRejectsInvalidSpecs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(*stepkind.StepKindSpec)
	}{
		{"missing name", func(spec *stepkind.StepKindSpec) { spec.Name = "" }},
		{"invalid version", func(spec *stepkind.StepKindSpec) { spec.Version = "bad version" }},
		{"missing config schema", func(spec *stepkind.StepKindSpec) { spec.ConfigSchema = nil }},
		{"missing input schema", func(spec *stepkind.StepKindSpec) { spec.InputSchema = nil }},
		{"missing output schema", func(spec *stepkind.StepKindSpec) { spec.OutputSchema = nil }},
		{"missing effects", func(spec *stepkind.StepKindSpec) { spec.Effects = nil }},
		{"invalid effect", func(spec *stepkind.StepKindSpec) { spec.Effects = graph.EffectSet{"launch"} }},
		{"duplicate effect", func(spec *stepkind.StepKindSpec) { spec.Effects = graph.EffectSet{graph.EffectRead, graph.EffectRead} }},
		{"invalid idempotency", func(spec *stepkind.StepKindSpec) { spec.Idempotency = "automatic" }},
		{"invalid retry safety", func(spec *stepkind.StepKindSpec) { spec.RetrySafety = "sometimes" }},
		{"invalid cancellation", func(spec *stepkind.StepKindSpec) { spec.Cancellation.Mode = "signal" }},
		{"invalid observation", func(spec *stepkind.StepKindSpec) { spec.Observation.Mode = "push" }},
		{"empty capability", func(spec *stepkind.StepKindSpec) { spec.RequiredCapabilities = []string{""} }},
		{"invalid UTF-8 capability", func(spec *stepkind.StepKindSpec) { spec.RequiredCapabilities = []string{string([]byte{0xff})} }},
		{"duplicate capability", func(spec *stepkind.StepKindSpec) { spec.RequiredCapabilities = []string{"network", "network"} }},
		{"non-finite schema number", func(spec *stepkind.StepKindSpec) { spec.ConfigSchema = graph.Schema{"maximum": math.Inf(1)} }},
		{"invalid JSON number", func(spec *stepkind.StepKindSpec) { spec.ConfigSchema = graph.Schema{"maximum": json.Number("01")} }},
		{"invalid raw JSON", func(spec *stepkind.StepKindSpec) { spec.ConfigSchema = graph.Schema{"fragment": json.RawMessage(`{`)} }},
		{"cyclic schema", func(spec *stepkind.StepKindSpec) {
			cycle := map[string]any{}
			cycle["self"] = cycle
			spec.ConfigSchema = graph.Schema{"cycle": cycle}
		}},
		{"non-JSON schema value", func(spec *stepkind.StepKindSpec) { spec.ConfigSchema = graph.Schema{"invalid": make(chan int)} }},
		{"non-string schema map key", func(spec *stepkind.StepKindSpec) {
			spec.ConfigSchema = graph.Schema{"invalid": map[int]string{1: "one"}}
		}},
		{"invalid UTF-8 schema value", func(spec *stepkind.StepKindSpec) { spec.ConfigSchema = graph.Schema{"title": string([]byte{0xff})} }},
		{"invalid UTF-8 schema key", func(spec *stepkind.StepKindSpec) { spec.ConfigSchema = graph.Schema{string([]byte{0xff}): true} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kind := stepkindtest.NewNoopKind("noop", "v1")
			tc.mutate(&kind.SpecValue)
			err := stepkind.NewRegistry().Register(kind)
			var specErr *stepkind.SpecError
			if !errors.As(err, &specErr) {
				t.Fatalf("Register() error = %T %v, want *SpecError", err, err)
			}
			if len(specErr.Diagnostics) == 0 {
				t.Fatal("SpecError has no structured diagnostics")
			}
			for _, finding := range specErr.Diagnostics {
				if finding.Code != stepkind.CodeInvalidSpec {
					t.Errorf("diagnostic code = %q, want %q", finding.Code, stepkind.CodeInvalidSpec)
				}
				if err := finding.Validate(); err != nil {
					t.Errorf("malformed diagnostic: %v", err)
				}
			}
		})
	}
}

func TestRegistryRejectsNilKinds(t *testing.T) {
	t.Parallel()

	var typedNil *stepkindtest.Kind
	for _, kind := range []stepkind.StepKind{nil, typedNil} {
		err := stepkind.NewRegistry().Register(kind)
		var specErr *stepkind.SpecError
		if !errors.As(err, &specErr) {
			t.Fatalf("Register(%T) error = %T %v, want *SpecError", kind, err, err)
		}
	}
}

func TestRegistryRequiresLifecycleMetadataAndInterfacesToMatch(t *testing.T) {
	t.Parallel()

	advertisedWithoutInterface := stepkindtest.NewNoopKind("prepare-missing", "v1")
	advertisedWithoutInterface.SpecValue.Lifecycle.Prepare = true
	assertLifecycleMismatch(t, advertisedWithoutInterface)

	implementedWithoutAdvertisement := stepkindtest.NewLifecycleKind("prepare-hidden", "v1")
	implementedWithoutAdvertisement.SpecValue.Lifecycle.Prepare = false
	assertLifecycleMismatch(t, implementedWithoutAdvertisement)

	complete := stepkindtest.NewLifecycleKind("complete", "v1")
	if err := stepkind.NewRegistry().Register(complete); err != nil {
		t.Fatalf("Register(complete lifecycle) error = %v", err)
	}
}

func TestRegistryConcurrentAccess(t *testing.T) {
	t.Parallel()

	registry := stepkind.NewRegistry()
	var wait sync.WaitGroup
	for i := range 20 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			name := fmt.Sprintf("kind-%02d", i)
			if err := registry.Register(stepkindtest.NewNoopKind(name, "v1")); err != nil {
				t.Errorf("Register(%s) error = %v", name, err)
			}
			registry.Lookup(name, "v1")
			registry.List()
		}()
	}
	wait.Wait()
	if got := len(registry.List()); got != 20 {
		t.Fatalf("registered specs = %d, want 20", got)
	}
}

func TestExternalRequiredContractAndConfigDiagnostics(t *testing.T) {
	t.Parallel()

	kind := stepkindtest.NewNoopKind("external-noop", "v1")
	kind.ValidateConfigFunc = func(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
		if enabled, ok := config["enabled"].(bool); ok && enabled {
			return nil
		}
		return []diagnostic.Diagnostic{{
			Severity: diagnostic.SeverityError,
			Code:     stepkind.CodeInvalidConfig,
			Message:  "enabled must be true",
		}}
	}
	var executed graph.Config
	kind.ExecuteFunc = func(_ context.Context, invocation stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executed = invocation.Invocation.Config
		return stepkind.StepResult{}, nil
	}

	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	registered, ok := registry.Lookup("external-noop", "v1")
	if !ok {
		t.Fatal("Lookup() did not find external kind")
	}
	if diagnostics := registered.ValidateConfig(t.Context(), graph.Config{"enabled": false}); len(diagnostics) != 1 || diagnostics[0].Code != diagnostic.Code("HADR-SOURCE-100") {
		t.Fatalf("invalid config diagnostics = %#v", diagnostics)
	}
	config := graph.Config{"enabled": true}
	if diagnostics := registered.ValidateConfig(t.Context(), config); len(diagnostics) != 0 {
		t.Fatalf("valid config diagnostics = %#v, want none", diagnostics)
	}
	_, err := registered.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: stepkind.Invocation{Config: config}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !reflect.DeepEqual(executed, config) {
		t.Fatalf("executed config = %#v, want %#v", executed, config)
	}
}

func TestExternalOptionalLifecycleInvocation(t *testing.T) {
	t.Parallel()

	var calls []string
	kind := stepkindtest.NewLifecycleKind("external-lifecycle", "v1")
	kind.PrepareFunc = func(_ context.Context, invocation stepkind.Invocation) (stepkind.PreparedInvocation, error) {
		calls = append(calls, "prepare")
		return stepkind.PreparedInvocation{Invocation: invocation}, nil
	}
	kind.ExecuteFunc = func(_ context.Context, _ stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		calls = append(calls, "execute")
		return stepkind.StepResult{}, nil
	}
	kind.ObserveFunc = func(_ context.Context, ref stepkind.ExternalOperationRef) (stepkind.Observation, error) {
		calls = append(calls, "observe:"+ref.ID)
		return stepkind.Observation{Complete: true}, nil
	}
	kind.CancelFunc = func(_ context.Context, ref stepkind.ExternalOperationRef) error {
		calls = append(calls, "cancel:"+ref.ID)
		return nil
	}
	kind.FinalizeFunc = func(_ context.Context, _ stepkind.Finalization) error {
		calls = append(calls, "finalize")
		return nil
	}

	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	registered, _ := registry.Lookup("external-lifecycle", "v1")
	prepared, err := registered.(stepkind.Preparer).Prepare(t.Context(), stepkind.Invocation{Config: graph.Config{}})
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	result, err := registered.Execute(t.Context(), prepared)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	ref := stepkind.ExternalOperationRef{ID: "operation-1"}
	observation, err := registered.(stepkind.Observer).Observe(t.Context(), ref)
	if err != nil || !observation.Complete {
		t.Fatalf("Observe() = %#v, %v; want complete, nil", observation, err)
	}
	if err := registered.(stepkind.Canceler).Cancel(t.Context(), ref); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if err := registered.(stepkind.Finalizer).Finalize(t.Context(), stepkind.Finalization{Invocation: prepared, Result: result}); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}

	want := []string{"prepare", "execute", "observe:operation-1", "cancel:operation-1", "finalize"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("lifecycle calls = %v, want %v", calls, want)
	}
}

func assertLifecycleMismatch(t *testing.T, kind stepkind.StepKind) {
	t.Helper()
	err := stepkind.NewRegistry().Register(kind)
	var specErr *stepkind.SpecError
	if !errors.As(err, &specErr) {
		t.Fatalf("Register() error = %T %v, want *SpecError", err, err)
	}
	found := false
	for _, finding := range specErr.Diagnostics {
		found = found || finding.Code == stepkind.CodeLifecycleMismatch
	}
	if !found {
		t.Fatalf("diagnostics = %#v, want %q", specErr.Diagnostics, stepkind.CodeLifecycleMismatch)
	}
}

func specOrder(specs []stepkind.StepKindSpec) []string {
	ordered := make([]string, len(specs))
	for i, spec := range specs {
		ordered[i] = spec.Name + "@" + spec.Version
	}
	return ordered
}
