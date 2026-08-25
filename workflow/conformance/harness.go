package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

// Expectation describes whether a conformance fixture should be accepted or
// rejected by a runner.
type Expectation string

const (
	// ExpectPass marks a fixture that a conforming runner must accept.
	ExpectPass Expectation = "pass"
	// ExpectFail marks a fixture that a conforming runner must reject.
	ExpectFail Expectation = "fail"
)

// FixtureSet identifies a stable conformance fixture directory.
type FixtureSet string

const (
	// GraphValidationFixtures contains compiler graph-validation cases.
	GraphValidationFixtures FixtureSet = "graph-validation"
	// SourceMapFixtures contains compiler source-map cases.
	SourceMapFixtures FixtureSet = "source-maps"
	// ValueFixtures contains state-store value cases.
	ValueFixtures FixtureSet = "values"
	// SchedulerFixtures contains scheduler cases.
	SchedulerFixtures FixtureSet = "scheduler"
	// ControlFlowFixtures contains switch, catch, and finally scheduler cases.
	ControlFlowFixtures FixtureSet = "control-flow"
	// WaitFixtures contains wait and resume cases.
	WaitFixtures FixtureSet = "waits"
	// ExecutorMetadataFixtures contains step-kind registry metadata cases.
	ExecutorMetadataFixtures FixtureSet = "executor-metadata"
	// VerificationFixtures contains post-execution verification/evidence cases.
	VerificationFixtures FixtureSet = "verification"
	// MemoizationFixtures contains memo/pin provenance and safety cases.
	MemoizationFixtures FixtureSet = "memoization"
	// CompensationFixtures contains durable saga ordering and recovery cases.
	CompensationFixtures FixtureSet = "compensation"
)

// Fixture is a conformance-only test case. Input remains opaque to the harness
// so suites do not duplicate production workflow contracts.
type Fixture struct {
	Set         FixtureSet
	Name        string
	Path        string
	Expectation Expectation
	Input       json.RawMessage
}

// FixtureStore loads the fixtures for a stable fixture set. It is test-data
// storage and is unrelated to the workflow runtime state-store contract.
type FixtureStore interface {
	Fixtures(set FixtureSet) ([]Fixture, error)
}

// Runner executes one opaque conformance fixture through an implementation
// supplied by a host or adapter.
type Runner interface {
	Run(ctx context.Context, fixture Fixture) error
}

// RunnerFunc adapts a function to Runner.
type RunnerFunc func(ctx context.Context, fixture Fixture) error

// Run implements Runner.
func (f RunnerFunc) Run(ctx context.Context, fixture Fixture) error {
	return f(ctx, fixture)
}

// Factory creates an isolated runner for one fixture invocation.
type Factory func() (Runner, error)

// RequiredHost supplies factories for the stable required conformance suites.
// These factories are test seams, not production workflow host or runtime
// contracts.
type RequiredHost interface {
	CompilerFactory() Factory
	StateStoreFactory() Factory
	SchedulerFactory() Factory
	WaitFactory() Factory
	StepKindRegistryFactory() Factory
}

// Host is the original required-suite host contract.
//
// Deprecated: use RequiredHost for RunRequired or CompleteHost for RunComplete.
type Host = RequiredHost

// CompleteHost supplies the complete pre-compensation conformance family. It
// remains source-compatible for existing hosts; CompensationHost is the
// exhaustive extension.
type CompleteHost interface {
	RequiredHost
	VerificationFactory() Factory
	MemoizationFactory() Factory
}

// CompensationHost is the source-compatible exhaustive extension for hosts
// that implement the durable compensation fixture family. CompleteHost is
// intentionally unchanged.
type CompensationHost interface {
	CompleteHost
	CompensationFactory() Factory
}

// RunRequired invokes the stable required conformance suite set using one
// application-neutral host.
func RunRequired(t *testing.T, store FixtureStore, host RequiredHost) {
	t.Helper()
	if host == nil {
		t.Fatal("conformance: host is nil")
	}

	t.Run("compiler", func(t *testing.T) {
		CompilerSuite(t, store, host.CompilerFactory())
	})
	t.Run("state-store", func(t *testing.T) {
		StateStoreSuite(t, store, host.StateStoreFactory())
	})
	t.Run("scheduler", func(t *testing.T) {
		SchedulerSuite(t, store, host.SchedulerFactory())
	})
	t.Run("control-flow", func(t *testing.T) {
		ControlFlowSuite(t, store, host.SchedulerFactory())
	})
	t.Run("wait", func(t *testing.T) {
		WaitSuite(t, store, host.WaitFactory())
	})
	t.Run("step-kind-registry", func(t *testing.T) {
		StepKindRegistrySuite(t, store, host.StepKindRegistryFactory())
	})
}

// RunComplete invokes the source-compatible pre-compensation suite family,
// including verification and memoization.
func RunComplete(t *testing.T, store FixtureStore, host CompleteHost) {
	t.Helper()
	if host == nil {
		t.Fatal("conformance: complete host is nil")
	}
	RunRequired(t, store, host)
	t.Run("verification", func(t *testing.T) {
		VerificationSuite(t, store, host.VerificationFactory())
	})
	t.Run("memoization", func(t *testing.T) {
		MemoizationSuite(t, store, host.MemoizationFactory())
	})
}

// RunExhaustive invokes every current suite, including durable compensation.
func RunExhaustive(t *testing.T, store FixtureStore, host CompensationHost) {
	t.Helper()
	if host == nil {
		t.Fatal("conformance: compensation host is nil")
	}
	RunComplete(t, store, host)
	t.Run("compensation", func(t *testing.T) {
		CompensationSuite(t, store, host.CompensationFactory())
	})
}

// RunAll invokes the original required conformance suite set.
//
// Deprecated: its historical name predates optional verification and
// memoization families. Use RunRequired for that exact compatibility contract
// or RunComplete for the complete pre-compensation suite set.
func RunAll(t *testing.T, store FixtureStore, host Host) {
	t.Helper()
	RunRequired(t, store, host)
}

// CompilerSuite runs graph-validation and source-map fixtures.
func CompilerSuite(t *testing.T, store FixtureStore, factory Factory) {
	t.Helper()
	runSuite(t, "compiler", store, factory, GraphValidationFixtures, SourceMapFixtures)
}

// GraphValidationSuite runs only production graph-validation fixtures. It lets
// external compiler implementations invoke this contract without also
// implementing source-map fixture semantics.
func GraphValidationSuite(t *testing.T, store FixtureStore, factory Factory) {
	t.Helper()
	runSuite(t, "graph-validation", store, factory, GraphValidationFixtures)
}

// StateStoreSuite runs typed-value persistence fixtures.
func StateStoreSuite(t *testing.T, store FixtureStore, factory Factory) {
	t.Helper()
	runSuite(t, "state-store", store, factory, ValueFixtures)
}

// CompensationSuite runs graph-visible durable saga fixtures.
func CompensationSuite(t *testing.T, store FixtureStore, factory Factory) {
	t.Helper()
	runSuite(t, "compensation", store, factory, CompensationFixtures)
}

// SchedulerSuite runs ready-queue and scheduling fixtures.
func SchedulerSuite(t *testing.T, store FixtureStore, factory Factory) {
	t.Helper()
	runSuite(t, "scheduler", store, factory, SchedulerFixtures)
}

// ControlFlowSuite runs graph-native switch, catch, and finally fixtures using
// the scheduler/runtime factory without widening the extraction Host surface.
func ControlFlowSuite(t *testing.T, store FixtureStore, factory Factory) {
	t.Helper()
	runSuite(t, "control-flow", store, factory, ControlFlowFixtures)
}

// WaitSuite runs suspend and resume fixtures.
func WaitSuite(t *testing.T, store FixtureStore, factory Factory) {
	t.Helper()
	runSuite(t, "wait", store, factory, WaitFixtures)
}

// StepKindRegistrySuite runs executor-metadata registry fixtures.
func StepKindRegistrySuite(t *testing.T, store FixtureStore, factory Factory) {
	t.Helper()
	runSuite(t, "step-kind-registry", store, factory, ExecutorMetadataFixtures)
}

// VerificationSuite runs deterministic decision, literal evidence, retry,
// catch, and fail-closed reviewer fixtures independently of the combined host
// entry points.
func VerificationSuite(t *testing.T, store FixtureStore, factory Factory) {
	t.Helper()
	runSuite(t, "verification", store, factory, VerificationFixtures)
}

// MemoizationSuite runs production memo entry, pin binding, expiry, and effect
// safety fixtures independently of the combined host entry points.
func MemoizationSuite(t *testing.T, store FixtureStore, factory Factory) {
	t.Helper()
	runSuite(t, "memoization", store, factory, MemoizationFixtures)
}

func runSuite(t *testing.T, suite string, store FixtureStore, factory Factory, sets ...FixtureSet) {
	t.Helper()
	if store == nil {
		t.Fatalf("conformance %s: fixture store is nil", suite)
	}
	if factory == nil {
		t.Fatalf("conformance %s: runner factory is nil", suite)
	}

	for _, set := range sets {
		fixtures, err := store.Fixtures(set)
		if err != nil {
			t.Fatalf("conformance %s: load %s fixtures: %v", suite, set, err)
		}
		if len(fixtures) == 0 {
			t.Fatalf("conformance %s: fixture set %s is empty", suite, set)
		}

		for _, fixture := range fixtures {
			fixture.Set = set
			t.Run(string(set)+"/"+fixture.Name, func(t *testing.T) {
				runner, err := factory()
				if err != nil {
					t.Errorf("conformance %s/%s/%s: create runner: %v", suite, set, fixture.Name, err)
					return
				}
				if runner == nil {
					t.Errorf("conformance %s/%s/%s: factory returned nil runner", suite, set, fixture.Name)
					return
				}

				runErr := runner.Run(t.Context(), fixture)
				if err := checkOutcome(suite, fixture, runErr); err != nil {
					t.Error(err)
				}
			})
		}
	}
}

func checkOutcome(suite string, fixture Fixture, runErr error) error {
	id := fmt.Sprintf("%s/%s/%s", suite, fixture.Set, fixture.Name)
	switch fixture.Expectation {
	case ExpectPass:
		if runErr != nil {
			return fmt.Errorf("conformance %s: expected pass, got error: %w", id, runErr)
		}
	case ExpectFail:
		if runErr == nil {
			return fmt.Errorf("conformance %s: expected failure, got success", id)
		}
	default:
		return fmt.Errorf("conformance %s: unknown expectation %q", id, fixture.Expectation)
	}
	return nil
}
