package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/hollis-labs/hadron/workflow/conformance"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

type fakeHost struct {
	calls *int
}

type completeFakeHost struct{ fakeHost }

type compensationFakeHost struct{ completeFakeHost }

func (h compensationFakeHost) CompensationFactory() conformance.Factory {
	return func() (conformance.Runner, error) {
		return conformance.RunnerFunc(func(_ context.Context, fixture conformance.Fixture) error {
			var input struct {
				Scenario string `json:"scenario"`
			}
			if err := json.Unmarshal(fixture.Input, &input); err != nil {
				return err
			}
			(*h.calls)++
			switch input.Scenario {
			case "reverse_order", "independent_parallel", "crash_recovery", "stable_retry", "separate_cancel", "nested_child", "partial", "failed", "replay":
				return nil
			case "unsupported_claim":
				return errors.New("reversibility evidence is unsupported")
			default:
				return fmt.Errorf("unsupported compensation scenario %q", input.Scenario)
			}
		}), nil
	}
}

func (h completeFakeHost) VerificationFactory() conformance.Factory {
	return func() (conformance.Runner, error) {
		return conformance.RunnerFunc(func(ctx context.Context, fixture conformance.Fixture) error {
			(*h.calls)++
			return runVerificationFixture(ctx, fixture)
		}), nil
	}
}

func (h completeFakeHost) MemoizationFactory() conformance.Factory {
	return func() (conformance.Runner, error) {
		return conformance.RunnerFunc(func(ctx context.Context, fixture conformance.Fixture) error {
			(*h.calls)++
			return runMemoFixture(ctx, fixture)
		}), nil
	}
}

func (h fakeHost) CompilerFactory() conformance.Factory {
	return h.factory()
}

func (h fakeHost) StateStoreFactory() conformance.Factory {
	return h.factory()
}

func (h fakeHost) SchedulerFactory() conformance.Factory {
	return h.factory()
}

func (h fakeHost) WaitFactory() conformance.Factory {
	return h.factory()
}

func (h fakeHost) StepKindRegistryFactory() conformance.Factory {
	return h.factory()
}

func (h fakeHost) factory() conformance.Factory {
	return func() (conformance.Runner, error) {
		return conformance.RunnerFunc(func(_ context.Context, fixture conformance.Fixture) error {
			if fixture.Set == conformance.ControlFlowFixtures {
				var input struct {
					Scenario string `json:"scenario"`
				}
				if err := json.Unmarshal(fixture.Input, &input); err != nil {
					return fmt.Errorf("decode control-flow input: %w", err)
				}
				(*h.calls)++
				switch input.Scenario {
				case "switch", "catch", "finally", "completion":
					return nil
				default:
					return fmt.Errorf("unsupported control-flow scenario %q", input.Scenario)
				}
			}
			if fixture.Set == conformance.WaitFixtures {
				var input struct {
					Kind        workflowwait.Kind       `json:"kind"`
					WakeSource  workflowwait.WakeSource `json:"wake_source"`
					Correlation string                  `json:"correlation"`
				}
				if err := json.Unmarshal(fixture.Input, &input); err != nil {
					return fmt.Errorf("decode wait input: %w", err)
				}
				schema, err := workflowwait.NewSchemaRef(nil)
				if err != nil {
					return err
				}
				(*h.calls)++
				return (workflowwait.Record{Kind: input.Kind, Correlation: input.Correlation, ResumeSchema: schema, Visibility: workflowwait.VisibilityPrivate, Authority: workflowwait.ResponderAuthority{Kind: "fixture"}, WakeSource: input.WakeSource, Status: workflowwait.StatusOpen}).Validate()
			}
			if fixture.Set == conformance.SchedulerFixtures {
				(*h.calls)++
				return runSchedulerFixture(context.Background(), fixture)
			}

			if fixture.Set == conformance.ExecutorMetadataFixtures {
				var input struct {
					Operation string                `json:"operation"`
					Name      string                `json:"name"`
					Version   string                `json:"version"`
					Config    graph.Config          `json:"config"`
					Spec      stepkind.StepKindSpec `json:"spec"`
				}
				if err := json.Unmarshal(fixture.Input, &input); err != nil {
					return fmt.Errorf("decode step-kind metadata input: %w", err)
				}
				(*h.calls)++
				switch input.Operation {
				case "":
					return stepkind.ValidateSpec(input.Spec)
				case "duplicate_registration":
					registry := stepkind.NewRegistry()
					if err := registry.Register(stepkindtest.NewNoopKind(input.Name, input.Version)); err != nil {
						return err
					}
					return registry.Register(stepkindtest.NewNoopKind(input.Name, input.Version))
				case "resolve":
					_, _, err := stepkind.Resolve(stepkind.NewRegistry(), input.Name, input.Version)
					return err
				case "validate_config":
					kind := stepkindtest.NewNoopKind(input.Name, input.Version)
					kind.ValidateConfigFunc = func(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
						if accepted, _ := config["accepted"].(bool); accepted {
							return nil
						}
						return []diagnostic.Diagnostic{{
							Severity: diagnostic.SeverityError, Code: stepkind.CodeInvalidConfig,
							Message: "config is not accepted",
						}}
					}
					return errors.Join(diagnosticErrors(kind.ValidateConfig(context.Background(), input.Config))...)
				case "optional_lifecycle":
					kind := stepkindtest.NewLifecycleKind(input.Name, input.Version)
					if err := stepkind.NewRegistry().Register(kind); err != nil {
						return err
					}
					_, prepares := any(kind).(stepkind.Preparer)
					_, observes := any(kind).(stepkind.Observer)
					_, heartbeats := any(kind).(stepkind.Heartbeater)
					_, cancels := any(kind).(stepkind.Canceler)
					_, finalizes := any(kind).(stepkind.Finalizer)
					if !prepares || !observes || !heartbeats || !cancels || !finalizes {
						return errors.New("optional lifecycle is incomplete")
					}
					return nil
				case "immutable_snapshot":
					registry := stepkind.NewRegistry()
					kind := stepkindtest.NewNoopKind(input.Name, input.Version)
					if err := registry.Register(kind); err != nil {
						return err
					}
					kind.SpecValue.Name = "mutated"
					kind.SpecValue.OutputSchema = graph.Schema{"not": graph.Schema{}}
					_, spec, err := stepkind.Resolve(registry, input.Name, input.Version)
					if err != nil {
						return err
					}
					if spec.Name != input.Name || len(spec.OutputSchema) != 0 {
						return errors.New("registered metadata snapshot changed")
					}
					return nil
				default:
					return fmt.Errorf("unsupported executor metadata operation %q", input.Operation)
				}
			}

			var input struct {
				Accepted bool `json:"accepted"`
			}
			if err := json.Unmarshal(fixture.Input, &input); err != nil {
				return fmt.Errorf("decode fake input: %w", err)
			}
			(*h.calls)++
			if !input.Accepted {
				return errors.New("fixture rejected")
			}
			return nil
		}), nil
	}
}

func TestExternalHostRunsAllSuites(t *testing.T) {
	calls := 0
	conformance.RunAll(t, conformance.EmbeddedFixtures(), fakeHost{calls: &calls})

	const wantCalls = 44
	if calls != wantCalls {
		t.Fatalf("fixture calls = %d, want %d", calls, wantCalls)
	}
}

func TestExternalHostRunsCompleteSuites(t *testing.T) {
	calls := 0
	conformance.RunComplete(t, conformance.EmbeddedFixtures(), completeFakeHost{fakeHost{calls: &calls}})

	const wantCalls = 54
	if calls != wantCalls {
		t.Fatalf("fixture calls = %d, want %d", calls, wantCalls)
	}
}

func TestExternalHostRunsExhaustiveCompensationSuiteWithoutWideningCompleteHost(t *testing.T) {
	calls := 0
	conformance.RunExhaustive(t, conformance.EmbeddedFixtures(), compensationFakeHost{completeFakeHost{fakeHost{calls: &calls}}})

	const wantCalls = 64
	if calls != wantCalls {
		t.Fatalf("fixture calls = %d, want %d", calls, wantCalls)
	}
	var _ conformance.CompleteHost = completeFakeHost{}
}

func TestEmbeddedFixtureTopology(t *testing.T) {
	sets := []conformance.FixtureSet{
		conformance.GraphValidationFixtures,
		conformance.SourceMapFixtures,
		conformance.ValueFixtures,
		conformance.SchedulerFixtures,
		conformance.ControlFlowFixtures,
		conformance.WaitFixtures,
		conformance.ExecutorMetadataFixtures,
		conformance.VerificationFixtures,
		conformance.MemoizationFixtures,
		conformance.CompensationFixtures,
	}
	store := conformance.EmbeddedFixtures()

	for _, set := range sets {
		t.Run(string(set), func(t *testing.T) {
			fixtures, err := store.Fixtures(set)
			if err != nil {
				t.Fatal(err)
			}
			wantCount := 2
			if set == conformance.GraphValidationFixtures {
				wantCount = 7
			}
			if set == conformance.SchedulerFixtures {
				wantCount = 10
			}
			if set == conformance.WaitFixtures {
				wantCount = 8
			}
			if set == conformance.ExecutorMetadataFixtures {
				wantCount = 9
			}
			if set == conformance.ControlFlowFixtures {
				wantCount = 6
			}
			if set == conformance.VerificationFixtures {
				wantCount = 6
			}
			if set == conformance.MemoizationFixtures {
				wantCount = 4
			}
			if set == conformance.CompensationFixtures {
				wantCount = 10
			}
			if len(fixtures) != wantCount {
				t.Fatalf("fixture count = %d, want %d", len(fixtures), wantCount)
			}
			byName := make(map[string]conformance.Fixture, len(fixtures))
			for _, fixture := range fixtures {
				byName[fixture.Name] = fixture
			}
			if set == conformance.SchedulerFixtures {
				for _, name := range []string{
					"readiness-all-success", "readiness-all-done", "readiness-one-failed",
					"readiness-all-failed", "readiness-none-failed", "readiness-always",
				} {
					if fixture := byName[name]; fixture.Expectation != conformance.ExpectPass {
						t.Fatalf("%s fixture = %#v, want semantic pass fixture", name, fixture)
					}
				}
				if fixture := byName["readiness-unsupported-rule"]; fixture.Expectation != conformance.ExpectFail {
					t.Fatalf("unsupported readiness fixture = %#v, want semantic fail fixture", fixture)
				}
				for _, name := range []string{"resource-cross-run", "run-policy-fail-fast", "run-policy-run-to-completion"} {
					if fixture := byName[name]; fixture.Expectation != conformance.ExpectPass {
						t.Fatalf("%s fixture = %#v, want scheduler policy pass fixture", name, fixture)
					}
				}
				return
			}
			if set == conformance.CompensationFixtures {
				for _, name := range []string{
					"compensation-reverse-order", "compensation-independent-parallel", "compensation-crash-recovery",
					"compensation-stable-retry", "compensation-separate-cancel", "compensation-nested-child-ledger",
					"compensation-partial-best-effort", "compensation-all-handlers-failed", "compensation-forward-replay-fence",
				} {
					if byName[name].Expectation != conformance.ExpectPass {
						t.Fatalf("%s fixture = %#v", name, byName[name])
					}
				}
				if byName["compensation-unsupported-reversibility"].Expectation != conformance.ExpectFail {
					t.Fatalf("unsupported compensation fixture = %#v", byName["compensation-unsupported-reversibility"])
				}
				return
			}
			if set == conformance.WaitFixtures {
				for _, name := range []string{"wait-gate", "wait-checkpoint-gate", "wait-message", "wait-timer", "wait-callback", "wait-child-run", "wait-signal"} {
					if fixture := byName[name]; fixture.Expectation != conformance.ExpectPass {
						t.Fatalf("%s fixture = %#v", name, fixture)
					}
				}
				if fixture := byName["wait-unsupported-source"]; fixture.Expectation != conformance.ExpectFail {
					t.Fatalf("unsupported wait fixture = %#v", fixture)
				}
				return
			}
			if set == conformance.ControlFlowFixtures {
				for _, name := range []string{"switch-default", "catch", "continue-on-error", "timeout-catch", "nested-finally", "cleanup-failure"} {
					if fixture := byName[name]; fixture.Expectation != conformance.ExpectPass {
						t.Fatalf("%s fixture = %#v", name, fixture)
					}
				}
				return
			}
			if set == conformance.VerificationFixtures {
				for _, name := range []string{"verification-deterministic-pass", "verification-retry-safety", "verification-catch-route"} {
					if fixture := byName[name]; fixture.Expectation != conformance.ExpectPass {
						t.Fatalf("%s fixture = %#v", name, fixture)
					}
				}
				for _, name := range []string{"verification-deterministic-fail", "verification-missing-evidence", "verification-reviewer-malformed"} {
					if fixture := byName[name]; fixture.Expectation != conformance.ExpectFail {
						t.Fatalf("%s fixture = %#v", name, fixture)
					}
				}
				return
			}
			if set == conformance.MemoizationFixtures {
				for _, name := range []string{"memo-safe-entry", "memo-pin-binding"} {
					if byName[name].Expectation != conformance.ExpectPass {
						t.Fatalf("%s fixture = %#v", name, byName[name])
					}
				}
				for _, name := range []string{"memo-expired", "memo-unsafe-effect"} {
					if byName[name].Expectation != conformance.ExpectFail {
						t.Fatalf("%s fixture = %#v", name, byName[name])
					}
				}
				return
			}
			if fixture := byName["minimal-fail"]; fixture.Expectation != conformance.ExpectFail {
				t.Fatalf("minimal-fail fixture = %#v, want stable fail fixture", fixture)
			}
			if fixture := byName["minimal-pass"]; fixture.Expectation != conformance.ExpectPass {
				t.Fatalf("minimal-pass fixture = %#v, want stable pass fixture", fixture)
			}
		})
	}
}

func diagnosticErrors(findings []diagnostic.Diagnostic) []error {
	errs := make([]error, 0, len(findings))
	for _, finding := range findings {
		if finding.Severity == diagnostic.SeverityError {
			errs = append(errs, errors.New(finding.Message))
		}
	}
	return errs
}
