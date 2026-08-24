package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/hollis-labs/hadron/workflow/conformance"
	"github.com/hollis-labs/hadron/workflow/stepkind"
)

type fakeHost struct {
	calls *int
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
			if fixture.Set == conformance.ExecutorMetadataFixtures {
				var input struct {
					Spec stepkind.StepKindSpec `json:"spec"`
				}
				if err := json.Unmarshal(fixture.Input, &input); err != nil {
					return fmt.Errorf("decode step-kind metadata input: %w", err)
				}
				(*h.calls)++
				return stepkind.ValidateSpec(input.Spec)
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

	const wantCalls = 12
	if calls != wantCalls {
		t.Fatalf("fixture calls = %d, want %d", calls, wantCalls)
	}
}

func TestEmbeddedFixtureTopology(t *testing.T) {
	sets := []conformance.FixtureSet{
		conformance.GraphValidationFixtures,
		conformance.SourceMapFixtures,
		conformance.ValueFixtures,
		conformance.SchedulerFixtures,
		conformance.WaitFixtures,
		conformance.ExecutorMetadataFixtures,
	}
	store := conformance.EmbeddedFixtures()

	for _, set := range sets {
		t.Run(string(set), func(t *testing.T) {
			fixtures, err := store.Fixtures(set)
			if err != nil {
				t.Fatal(err)
			}
			if len(fixtures) != 2 {
				t.Fatalf("fixture count = %d, want 2", len(fixtures))
			}
			if fixtures[0].Name != "minimal-fail" || fixtures[0].Expectation != conformance.ExpectFail {
				t.Fatalf("first fixture = %#v, want stable fail fixture", fixtures[0])
			}
			if fixtures[1].Name != "minimal-pass" || fixtures[1].Expectation != conformance.ExpectPass {
				t.Fatalf("second fixture = %#v, want stable pass fixture", fixtures[1])
			}
		})
	}
}
