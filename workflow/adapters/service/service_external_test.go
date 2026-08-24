package service_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	serviceadapter "github.com/hollis-labs/hadron/workflow/adapters/service"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestServiceKindRegistersTruthfulLifecycleAndDefensiveStart(t *testing.T) {
	host := &serviceHost{}
	registry := stepkind.NewRegistry()
	kind, err := serviceadapter.Register(registry, serviceadapter.Options{Host: host})
	if err != nil {
		t.Fatal(err)
	}
	spec := kind.Spec()
	if !spec.Lifecycle.Service || spec.Observation.Mode != stepkind.ObservationPoll || !spec.Observation.Heartbeat || spec.Cancellation.Mode != stepkind.CancellationExplicit || spec.EmbeddedModeSupported || spec.Idempotency != graph.IdempotencyKeyed {
		t.Fatalf("service spec = %#v", spec)
	}
	config := graph.Config{"provider": "fixture", "config": map[string]any{"replicas": 2}}
	prepared := stepkind.PreparedInvocation{Invocation: stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run", NodeID: "service", Attempt: 1},
		Config:   config, Inputs: values.ValueSet{}, IdempotencyKey: "service-key",
	}}
	result, err := kind.Execute(context.Background(), prepared)
	if err != nil || result.Outcome != stepkind.StepExternal || result.External == nil || result.External.ID != "service-1" {
		t.Fatalf("Execute() = %#v, %v", result, err)
	}
	config["provider"] = "mutated"
	config["config"].(map[string]any)["replicas"] = 99
	if host.start.Provider != "fixture" || fmt.Sprint(host.start.Config["replicas"]) != "2" {
		t.Fatalf("host request aliased caller config: %#v", host.start)
	}
}

func TestServiceKindRejectsTypedNilAndUnsafeProviderObservations(t *testing.T) {
	var typedNil *serviceHost
	if _, err := serviceadapter.New(serviceadapter.Options{Host: typedNil}); err == nil {
		t.Fatal("typed-nil host accepted")
	}
	host := &serviceHost{observation: stepkind.ServiceObservation{
		State:    stepkind.ServiceObservationStarting,
		Progress: map[string]string{"status": "token=super-secret-value"},
	}}
	kind, err := serviceadapter.New(serviceadapter.Options{Host: host})
	if err != nil {
		t.Fatal(err)
	}
	_, err = kind.ObserveService(context.Background(), stepkind.ExternalOperationRef{Kind: "fixture", ID: "service-1"})
	if err == nil || strings.Contains(err.Error(), "super-secret-value") {
		t.Fatalf("unsafe provider observation error = %v", err)
	}
	tooMany := make(map[string]string, 33)
	for i := 0; i < 33; i++ {
		tooMany[string(rune('a'+i))] = "ok"
	}
	if err := (stepkind.ServiceObservation{State: stepkind.ServiceObservationStarting, Progress: tooMany}).Validate(); err == nil {
		t.Fatal("unbounded service progress accepted")
	}
}

type serviceHost struct {
	start       serviceadapter.StartRequest
	observation stepkind.ServiceObservation
}

func (h *serviceHost) Start(_ context.Context, request serviceadapter.StartRequest) (stepkind.ExternalOperationRef, error) {
	h.start = request
	return stepkind.ExternalOperationRef{Kind: "fixture", ID: "service-1"}, nil
}

func (h *serviceHost) Observe(context.Context, stepkind.ExternalOperationRef) (stepkind.ServiceObservation, error) {
	if !h.observation.State.Valid() {
		return stepkind.ServiceObservation{State: stepkind.ServiceObservationStarting}, nil
	}
	return h.observation, nil
}

func (*serviceHost) RequestStop(context.Context, stepkind.ExternalOperationRef, string) error {
	return nil
}

var _ serviceadapter.Host = (*serviceHost)(nil)
