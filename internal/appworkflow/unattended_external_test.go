package appworkflow_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	workflowruntime "github.com/hollis-labs/libs/workflow/runtime"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/unattended"
)

// A confirmed start records who confirmed it on the immutable start record.
// Confirmed stays out of the request digest, so the standard 409 followed by a
// same-key confirmed retry replays the stored Confirm decision and succeeds.
func TestStartRecordsWhoConfirmed(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyConfirm, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-who-confirmed", "key-who-confirmed", "user:alice")
	caller := authenticatedContext(t.Context(), "user:alice")
	if _, err := fixture.host.StartRun(caller, request); !errors.Is(err, appworkflow.ErrConfirmationRequired) {
		t.Fatalf("unconfirmed start = %v", err)
	}
	request.Confirmed = true
	started, err := fixture.host.StartRun(caller, request)
	if err != nil || started.Run == nil || started.Decision.Outcome != hoststate.PolicyConfirm {
		t.Fatalf("same-key confirmed retry = %#v, %v", started, err)
	}
	if fixture.policyCalls.Load() != 1 {
		t.Fatalf("retry re-evaluated policy: calls=%d", fixture.policyCalls.Load())
	}
	stored, err := fixture.journal.LoadStartByKey(t.Context(), request.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	want := hoststate.StartConfirmation{Confirmed: true, ConfirmedBy: "user:alice"}
	if stored.Record.Confirmation == nil || *stored.Record.Confirmation != want {
		t.Fatalf("start confirmation = %#v, want %#v", stored.Record.Confirmation, want)
	}
}

// A plain Allow needs no confirmation and records none.
func TestAllowedStartRecordsNoConfirmation(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-plain-allow", "key-plain-allow", "user:bob")
	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:bob"), request); err != nil {
		t.Fatal(err)
	}
	stored, err := fixture.journal.LoadStartByKey(t.Context(), request.IdempotencyKey)
	if err != nil || stored.Record.Confirmation != nil {
		t.Fatalf("confirmation = %#v, %v", stored.Record.Confirmation, err)
	}
}

// A dry run has no effects, so it does not ask for confirmation. It still
// needs dry-run support, and a denial still stops it.
func TestDryRunSkipsConfirmation(t *testing.T) {
	t.Run("supported", func(t *testing.T) {
		fixture := newHostFixture(t, hoststate.PolicyConfirm, time.Hour, nil)
		fixture.dryRunSupported.Store(true)
		if err := fixture.host.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
		request := fixture.startRequest("run-dry", "key-dry", "user:dry")
		request.DryRun = true
		result, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:dry"), request)
		if err != nil || !result.DryRun || result.Phase != hoststate.StartDryRunComplete {
			t.Fatalf("unconfirmed dry run = %#v, %v", result, err)
		}
		stored, err := fixture.journal.LoadStartByKey(t.Context(), request.IdempotencyKey)
		if err != nil || stored.Record.Confirmation != nil {
			t.Fatalf("dry run should record no confirmation: %#v, %v", stored.Record.Confirmation, err)
		}
	})
	t.Run("unsupported", func(t *testing.T) {
		fixture := newHostFixture(t, hoststate.PolicyConfirm, time.Hour, nil)
		if err := fixture.host.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
		request := fixture.startRequest("run-dry-unsupported", "key-dry-unsupported", "user:dry")
		request.DryRun = true
		if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:dry"), request); !errors.Is(err, appworkflow.ErrDryRunUnsupported) {
			t.Fatalf("dry run without support = %v, want ErrDryRunUnsupported", err)
		}
	})
	t.Run("denied", func(t *testing.T) {
		fixture := newHostFixture(t, hoststate.PolicyDeny, time.Hour, nil)
		fixture.dryRunSupported.Store(true)
		if err := fixture.host.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
		request := fixture.startRequest("run-dry-denied", "key-dry-denied", "user:dry")
		request.DryRun = true
		if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:dry"), request); !errors.Is(err, appworkflow.ErrPolicyDenied) {
			t.Fatalf("denied dry run = %v, want ErrPolicyDenied", err)
		}
	})
}

// allowListPolicy is the production shape: an effect-advised start asks for
// confirmation, and the unattended allow-list may waive it.
func allowListPolicy(store *unattended.Store, now func() time.Time) func(hoststate.PolicyFacts) hoststate.PolicyDecision {
	return func(facts hoststate.PolicyFacts) hoststate.PolicyDecision {
		confirm := hoststate.PolicyDecision{Outcome: hoststate.PolicyConfirm, Reason: "workflow effects require explicit confirmation"}
		return unattended.Apply(store, facts, confirm, now())
	}
}

func writeAllowList(t *testing.T, entries ...unattended.Entry) (*unattended.Store, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), unattended.FileName)
	if err := unattended.WriteAtomic(path, unattended.File{Version: unattended.FileVersion, Entries: entries}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return unattended.NewStore(path, os.Getuid(), nil, nil), info.ModTime()
}

// A scheduled or triggered activation has nobody to confirm. An allow-listed
// plan starts anyway and its audit names the entry; the same workflow from an
// activation the entry does not cover is still refused.
func TestUnattendedAllowListLetsActivationStartAdvisedWorkflow(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyConfirm, time.Hour, nil)
	identity := testIdentityBinding("service:activation", "activation")
	identity.Extension = map[string]string{"exposure_ref": "webhook-main"}
	host := hostWithFixedIdentity(t, fixture, identity)
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	activations, err := persistence.NewWorkflowActivationStore(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	now := fixture.now.Add(time.Minute)
	service := appworkflow.ActivationService{Host: host, Store: activations, Clock: appworkflow.ClockFunc(func() time.Time { return now })}
	listed := activationHostRegistration(t, fixture, identity, "nightly-listed")
	unlisted := activationHostRegistration(t, fixture, identity, "nightly-unlisted")
	for _, registration := range []hoststate.ActivationRegistration{listed, unlisted} {
		if _, _, registerErr := service.Register(t.Context(), registration); registerErr != nil {
			t.Fatalf("Register %s: %v", registration.ID, registerErr)
		}
	}

	addedAt := fixture.now.Add(-time.Hour)
	entry := unattended.Entry{
		ID: "ua-nightly", PlanID: fixture.plan.ID, Digest: fixture.plan.Graph.Digest,
		Scope:  unattended.Scope{ActivationID: listed.ID},
		Reason: "nightly report runs unattended", AddedBy: "local:operator", AddedAt: addedAt,
	}
	store, mtime := writeAllowList(t, entry)
	fixture.setPolicy(allowListPolicy(store, func() time.Time { return now }))

	caller := authenticatedContext(t.Context(), "service:activation")
	started, err := service.ActivateExternal(caller, appworkflow.ExternalActivationRequest{
		RegistrationID: listed.ID, IdempotencyKey: "listed-fire", OccurredAt: now, ReceivedAt: now,
		Payload: map[string]any{"message": "unattended"},
	})
	if err != nil || started.Start.Run == nil {
		t.Fatalf("allow-listed activation = %#v, %v", started, err)
	}
	decision := started.Start.Decision
	if decision.Outcome != hoststate.PolicyAllow {
		t.Fatalf("decision outcome = %q, want allow", decision.Outcome)
	}
	wantAttrs := map[string]string{
		unattended.AttrEntry:     "ua-nightly",
		unattended.AttrReason:    "nightly report runs unattended",
		unattended.AttrAddedBy:   "local:operator",
		unattended.AttrAddedAt:   addedAt.UTC().Format(time.RFC3339Nano),
		unattended.AttrFileMTime: mtime.UTC().Format(time.RFC3339Nano),
	}
	for key, want := range wantAttrs {
		if got := decision.Attributes[key]; got != want {
			t.Errorf("decision attribute %s = %q, want %q", key, got, want)
		}
	}
	persisted, err := fixture.journal.ListPolicyDecisions(t.Context(), started.Start.Run.ID)
	if err != nil || len(persisted) != 1 || persisted[0].Attributes[unattended.AttrEntry] != "ua-nightly" ||
		persisted[0].Attributes[unattended.AttrAddedAt] != wantAttrs[unattended.AttrAddedAt] {
		t.Fatalf("persisted decision = %#v, %v", persisted, err)
	}
	stored, err := fixture.journal.LoadStartByKey(t.Context(), started.Dispatch.HostStartKey)
	if err != nil || stored.Record.Confirmation == nil || stored.Record.Confirmation.AllowlistEntry != "ua-nightly" || stored.Record.Confirmation.Confirmed {
		t.Fatalf("start confirmation = %#v, %v", stored.Record.Confirmation, err)
	}

	refused, err := service.ActivateExternal(caller, appworkflow.ExternalActivationRequest{
		RegistrationID: unlisted.ID, IdempotencyKey: "unlisted-fire", OccurredAt: now, ReceivedAt: now,
		Payload: map[string]any{"message": "unattended"},
	})
	if !errors.Is(err, appworkflow.ErrConfirmationRequired) {
		t.Fatalf("unlisted activation = %#v, %v; want ErrConfirmationRequired", refused, err)
	}
}

// Entries pin the exact plan digest and expire; neither a different digest
// nor an expired entry waives confirmation, and a live principal-scoped entry
// does.
func TestUnattendedAllowListMatchesExactDigestScopeAndExpiry(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyConfirm, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	addedAt := fixture.now.Add(-2 * time.Hour)
	expired := fixture.now.Add(-time.Hour)
	// The compiled plan's digest is not what entries pin; an entry holding it
	// (an easy operator mistake) must not match.
	otherDigest := fixture.plan.Digest
	store, _ := writeAllowList(t,
		unattended.Entry{ID: "ua-a-other-digest", PlanID: fixture.plan.ID, Digest: otherDigest, Reason: "stale", AddedBy: "local:op", AddedAt: addedAt},
		unattended.Entry{ID: "ua-b-expired", PlanID: fixture.plan.ID, Digest: fixture.plan.Graph.Digest, ExpiresAt: &expired, Reason: "old", AddedBy: "local:op", AddedAt: addedAt},
		unattended.Entry{ID: "ua-c-carol", PlanID: fixture.plan.ID, Digest: fixture.plan.Graph.Digest, Scope: unattended.Scope{Principal: "user:carol"}, Reason: "carol's batch", AddedBy: "local:op", AddedAt: addedAt},
	)
	fixture.setPolicy(allowListPolicy(store, func() time.Time { return fixture.now }))

	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:dave"), fixture.startRequest("run-dave", "key-dave", "user:dave")); !errors.Is(err, appworkflow.ErrConfirmationRequired) {
		t.Fatalf("principal outside every live entry = %v, want ErrConfirmationRequired", err)
	}
	started, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:carol"), fixture.startRequest("run-carol", "key-carol", "user:carol"))
	if err != nil || started.Run == nil || started.Decision.Attributes[unattended.AttrEntry] != "ua-c-carol" {
		t.Fatalf("carol's unattended start = %#v, %v", started, err)
	}
	if started.Run.Status != workflowruntime.RunRunning {
		t.Fatalf("run status = %q", started.Run.Status)
	}
}
