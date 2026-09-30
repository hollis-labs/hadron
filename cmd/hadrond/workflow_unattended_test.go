package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/unattended"
)

// The production daemon reads the unattended allow-list from its data dir:
// an effect-advised workflow (here: one with a call node) needs
// confirmation until an entry pins its plan id and the graph digest
// `workflow validate` reports, and then starts unconfirmed with the entry
// named in its decision.
func TestProductionUnattendedAllowListWaivesConfirmation(t *testing.T) {
	runtime, cfg, _ := newTestProductionWorkflowRuntime(t)
	if err := os.WriteFile(filepath.Join(workflowSourceRoot(cfg), "call-parent.workflow.yaml"), []byte(productionCallParentSource), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	ctx := productionLocalContext(t)
	definition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "call-parent", Locator: "call-parent.workflow.yaml", Version: "v1"}
	start := func(key string) (appworkflow.StartRunResult, error) {
		return runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
			RunID: appworkflow.RunID(key), IdempotencyKey: key, Definition: definition,
			Inputs: map[string]any{"message": "unattended"}, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"},
		})
	}
	if _, err := start("unlisted"); !errors.Is(err, appworkflow.ErrConfirmationRequired) {
		t.Fatalf("unconfirmed start before any entry = %v, want ErrConfirmationRequired", err)
	}

	validated, err := runtime.operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: definition})
	if err != nil || validated.Plan == nil || validated.GraphDigest == "" {
		t.Fatalf("validate = %#v, %v", validated, err)
	}
	entry := unattended.Entry{
		ID: "ua-call-parent", PlanID: validated.Plan.ID, Digest: validated.GraphDigest,
		Reason: "scheduled report", AddedBy: "local:operator", AddedAt: time.Now().Add(-time.Minute).UTC(),
	}
	if writeErr := unattended.WriteAtomic(filepath.Join(cfg.DataDir, unattended.FileName), unattended.File{Version: unattended.FileVersion, Entries: []unattended.Entry{entry}}); writeErr != nil {
		t.Fatal(writeErr)
	}
	started, err := start("listed")
	if err != nil || started.Run == nil {
		t.Fatalf("allow-listed unconfirmed start = %#v, %v", started, err)
	}
	if started.Decision.Outcome != hoststate.PolicyAllow || started.Decision.Attributes[unattended.AttrEntry] != "ua-call-parent" {
		t.Fatalf("decision = %#v", started.Decision)
	}
}

func TestAllowListedWorkflowPolicyOnlyWaivesConfirm(t *testing.T) {
	path := filepath.Join(t.TempDir(), unattended.FileName)
	identity := localWorkflowIdentity()
	facts := hoststate.PolicyFacts{
		Operation: "start", RunID: "policy-run",
		Identity: identity, RunScope: identity.RunScope, ExecutionTarget: identity.ExecutionTarget,
		Effects: graph.EffectSet{graph.EffectMutate}, NodeCount: 1, BlastRadius: map[string]int{"nodes": 1},
		ConfirmationAdvised: true,
	}
	facts.Plan.ID, facts.Plan.Version, facts.Plan.SchemaVersion = "policy", "v1", "1"
	facts.Plan.Digest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	facts.GraphDigest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	if err := unattended.WriteAtomic(path, unattended.File{Version: unattended.FileVersion, Entries: []unattended.Entry{{
		ID: "ua-policy", PlanID: "policy", Digest: facts.GraphDigest, Reason: "r", AddedBy: "local:op", AddedAt: time.Now().Add(-time.Hour),
	}}}); err != nil {
		t.Fatal(err)
	}
	policy := allowListedWorkflowPolicy(unattended.NewStore(path, os.Geteuid(), nil, nil))
	decision, err := policy.EvaluatePolicy(t.Context(), facts)
	if err != nil || decision.Outcome != hoststate.PolicyAllow || decision.Attributes[unattended.AttrEntry] != "ua-policy" {
		t.Fatalf("listed advised facts = %#v, %v", decision, err)
	}
	facts.Identity.Principal = ""
	if decision, err = policy.EvaluatePolicy(t.Context(), facts); err != nil || decision.Outcome != hoststate.PolicyDeny {
		t.Fatalf("invalid facts must stay denied even when listed: %#v, %v", decision, err)
	}
}
