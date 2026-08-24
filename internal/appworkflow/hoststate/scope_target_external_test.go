package hoststate_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/workflow/graph"
)

func TestRunScopeIsLogicalOnlyAndDefensivelyCloned(t *testing.T) {
	scope := hoststate.RunScope{
		Version: hoststate.ScopeTargetVersionV1,
		Kind:    hoststate.RunScopeProject,
		ID:      "project:alpha",
		Attributes: map[string]string{
			"cost_center": "research",
		},
	}
	if err := scope.Validate(); err != nil {
		t.Fatal(err)
	}
	clone := scope.Clone()
	scope.Attributes["cost_center"] = "mutated"
	if clone.Attributes["cost_center"] != "research" {
		t.Fatalf("Clone retained caller map: %#v", clone)
	}
	encoded, err := json.Marshal(clone)
	if err != nil {
		t.Fatal(err)
	}
	var decoded hoststate.RunScope
	if err := json.Unmarshal(encoded, &decoded); err != nil || !reflect.DeepEqual(decoded, clone) {
		t.Fatalf("JSON round trip = %#v, %v", decoded, err)
	}

	for name, invalid := range map[string]hoststate.RunScope{
		"compute fact":    {Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "alpha", Attributes: map[string]string{"sandbox": "isolated"}},
		"workspace alias": {Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "alpha", Attributes: map[string]string{"workspace.id": "old"}},
		"secret id":       {Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "secret://scope"},
		"query id":        {Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "https://scope.invalid/path?token=benign"},
		"unknown kind":    {Version: hoststate.ScopeTargetVersionV1, Kind: "workspace", ID: "alpha"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := invalid.Validate(); err == nil {
				t.Fatal("Validate unexpectedly succeeded")
			}
		})
	}
}

func TestExecutionTargetValidationCloningAndSelector(t *testing.T) {
	target := validRemoteTarget()
	if err := target.Validate(); err != nil {
		t.Fatal(err)
	}
	clone := target.Clone()
	target.Labels["region"] = "mutated"
	target.EnvironmentRefs["locale"] = hoststate.TargetConfigReference{Authority: "mutated", Name: "mutated"}
	target.Provenance.Attributes["pool"] = "mutated"
	target.Capabilities[0] = "mutated"
	target.Lease.ID = "mutated"
	if clone.Labels["region"] != "us-central" || clone.EnvironmentRefs["locale"].Authority != "host-config" || clone.Provenance.Attributes["pool"] != "default" || clone.Capabilities[0] != "compute" || clone.Lease.ID != "lease-one" {
		t.Fatalf("Clone retained caller aliases: %#v", clone)
	}
	encoded, err := json.Marshal(clone)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "workspace_id") {
		t.Fatalf("graph-native target JSON contains legacy workspace_id: %s", encoded)
	}
	var decoded hoststate.ExecutionTarget
	if err := json.Unmarshal(encoded, &decoded); err != nil || !reflect.DeepEqual(decoded, clone) {
		t.Fatalf("JSON round trip = %#v, %v", decoded, err)
	}
	selector := hoststate.ExecutionTargetSelector{
		Version:              hoststate.ScopeTargetVersionV1,
		Kinds:                []hoststate.ExecutionTargetKind{hoststate.ExecutionTargetRemote},
		RequiredCapabilities: []string{"compute", "network"},
		RequiredLabels:       map[string]string{"region": "us-central"},
		SandboxModes:         []hoststate.SandboxMode{hoststate.SandboxIsolated},
	}
	if err := selector.Validate(); err != nil || !selector.Matches(clone) {
		t.Fatalf("selector match = %v, %v", selector.Matches(clone), err)
	}
	selectorClone := selector.Clone()
	selector.RequiredLabels["region"] = "elsewhere"
	if selector.Matches(clone) {
		t.Fatal("mismatched selector unexpectedly matched")
	}
	if selectorClone.RequiredLabels["region"] != "us-central" {
		t.Fatalf("selector clone retained caller map: %#v", selectorClone)
	}
}

func TestExecutionTargetRejectsMalformedAndSecretFacts(t *testing.T) {
	for name, mutate := range map[string]func(*hoststate.ExecutionTarget){
		"non utc readiness": func(target *hoststate.ExecutionTarget) {
			target.Readiness.CheckedAt = target.Readiness.CheckedAt.In(time.FixedZone("offset", 3600))
		},
		"non utc lease": func(target *hoststate.ExecutionTarget) {
			target.Lease.ExpiresAt = target.Lease.ExpiresAt.In(time.FixedZone("offset", 3600))
		},
		"missing pending reason": func(target *hoststate.ExecutionTarget) {
			target.Readiness.State, target.Readiness.Reason = hoststate.TargetPending, ""
		},
		"credential label": func(target *hoststate.ExecutionTarget) { target.Labels["api-key"] = "masked" },
		"query provenance": func(target *hoststate.ExecutionTarget) {
			target.Provenance.Reference = "https://runner.invalid/select?pool=default"
		},
		"secret ref": func(target *hoststate.ExecutionTarget) {
			target.EnvironmentRefs["locale"] = hoststate.TargetConfigReference{Authority: "secret://vault", Name: "locale"}
		},
		"unsorted capabilities":  func(target *hoststate.ExecutionTarget) { target.Capabilities = []string{"network", "compute"} },
		"duplicate capabilities": func(target *hoststate.ExecutionTarget) { target.Capabilities = []string{"compute", "compute"} },
		"uri capability": func(target *hoststate.ExecutionTarget) {
			target.Capabilities = []string{"https://capability.invalid/compute"}
		},
		"workspace alias": func(target *hoststate.ExecutionTarget) { target.Provenance.Attributes["workspace-id"] = "legacy" },
	} {
		t.Run(name, func(t *testing.T) {
			target := validRemoteTarget()
			mutate(&target)
			if err := target.Validate(); err == nil {
				t.Fatal("Validate unexpectedly succeeded")
			}
		})
	}
}

func TestValidateExecutionTargetBindingFailsClosedOnMalformedPlan(t *testing.T) {
	target := validRemoteTarget()
	valid := map[string]graph.ExecutionTargetRequirements{
		"node": {
			Kinds:        []string{"remote_runner"},
			Capabilities: []string{"compute"},
			Labels:       map[string]string{"region": "us-central"},
			Constraints:  graph.Config{"sandbox": "isolated", "sandbox_profile": "locked"},
		},
	}
	if err := hoststate.ValidateExecutionTargetBinding(&target, []string{"compute"}, valid); err != nil {
		t.Fatal(err)
	}
	if err := hoststate.ValidateExecutionTargetBinding(nil, nil, nil); err != nil {
		t.Fatalf("optional target = %v", err)
	}
	if err := hoststate.ValidateExecutionTargetBinding(nil, []string{"compute"}, nil); err == nil {
		t.Fatal("missing required target unexpectedly accepted")
	}

	tooManyLabels := make(map[string]string, hoststate.MaximumScopeTargetAttributes+1)
	for index := 0; index <= hoststate.MaximumScopeTargetAttributes; index++ {
		tooManyLabels[fmt.Sprintf("label-%03d", index)] = "safe"
	}
	cases := map[string]graph.ExecutionTargetRequirements{
		"unsorted kinds":        {Kinds: []string{"remote_runner", "local"}},
		"duplicate kinds":       {Kinds: []string{"local", "local"}},
		"unknown kind":          {Kinds: []string{"container"}},
		"unsorted capabilities": {Capabilities: []string{"network", "compute"}},
		"credential label":      {Labels: map[string]string{"auth-token": "masked"}},
		"oversized labels":      {Labels: tooManyLabels},
		"unknown constraint":    {Constraints: graph.Config{"region": "us-central"}},
		"non-string constraint": {Constraints: graph.Config{"sandbox": true}},
	}
	for name, requirement := range cases {
		t.Run(name, func(t *testing.T) {
			if err := hoststate.ValidateExecutionTargetBinding(&target, nil, map[string]graph.ExecutionTargetRequirements{"node": requirement}); err == nil {
				t.Fatal("malformed plan requirement unexpectedly accepted")
			}
		})
	}
	nondeterministicInput := map[string]graph.ExecutionTargetRequirements{
		"z-node": {Kinds: []string{"z-invalid"}},
		"a-node": {Kinds: []string{"a-invalid"}},
	}
	var first string
	for iteration := 0; iteration < 50; iteration++ {
		err := hoststate.ValidateExecutionTargetBinding(&target, nil, nondeterministicInput)
		if err == nil {
			t.Fatal("multiple malformed requirements unexpectedly accepted")
		}
		if iteration == 0 {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("requirement validation is nondeterministic: %q != %q", err, first)
		}
	}
}

func TestIdentityBindingPublicMetadataIsConservative(t *testing.T) {
	binding := hoststate.IdentityBinding{
		Principal:       "user:one",
		SourceAuthority: "local",
		Trust:           "trusted",
		Grants:          []string{"workflow.run"},
		RunScope:        hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeUser, ID: "user:one"},
		Extension:       map[string]string{"trace": "trace-one"},
	}
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, grants := range map[string][]string{
		"uri":     {"https://grant.invalid/run"},
		"control": {"workflow.\nrun"},
		"secret":  {"api-token"},
	} {
		t.Run("grant "+name, func(t *testing.T) {
			candidate := binding.Clone()
			candidate.Grants = grants
			err := candidate.Validate()
			if err == nil || strings.Contains(err.Error(), grants[0]) {
				t.Fatalf("grant validation error = %v", err)
			}
		})
	}
	tooManyGrants := binding.Clone()
	tooManyGrants.Grants = make([]string, hoststate.MaximumIdentityGrants+1)
	for index := range tooManyGrants.Grants {
		tooManyGrants.Grants[index] = fmt.Sprintf("grant.%03d", index)
	}
	if err := tooManyGrants.Validate(); err == nil {
		t.Fatal("oversized grants unexpectedly accepted")
	}
	for _, extension := range []map[string]string{
		{"workspace-id": "legacy"},
		{"hint": "Bearer credential"},
		{"hint": "https://example.invalid/path?benign=true"},
		{"hint": "https://user@example.invalid/path"},
		{"hint": "https://example.invalid/%zz"},
	} {
		candidate := binding.Clone()
		candidate.Extension = extension
		err := candidate.Validate()
		if err == nil {
			t.Fatalf("unsafe extension accepted: %#v", extension)
		}
		for _, value := range extension {
			if strings.Contains(err.Error(), value) {
				t.Fatalf("validation error leaked rejected value: %v", err)
			}
		}
	}
}

func validRemoteTarget() hoststate.ExecutionTarget {
	checkedAt := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	return hoststate.ExecutionTarget{
		Version: hoststate.ScopeTargetVersionV1,
		ID:      "runner-one",
		Kind:    hoststate.ExecutionTargetRemote,
		CWD:     "/workspace/project",
		EnvironmentRefs: map[string]hoststate.TargetConfigReference{
			"locale": {Authority: "host-config", Name: "locale", Version: "v1"},
		},
		ConfigRefs:   map[string]hoststate.TargetConfigReference{"tooling": {Authority: "host-config", Name: "tooling"}},
		Capabilities: []string{"compute", "network"},
		Labels:       map[string]string{"region": "us-central"},
		Sandbox:      hoststate.SandboxPolicy{Mode: hoststate.SandboxIsolated, Profile: "locked"},
		Readiness:    hoststate.TargetReadiness{State: hoststate.TargetReady, CheckedAt: checkedAt},
		Lease:        &hoststate.ExecutionTargetLease{ID: "lease-one", AcquiredAt: checkedAt.Add(-time.Minute), ExpiresAt: checkedAt.Add(time.Hour)},
		Provenance:   hoststate.TargetProvenance{Authority: "scheduler", Reference: "runner-pool/default", Revision: "v1", Attributes: map[string]string{"pool": "default"}},
	}
}
