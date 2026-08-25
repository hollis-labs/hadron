package hoststate_test

import (
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

func TestMCPTokenDigestIsBoundedAndComparedWithoutRawMaterial(t *testing.T) {
	first, err := hoststate.DigestMCPToken("one-time-bearer")
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := hoststate.DigestMCPToken("one-time-bearer")
	if err != nil {
		t.Fatal(err)
	}
	other, err := hoststate.DigestMCPToken("different-bearer")
	if err != nil {
		t.Fatal(err)
	}
	if first != replayed || !hoststate.MatchMCPTokenDigest(first, replayed) || hoststate.MatchMCPTokenDigest(first, other) {
		t.Fatalf("digest comparison first=%q replayed=%q other=%q", first, replayed, other)
	}
	if strings.Contains(first, "one-time-bearer") {
		t.Fatal("credential digest contains raw bearer material")
	}
	if _, err := hoststate.DigestMCPToken(""); err == nil {
		t.Fatal("empty token accepted")
	}
	if _, err := hoststate.DigestMCPToken(strings.Repeat("x", hoststate.MaximumMCPTokenBytes+1)); err == nil {
		t.Fatal("oversized token accepted")
	}
	for _, token := range []string{" leading", "trailing ", "line\nbreak", "tab\tvalue"} {
		if _, err := hoststate.DigestMCPToken(token); err == nil {
			t.Fatalf("non-canonical token %q accepted", token)
		}
	}
}

func TestAgentNamespaceDerivationIsCanonicalAndValidated(t *testing.T) {
	if namespace, err := hoststate.AgentNamespaceForPrincipal("agent:nanite/reviewer", ""); err != nil || namespace != "nanite/reviewer" {
		t.Fatalf("derived namespace = %q, %v", namespace, err)
	}
	for _, principal := range []string{"agent:", "agent:/root", "agent:../root", "agent:nanite//reviewer"} {
		if _, err := hoststate.AgentNamespaceForPrincipal(principal, ""); err == nil {
			t.Fatalf("invalid agent principal %q produced a namespace", principal)
		}
	}
	if _, err := hoststate.AgentNamespaceForPrincipal("agent:nanite/reviewer", "other/reviewer"); err == nil {
		t.Fatal("agent principal accepted a different owned namespace")
	}
}
