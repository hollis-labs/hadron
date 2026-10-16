package agentsubstrate

import (
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/localauth"
)

// An agent Hadron launches never inherits the operator's credential, even
// when the daemon was started from a shell that exported it.
func TestMergeEnvScrubsOperatorToken(t *testing.T) {
	t.Setenv(localauth.TokenEnvVar, "operator-secret")
	for _, entry := range mergeEnv(map[string]string{"AGENT": "x"}, []string{"PATH=/bin"}) {
		if strings.HasPrefix(entry, localauth.TokenEnvVar+"=") {
			t.Fatalf("agent environment carries %s", localauth.TokenEnvVar)
		}
	}
}
