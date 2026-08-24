package trigger

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

func TestCloneActivationPayloadIsBoundedExactAndDefensive(t *testing.T) {
	input := map[string]any{"number": json.Number("9007199254740993"), "nested": map[string]any{"value": "original"}}
	cloned, err := cloneActivationPayload(input)
	if err != nil {
		t.Fatal(err)
	}
	input["nested"].(map[string]any)["value"] = "mutated"
	if cloned["number"] != json.Number("9007199254740993") || cloned["nested"].(map[string]any)["value"] != "original" {
		t.Fatalf("clone = %#v", cloned)
	}
	if _, err := cloneActivationPayload(map[string]any{"invalid": make(chan struct{})}); err == nil {
		t.Fatal("non-JSON payload passed")
	}
	if _, err := cloneActivationPayload(map[string]any{"large": strings.Repeat("x", hoststate.MaximumActivationPayloadBytes)}); err == nil {
		t.Fatal("oversized payload passed")
	}
}
