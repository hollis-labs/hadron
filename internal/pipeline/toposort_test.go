package pipeline

import (
	"strings"
	"testing"
)

func stageNames(levels [][]Stage) [][]string {
	out := make([][]string, len(levels))
	for i, level := range levels {
		names := make([]string, len(level))
		for j, s := range level {
			names[j] = s.Name
		}
		out[i] = names
	}
	return out
}

func containsAll(have, want []string) bool {
	if len(have) != len(want) {
		return false
	}
	set := make(map[string]bool, len(have))
	for _, s := range have {
		set[s] = true
	}
	for _, s := range want {
		if !set[s] {
			return false
		}
	}
	return true
}

func TestTopoSort_LinearChain(t *testing.T) {
	stages := []Stage{
		{Name: "A", BlueprintPath: "./a.yaml"},
		{Name: "B", BlueprintPath: "./b.yaml", DependsOn: []string{"A"}},
		{Name: "C", BlueprintPath: "./c.yaml", DependsOn: []string{"B"}},
	}
	levels, err := TopoSort(stages)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	names := stageNames(levels)
	if len(names) != 3 {
		t.Fatalf("expected 3 levels, got %d: %v", len(names), names)
	}
	if !containsAll(names[0], []string{"A"}) {
		t.Fatalf("level 0: expected [A], got %v", names[0])
	}
	if !containsAll(names[1], []string{"B"}) {
		t.Fatalf("level 1: expected [B], got %v", names[1])
	}
	if !containsAll(names[2], []string{"C"}) {
		t.Fatalf("level 2: expected [C], got %v", names[2])
	}
}

func TestTopoSort_FanOut(t *testing.T) {
	stages := []Stage{
		{Name: "A", BlueprintPath: "./a.yaml"},
		{Name: "B", BlueprintPath: "./b.yaml", DependsOn: []string{"A"}},
		{Name: "C", BlueprintPath: "./c.yaml", DependsOn: []string{"A"}},
	}
	levels, err := TopoSort(stages)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	names := stageNames(levels)
	if len(names) != 2 {
		t.Fatalf("expected 2 levels, got %d: %v", len(names), names)
	}
	if !containsAll(names[0], []string{"A"}) {
		t.Fatalf("level 0: expected [A], got %v", names[0])
	}
	if !containsAll(names[1], []string{"B", "C"}) {
		t.Fatalf("level 1: expected [B,C], got %v", names[1])
	}
}

func TestTopoSort_FanIn(t *testing.T) {
	stages := []Stage{
		{Name: "A", BlueprintPath: "./a.yaml"},
		{Name: "B", BlueprintPath: "./b.yaml"},
		{Name: "C", BlueprintPath: "./c.yaml", DependsOn: []string{"A", "B"}},
	}
	levels, err := TopoSort(stages)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	names := stageNames(levels)
	if len(names) != 2 {
		t.Fatalf("expected 2 levels, got %d: %v", len(names), names)
	}
	if !containsAll(names[0], []string{"A", "B"}) {
		t.Fatalf("level 0: expected [A,B], got %v", names[0])
	}
	if !containsAll(names[1], []string{"C"}) {
		t.Fatalf("level 1: expected [C], got %v", names[1])
	}
}

func TestTopoSort_Diamond(t *testing.T) {
	stages := []Stage{
		{Name: "A", BlueprintPath: "./a.yaml"},
		{Name: "B", BlueprintPath: "./b.yaml", DependsOn: []string{"A"}},
		{Name: "C", BlueprintPath: "./c.yaml", DependsOn: []string{"A"}},
		{Name: "D", BlueprintPath: "./d.yaml", DependsOn: []string{"B", "C"}},
	}
	levels, err := TopoSort(stages)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	names := stageNames(levels)
	if len(names) != 3 {
		t.Fatalf("expected 3 levels, got %d: %v", len(names), names)
	}
	if !containsAll(names[0], []string{"A"}) {
		t.Fatalf("level 0: expected [A], got %v", names[0])
	}
	if !containsAll(names[1], []string{"B", "C"}) {
		t.Fatalf("level 1: expected [B,C], got %v", names[1])
	}
	if !containsAll(names[2], []string{"D"}) {
		t.Fatalf("level 2: expected [D], got %v", names[2])
	}
}

func TestTopoSort_ComplexDAG(t *testing.T) {
	// A has no deps
	// B depends on A
	// C has no deps
	// D depends on B, C
	// E depends on D
	// F depends on A
	stages := []Stage{
		{Name: "A", BlueprintPath: "./a.yaml"},
		{Name: "B", BlueprintPath: "./b.yaml", DependsOn: []string{"A"}},
		{Name: "C", BlueprintPath: "./c.yaml"},
		{Name: "D", BlueprintPath: "./d.yaml", DependsOn: []string{"B", "C"}},
		{Name: "E", BlueprintPath: "./e.yaml", DependsOn: []string{"D"}},
		{Name: "F", BlueprintPath: "./f.yaml", DependsOn: []string{"A"}},
	}
	levels, err := TopoSort(stages)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	names := stageNames(levels)
	if len(names) != 4 {
		t.Fatalf("expected 4 levels, got %d: %v", len(names), names)
	}
	// Level 0: A, C (no deps)
	if !containsAll(names[0], []string{"A", "C"}) {
		t.Fatalf("level 0: expected [A,C], got %v", names[0])
	}
	// Level 1: B, F (depend only on A)
	if !containsAll(names[1], []string{"B", "F"}) {
		t.Fatalf("level 1: expected [B,F], got %v", names[1])
	}
	// Level 2: D (depends on B, C)
	if !containsAll(names[2], []string{"D"}) {
		t.Fatalf("level 2: expected [D], got %v", names[2])
	}
	// Level 3: E (depends on D)
	if !containsAll(names[3], []string{"E"}) {
		t.Fatalf("level 3: expected [E], got %v", names[3])
	}
}

func TestTopoSort_V1Fallback(t *testing.T) {
	stages := []Stage{
		{Name: "A", BlueprintPath: "./a.yaml"},
		{Name: "B", BlueprintPath: "./b.yaml"},
		{Name: "C", BlueprintPath: "./c.yaml"},
	}
	levels, err := TopoSort(stages)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	names := stageNames(levels)
	if len(names) != 3 {
		t.Fatalf("expected 3 levels (v1 fallback), got %d: %v", len(names), names)
	}
	// Each stage is its own level, in array order.
	for i, expected := range []string{"A", "B", "C"} {
		if len(names[i]) != 1 || names[i][0] != expected {
			t.Fatalf("level %d: expected [%s], got %v", i, expected, names[i])
		}
	}
}

func TestTopoSort_CycleDetection(t *testing.T) {
	stages := []Stage{
		{Name: "A", BlueprintPath: "./a.yaml", DependsOn: []string{"C"}},
		{Name: "B", BlueprintPath: "./b.yaml", DependsOn: []string{"A"}},
		{Name: "C", BlueprintPath: "./c.yaml", DependsOn: []string{"B"}},
	}
	_, err := TopoSort(stages)
	if err == nil {
		t.Fatalf("expected cycle error")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected 'cycle' in error, got: %v", err)
	}
}

func TestTopoSort_Empty(t *testing.T) {
	levels, err := TopoSort(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if levels != nil {
		t.Fatalf("expected nil for empty input, got %v", levels)
	}
}
