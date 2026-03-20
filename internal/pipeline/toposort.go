package pipeline

import "fmt"

// TopoSort returns stages grouped into execution levels using Kahn's algorithm.
// Each level contains stages whose dependencies are all satisfied by prior levels.
// Level 0 contains root stages (no depends_on). Stages within a level can run in parallel.
//
// v1 fallback: when no stage has depends_on, each stage becomes its own level
// in array order (preserves v1 sequential behavior).
//
// Returns an error if the graph contains a cycle.
func TopoSort(stages []Stage) ([][]Stage, error) {
	if len(stages) == 0 {
		return nil, nil
	}

	// Check whether any stage uses depends_on.
	hasDeps := false
	for i := range stages {
		if len(stages[i].DependsOn) > 0 {
			hasDeps = true
			break
		}
	}

	// v1 fallback: no depends_on anywhere → sequential, one stage per level.
	if !hasDeps {
		levels := make([][]Stage, len(stages))
		for i := range stages {
			levels[i] = []Stage{stages[i]}
		}
		return levels, nil
	}

	// Build name→index map.
	nameIndex := make(map[string]int, len(stages))
	for i, st := range stages {
		nameIndex[st.Name] = i
	}

	// Build in-degree counts and adjacency list (dependency → dependents).
	inDegree := make([]int, len(stages))
	dependents := make([][]int, len(stages)) // dependents[i] = indices that depend on stages[i]
	for i, st := range stages {
		inDegree[i] = len(st.DependsOn)
		for _, dep := range st.DependsOn {
			di, ok := nameIndex[dep]
			if !ok {
				return nil, fmt.Errorf("stage %q depends on unknown stage %q", st.Name, dep)
			}
			dependents[di] = append(dependents[di], i)
		}
	}

	// Kahn's algorithm — collect levels.
	var levels [][]Stage
	// Seed: all zero-in-degree nodes.
	var queue []int
	for i, d := range inDegree {
		if d == 0 {
			queue = append(queue, i)
		}
	}

	processed := 0
	for len(queue) > 0 {
		level := make([]Stage, 0, len(queue))
		var nextQueue []int
		for _, idx := range queue {
			level = append(level, stages[idx])
			processed++
			for _, dep := range dependents[idx] {
				inDegree[dep]--
				if inDegree[dep] == 0 {
					nextQueue = append(nextQueue, dep)
				}
			}
		}
		levels = append(levels, level)
		queue = nextQueue
	}

	if processed != len(stages) {
		return nil, fmt.Errorf("cycle detected in pipeline stages")
	}

	return levels, nil
}
