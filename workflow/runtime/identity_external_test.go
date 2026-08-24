package runtime_test

import (
	"testing"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

func TestEncodeAttemptIdentityIsCollisionFreeForOpaqueDelimiters(t *testing.T) {
	pairs := [][2]workflowruntime.AttemptID{
		{
			{Invocation: workflowruntime.NodeInvocationID{RunID: "a:b", NodeID: "c", Iteration: "d"}, Number: 2},
			{Invocation: workflowruntime.NodeInvocationID{RunID: "a", NodeID: "b", Iteration: "c:d"}, Number: 2},
		},
		{
			{Invocation: workflowruntime.NodeInvocationID{RunID: "a/b", NodeID: "c", Iteration: "d"}, Number: 23},
			{Invocation: workflowruntime.NodeInvocationID{RunID: "a", NodeID: "b", Iteration: "c/d"}, Number: 23},
		},
		{
			{Invocation: workflowruntime.NodeInvocationID{RunID: "run", NodeID: "node", Iteration: "1/2"}, Number: 3},
			{Invocation: workflowruntime.NodeInvocationID{RunID: "run", NodeID: "node", Iteration: "1"}, Number: 23},
		},
	}
	for _, pair := range pairs {
		left, leftErr := workflowruntime.EncodeAttemptIdentity(pair[0])
		right, rightErr := workflowruntime.EncodeAttemptIdentity(pair[1])
		if leftErr != nil || rightErr != nil || left == right {
			t.Fatalf("EncodeAttemptIdentity(%#v/%#v) = %q/%q, errors %v/%v", pair[0], pair[1], left, right, leftErr, rightErr)
		}
	}
}
