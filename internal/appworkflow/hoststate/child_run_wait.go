package hoststate

import (
	"context"
	"errors"

	"github.com/hollis-labs/go-workflow/runtime"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

// ChildRunWait pairs a terminal child run with an open child_run wait that
// its parent run opened on any node. It generalizes
// runtime.ChildTerminalWait, whose contract only admits a wait opened by the
// same node invocation that created the child link. call@v1 never suspends
// (it completes with the child handle), so the documented pattern and the
// agent_launch composition collect the child from a separate wait_for node,
// which the core contract cannot represent.
type ChildRunWait struct {
	Link  runtime.ChildRunLink `json:"link"`
	Child runtime.RunSnapshot  `json:"child"`
	Wait  runtime.WaitSnapshot `json:"wait"`
}

// Validate binds the wait to the child through its durable link: the wait
// belongs to the link's parent run and correlates exactly the child run ID.
func (c ChildRunWait) Validate() error {
	if err := c.Link.Validate(); err != nil {
		return err
	}
	if err := c.Child.Validate(); err != nil {
		return err
	}
	if err := c.Wait.Validate(); err != nil {
		return err
	}
	if !c.Child.Status.Terminal() || c.Child.ID != c.Link.ChildRunID || c.Wait.Invocation.RunID != c.Link.ParentRunID ||
		c.Wait.Kind != workflowwait.KindChildRun || c.Wait.WakeSource != workflowwait.WakeChildRun ||
		c.Wait.Correlation != string(c.Link.ChildRunID) || c.Wait.Status != runtime.WaitOpen {
		return errors.New("child run wait does not match its immutable link and an open parent-run wait")
	}
	return nil
}

// ChildRunWaitStore exposes terminal-child/open-parent-wait pairs across all
// nodes of the parent run. Limit zero means the store's bounded default page.
type ChildRunWaitStore interface {
	RecoverChildRunWaits(context.Context, int) ([]ChildRunWait, error)
}
