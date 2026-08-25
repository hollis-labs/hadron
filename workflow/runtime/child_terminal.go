package runtime

import (
	"context"
	"errors"

	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

// ChildTerminalWait is a bounded recovery candidate for a terminal child and
// its still-open parent wait. The host turns it into the adapter's typed
// terminal envelope and uses WaitCoordinator.Resume; storage owns no adapter
// payload or alternate completion state machine.
type ChildTerminalWait struct {
	Link  ChildRunLink `json:"link"`
	Child RunSnapshot  `json:"child"`
	Wait  WaitSnapshot `json:"wait"`
}

func (c ChildTerminalWait) Validate() error {
	if err := c.Link.Validate(); err != nil {
		return err
	}
	if err := c.Child.Validate(); err != nil {
		return err
	}
	if err := c.Wait.Validate(); err != nil {
		return err
	}
	if !c.Child.Status.Terminal() || c.Child.ID != c.Link.ChildRunID || c.Wait.Invocation != c.Link.Invocation ||
		c.Wait.Kind != workflowwait.KindChildRun || c.Wait.WakeSource != workflowwait.WakeChildRun || c.Wait.Correlation != string(c.Link.ChildRunID) || c.Wait.Status != WaitOpen {
		return errors.New("child terminal wait does not match its immutable link and open parent wait")
	}
	return nil
}

// ChildTerminalWaitStore exposes only eligible terminal-child/open-wait
// pairs. Limit zero means the store's bounded default/all-current page.
type ChildTerminalWaitStore interface {
	RecoverChildTerminalWaits(context.Context, int) ([]ChildTerminalWait, error)
}
