//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package cmd

import (
	"fmt"
	"os/exec"
)

func configureProcessCancellation(*exec.Cmd) error {
	return fmt.Errorf("%w: direct process-group cancellation is unsupported on this platform", ErrProcessFailed)
}
