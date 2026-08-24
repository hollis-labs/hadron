//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package cmd

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func configureProcessCancellation(command *exec.Cmd) error {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	return nil
}
