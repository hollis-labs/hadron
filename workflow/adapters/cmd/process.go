package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
)

// OSProcessRunner is the standard-library direct process runner. It supplies
// no sandbox isolation and therefore accepts only direct or none profiles
// already authorized by Policy. It never invokes a shell or inherits env/cwd.
type OSProcessRunner struct{}

// Run launches one direct child and waits for its streams and process to stop.
func (OSProcessRunner) Run(ctx context.Context, request ProcessRequest, stdout, stderr io.Writer) (ProcessResult, error) {
	if ctx == nil || stdout == nil || stderr == nil {
		return ProcessResult{}, ErrProcessFailed
	}
	if err := ctx.Err(); err != nil {
		return ProcessResult{}, err
	}
	if request.Sandbox.Profile != SandboxDirect && request.Sandbox.Profile != SandboxNone {
		return ProcessResult{}, fmt.Errorf("%w: direct runner cannot enforce requested sandbox", ErrProcessFailed)
	}
	if err := validateProcessRequest(request); err != nil {
		return ProcessResult{}, fmt.Errorf("%w: invalid launch request", ErrProcessFailed)
	}

	// Policy supplied an absolute, structured executable and argv. No shell is
	// involved and validateProcessRequest rechecks the launch contract.
	command := exec.CommandContext(ctx, request.Executable, request.Arguments...) //nolint:gosec // Structured policy-authorized direct execution.
	command.Dir = request.CWD
	command.Env = make([]string, len(request.Environment))
	for index, variable := range request.Environment {
		command.Env[index] = variable.Name + "=" + string(variable.Value)
	}
	command.Stdout = stdout
	command.Stderr = stderr
	command.Stdin = strings.NewReader("")
	command.WaitDelay = 5 * time.Second
	if err := configureProcessCancellation(command); err != nil {
		return ProcessResult{}, err
	}

	err := command.Run()
	if contextErr := ctx.Err(); contextErr != nil {
		return ProcessResult{}, contextErr
	}
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return ProcessResult{ExitCode: exitError.ExitCode(), EnforcedSandbox: request.Sandbox}, nil
		}
		return ProcessResult{}, err
	}
	return ProcessResult{ExitCode: 0, EnforcedSandbox: request.Sandbox}, nil
}

func validateProcessRequest(request ProcessRequest) error {
	resolved := ResolvedCommand{
		Executable: request.Executable, Arguments: request.Arguments, CWD: request.CWD,
		EffectiveEffects:      graph.EffectSet{graph.EffectDestructive},
		EffectiveCapabilities: []string{CapabilityProcessExecute}, Sandbox: request.Sandbox,
	}
	if err := validateResolved(resolved); err != nil {
		return err
	}
	names := make([]string, len(request.Environment))
	for index, variable := range request.Environment {
		if !validEnvironmentName(variable.Name) || len(variable.Value) == 0 {
			return errors.New("invalid process environment")
		}
		names[index] = variable.Name
	}
	if !sort.StringsAreSorted(names) {
		return errors.New("process environment must be sorted")
	}
	for index := 1; index < len(names); index++ {
		if names[index] == names[index-1] {
			return errors.New("process environment contains duplicate names")
		}
	}
	return nil
}
