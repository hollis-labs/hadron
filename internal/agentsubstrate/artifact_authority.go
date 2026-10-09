package agentsubstrate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/launchartifacts"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/substrate/harness/adapters/runtimebind"
	"github.com/hollis-labs/substrate/llm-core/contracts/runtimes"
)

// acceptArtifactOperation narrows this launcher's accepted configured launch to
// one inactive artifact operation. The operation is process-local, with no
// durable admission or restart claim, and expires before session startup.
func (l *Launcher) acceptArtifactOperation(ctx context.Context, id string, cfg settings.AgentSubstrateSettings, req execution.AgentLaunchRequest) (launchartifacts.Admission, func(), error) {
	version, err := launchartifacts.Digest(struct {
		Config  settings.AgentSubstrateSettings
		Request execution.AgentLaunchRequest
	}{cfg, req})
	if err != nil {
		return launchartifacts.Admission{}, nil, err
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return launchartifacts.Admission{}, nil, errors.New("launcher is closing")
	}
	if l.artifactOperations == nil {
		l.artifactOperations = make(map[string]string)
	}
	if _, exists := l.artifactOperations[id]; exists {
		l.mu.Unlock()
		return launchartifacts.Admission{}, nil, errors.New("artifact operation already accepted")
	}
	l.artifactOperations[id] = version
	l.mu.Unlock()
	release := func() { l.mu.Lock(); delete(l.artifactOperations, id); l.mu.Unlock() }
	validate := func(current context.Context) error {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		if contextErr := current.Err(); contextErr != nil {
			return contextErr
		}
		l.mu.Lock()
		held := !l.closed && l.artifactOperations[id] == version
		l.mu.Unlock()
		if !held {
			return errors.New("accepted artifact operation custody changed")
		}
		if _, started := l.sessions.Get(id); started {
			return errors.New("artifact session already started")
		}
		currentVersion, digestErr := launchartifacts.Digest(struct {
			Config  settings.AgentSubstrateSettings
			Request execution.AgentLaunchRequest
		}{cfg, req})
		if digestErr != nil || currentVersion != version {
			return errors.New("accepted artifact request changed")
		}
		return nil
	}
	parent, err := filepath.Abs(l.dataDir)
	if err != nil {
		release()
		return launchartifacts.Admission{}, nil, err
	}
	return launchartifacts.Admission{OperationID: id, DecisionID: "hadron.accepted-launch:" + id, Version: version, Owner: fmt.Sprintf("hadron-daemon-uid:%d", os.Geteuid()), ControlParent: parent, LocalFilesystem: true, Validate: validate}, release, nil
}

// Retain existing settings spellings at the application boundary; the harness
// uses canonical modes and a separate debug posture.
func runtimeMode(value string) runtimes.Mode {
	switch value {
	case "subprocess":
		return runtimes.ModeSubprocessPerTurn
	case "serve-http":
		return runtimes.ModeHTTPSSE
	case "pty-debug":
		return runtimes.ModePTY
	default:
		return runtimes.Mode(value)
	}
}
func runtimePosture(value string) runtimebind.Posture {
	if value == "pty-debug" {
		return runtimebind.PostureDebug
	}
	return runtimebind.Posture("launch")
}

func runtimeSetting(mode runtimes.Mode) string {
	switch mode {
	case runtimes.ModeSubprocessPerTurn:
		return "subprocess"
	case runtimes.ModeHTTPSSE:
		return "serve-http"
	default:
		return string(mode)
	}
}
