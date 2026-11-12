// Package agentsettings is the configuration of Hadron's agent substrates: the
// places an agent_session step can run. Only the Tether-backed substrate
// (kind "tether_session", sessions launched on a Tether daemon) exists; the
// blueprint-era in-process runtimes were not carried forward.
//
// It is the hadron.agent_substrates block of the Substrate config:
//
//	hadron:
//	  agent_substrates:
//	    work:
//	      kind: tether_session
//	      tether:
//	        endpoint: ~/.tether/run/tetherd.sock   # default
//	        launch: claude-work                    # or launches: {logical_agent: launch_id}
//	        result_optional: false
//	        stop_on_result: true                   # default
//	        unreachable_timeout: 10m               # default
package agentsettings

import (
	"fmt"
	"strings"
	"time"
)

// KindTetherSession selects the Tether-backed agent session host.
const KindTetherSession = "tether_session"

// Defaults applied to an unset field.
const (
	DefaultEndpoint           = "~/.tether/run/tetherd.sock"
	DefaultUnreachableTimeout = "10m"
)

// Substrate is one named agent substrate.
type Substrate struct {
	Kind   string  `json:"kind"`
	Tether *Tether `json:"tether,omitempty"`
}

// Tether configures one tether_session substrate.
type Tether struct {
	// Endpoint is the muxd socket path. Empty means DefaultEndpoint.
	Endpoint string `json:"endpoint,omitempty"`
	// Launch is the Tether launch_id used when Launches has no entry for the
	// step's logical_agent_id.
	Launch string `json:"launch,omitempty"`
	// Launches maps a logical_agent_id to a Tether launch_id.
	Launches map[string]string `json:"launches,omitempty"`
	// ResultOptional lets a session that completes without a result reply
	// succeed with a JSON null result instead of failing agent_no_result.
	ResultOptional bool `json:"result_optional,omitempty"`
	// StopOnResult stops the session once its result reply is accepted.
	// Nil means true.
	StopOnResult *bool `json:"stop_on_result,omitempty"`
	// UnreachableTimeout is how long muxd may stay unreachable while a step
	// observes its session before the step fails agent_host_unreachable.
	UnreachableTimeout string `json:"unreachable_timeout,omitempty"`
}

// EffectiveEndpoint returns Endpoint or its default.
func (t Tether) EffectiveEndpoint() string {
	if strings.TrimSpace(t.Endpoint) == "" {
		return DefaultEndpoint
	}
	return t.Endpoint
}

// EffectiveStopOnResult returns StopOnResult or its default (true).
func (t Tether) EffectiveStopOnResult() bool { return t.StopOnResult == nil || *t.StopOnResult }

// EffectiveUnreachableTimeout parses UnreachableTimeout or its default.
func (t Tether) EffectiveUnreachableTimeout() (time.Duration, error) {
	raw := strings.TrimSpace(t.UnreachableTimeout)
	if raw == "" {
		raw = DefaultUnreachableTimeout
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("must be positive")
	}
	return parsed, nil
}

// Validate checks every substrate.
func Validate(substrates map[string]Substrate) error {
	for name, s := range substrates {
		if strings.TrimSpace(s.Kind) != KindTetherSession {
			return fmt.Errorf("hadron.agent_substrates.%s.kind: unsupported kind %q (only %q)", name, s.Kind, KindTetherSession)
		}
		if err := validateTether(name, s.Tether); err != nil {
			return err
		}
	}
	return nil
}

func validateTether(name string, cfg *Tether) error {
	if cfg == nil {
		return fmt.Errorf("hadron.agent_substrates.%s.tether: required for kind %q", name, KindTetherSession)
	}
	if strings.TrimSpace(cfg.Launch) == "" && len(cfg.Launches) == 0 {
		return fmt.Errorf("hadron.agent_substrates.%s.tether: launch or launches is required", name)
	}
	if cfg.Launch != strings.TrimSpace(cfg.Launch) {
		return fmt.Errorf("hadron.agent_substrates.%s.tether.launch: must not have surrounding whitespace", name)
	}
	for agent, launch := range cfg.Launches {
		if strings.TrimSpace(agent) == "" || strings.TrimSpace(launch) == "" || launch != strings.TrimSpace(launch) {
			return fmt.Errorf("hadron.agent_substrates.%s.tether.launches: logical agent %q needs a non-empty launch id", name, agent)
		}
	}
	if _, err := cfg.EffectiveUnreachableTimeout(); err != nil {
		return fmt.Errorf("hadron.agent_substrates.%s.tether.unreachable_timeout: %w", name, err)
	}
	return nil
}
