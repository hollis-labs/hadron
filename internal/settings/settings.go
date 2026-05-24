package settings

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type Settings struct {
	BlueprintDir      string                             `json:"blueprint_dir"`
	MCPServers        map[string]MCPServerSettings       `json:"mcp_servers,omitempty"`
	AgentSubstrates   map[string]AgentSubstrateSettings  `json:"agent_substrates,omitempty"`
	MessageSubstrates map[string]MessageSubstrateSetting `json:"message_substrates,omitempty"`
	Execution         ExecutionSettings                  `json:"execution"`
	Safety            SafetySettings                     `json:"safety"`
	Telemetry         TelemetrySettings                  `json:"telemetry"`
}

type MCPServerSettings struct {
	Transport      string            `json:"transport"`
	Command        string            `json:"command,omitempty"`
	Args           []string          `json:"args,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	URL            string            `json:"url,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
}

type AgentSubstrateSettings struct {
	Kind                   string            `json:"kind"`
	Provider               string            `json:"provider,omitempty"`
	Runtime                string            `json:"runtime,omitempty"`
	Authority              string            `json:"authority,omitempty"`
	WorkingDirMode         string            `json:"working_dir_mode,omitempty"`
	AllowGenericSubprocess bool              `json:"allow_generic_subprocess,omitempty"`
	Command                string            `json:"command,omitempty"`
	Args                   []string          `json:"args,omitempty"`
	Env                    map[string]string `json:"env,omitempty"`
	BaseURL                string            `json:"base_url,omitempty"`
	Headers                map[string]string `json:"headers,omitempty"`
	TimeoutSeconds         int               `json:"timeout_seconds,omitempty"`
	Boot                   AgentBootSettings `json:"boot,omitempty"`
}

type AgentBootSettings struct {
	Profile          string `json:"profile,omitempty"`
	CallbacksProfile string `json:"callbacks_profile,omitempty"`
	PlantNativeFiles bool   `json:"plant_native_files,omitempty"`
}

type MessageSubstrateSetting struct {
	Kind           string            `json:"kind"`
	Authority      string            `json:"authority,omitempty"`
	Command        string            `json:"command,omitempty"`
	Args           []string          `json:"args,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	BaseURL        string            `json:"base_url,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	NotifyWake     bool              `json:"notify_wake,omitempty"`
}

type TelemetrySettings struct {
	Enabled    bool `json:"enabled"`
	RetainDays int  `json:"retainDays"`
}

type ExecutionSettings struct {
	AllowedCommands   []string `json:"allowedCommands"`
	DeniedCommands    []string `json:"deniedCommands"`
	AllowedDirs       []string `json:"allowedDirs"`
	DeniedDirs        []string `json:"deniedDirs"`
	MaxConcurrentJobs int      `json:"maxConcurrentJobs"`
	DefaultTimeout    int      `json:"defaultTimeout"`
	Workers           int      `json:"workers"`
}

type SafetySettings struct {
	RequireConfirmation bool `json:"requireConfirmation"`
	DryRunByDefault     bool `json:"dryRunByDefault"`
	BlockSudo           bool `json:"blockSudo"`
	SandboxMode         bool `json:"sandboxMode"`
}

// DefaultBlueprintDir returns the default blueprint directory path.
func DefaultBlueprintDir() string {
	home, _ := os.UserHomeDir()
	if home == "" {
		home = "."
	}
	return filepath.Join(home, ".hadron", "blueprints")
}

func DefaultSettings() *Settings {
	return &Settings{
		BlueprintDir:      DefaultBlueprintDir(),
		MCPServers:        map[string]MCPServerSettings{},
		AgentSubstrates:   map[string]AgentSubstrateSettings{},
		MessageSubstrates: map[string]MessageSubstrateSetting{},
		Telemetry: TelemetrySettings{
			Enabled:    true,
			RetainDays: 30,
		},
		Execution: ExecutionSettings{
			AllowedCommands:   []string{},
			DeniedCommands:    []string{"rm -rf /", "dd", "mkfs", "format", "shutdown", "reboot"},
			AllowedDirs:       []string{},
			DeniedDirs:        []string{"/", "/System", "/Library", "/bin", "/sbin", "/usr", "/etc"},
			MaxConcurrentJobs: 3,
			DefaultTimeout:    300,
			Workers:           3,
		},
		Safety: SafetySettings{
			RequireConfirmation: true,
			DryRunByDefault:     false,
			BlockSudo:           false,
			SandboxMode:         false,
		},
	}
}

func Load(dataDir string) (*Settings, error) {
	settingsPath := filepath.Join(dataDir, "settings.json")

	data, err := os.ReadFile(settingsPath) // #nosec G304 -- settingsPath is derived from Hadron's configured data directory.
	if err != nil {
		if os.IsNotExist(err) {
			return DefaultSettings(), nil
		}
		return nil, fmt.Errorf("failed to read settings: %w", err)
	}

	var s Settings
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("failed to parse settings: %w", err)
	}

	// Fill default for blueprint_dir if not set (backward compat).
	if s.BlueprintDir == "" {
		s.BlueprintDir = DefaultBlueprintDir()
	}
	if s.MCPServers == nil {
		s.MCPServers = map[string]MCPServerSettings{}
	}
	if s.AgentSubstrates == nil {
		s.AgentSubstrates = map[string]AgentSubstrateSettings{}
	}
	if s.MessageSubstrates == nil {
		s.MessageSubstrates = map[string]MessageSubstrateSetting{}
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}

	return &s, nil
}

func (s *Settings) Save(dataDir string) error {
	settingsPath := filepath.Join(dataDir, "settings.json")

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal settings: %w", err)
	}

	if err := os.WriteFile(settingsPath, data, 0o600); err != nil {
		return fmt.Errorf("failed to write settings: %w", err)
	}

	return nil
}

func (s *Settings) ValidateCommand(cmd string) error {
	if s.Safety.BlockSudo && (strings.HasPrefix(cmd, "sudo ") || strings.Contains(cmd, " sudo ")) {
		return fmt.Errorf("sudo commands are blocked by safety settings")
	}

	if len(s.Execution.AllowedCommands) > 0 {
		allowed := false
		for _, pattern := range s.Execution.AllowedCommands {
			if strings.Contains(cmd, pattern) {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("command not in allowed list")
		}
	}

	for _, pattern := range s.Execution.DeniedCommands {
		if strings.Contains(cmd, pattern) {
			return fmt.Errorf("command matches denied pattern: %s", pattern)
		}
	}

	return nil
}

func (s *Settings) ValidatePath(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("failed to resolve path: %w", err)
	}

	if len(s.Execution.AllowedDirs) > 0 {
		allowed := false
		for _, dir := range s.Execution.AllowedDirs {
			absDir, _ := filepath.Abs(dir)
			if strings.HasPrefix(absPath, absDir+string(filepath.Separator)) || absPath == absDir {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("path not in allowed directories")
		}
	}

	for _, dir := range s.Execution.DeniedDirs {
		absDir, _ := filepath.Abs(dir)
		if absDir == "/" {
			if absPath == "/" {
				return fmt.Errorf("path is in denied directory: %s", dir)
			}
			continue
		}
		if strings.HasPrefix(absPath, absDir+string(filepath.Separator)) || absPath == absDir {
			return fmt.Errorf("path is in denied directory: %s", dir)
		}
	}

	return nil
}

func (s *Settings) GetDefaultTimeout() int {
	return s.Execution.DefaultTimeout
}

func (s *Settings) Validate() error {
	for name, cfg := range s.AgentSubstrates {
		if strings.TrimSpace(cfg.Kind) != "go_agent_runtime" {
			return fmt.Errorf("agent_substrates.%s.kind: unsupported kind %q", name, cfg.Kind)
		}
		if strings.TrimSpace(cfg.Provider) == "" {
			return fmt.Errorf("agent_substrates.%s.provider: required", name)
		}
		if strings.TrimSpace(cfg.Runtime) == "" {
			return fmt.Errorf("agent_substrates.%s.runtime: required", name)
		}
		switch strings.TrimSpace(cfg.WorkingDirMode) {
		case "", "blueprint_dir", "step_dir", "cwd", "process_cwd":
		default:
			return fmt.Errorf("agent_substrates.%s.working_dir_mode: unsupported value %q", name, cfg.WorkingDirMode)
		}
		if cfg.TimeoutSeconds < 0 {
			return fmt.Errorf("agent_substrates.%s.timeout_seconds: must be >= 0", name)
		}
	}
	for name, cfg := range s.MessageSubstrates {
		switch strings.TrimSpace(cfg.Kind) {
		case "go_messaging":
		case "go_messaging_http", "tether_http":
			if strings.TrimSpace(cfg.BaseURL) == "" {
				return fmt.Errorf("message_substrates.%s.base_url: required", name)
			}
			if _, err := url.ParseRequestURI(cfg.BaseURL); err != nil {
				return fmt.Errorf("message_substrates.%s.base_url: %w", name, err)
			}
		default:
			return fmt.Errorf("message_substrates.%s.kind: unsupported kind %q", name, cfg.Kind)
		}
		if cfg.TimeoutSeconds < 0 {
			return fmt.Errorf("message_substrates.%s.timeout_seconds: must be >= 0", name)
		}
	}
	return nil
}
