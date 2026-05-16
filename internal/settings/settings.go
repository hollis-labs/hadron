package settings

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Settings struct {
	BlueprintDir string            `json:"blueprint_dir"`
	Execution    ExecutionSettings `json:"execution"`
	Safety       SafetySettings    `json:"safety"`
	Telemetry    TelemetrySettings `json:"telemetry"`
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
		BlueprintDir: DefaultBlueprintDir(),
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
