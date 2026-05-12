package config

import (
	"fmt"
	"os"
	"path/filepath"
)

const DefaultAddr = "127.0.0.1:8095"

// Config holds runtime configuration for hadrond and the hadron CLI.
type Config struct {
	Addr        string // daemon listen address
	DBPath      string // SQLite database path
	LogsDir     string // run event logs directory
	DataDir     string // settings + misc data directory
	WorkspaceID string // default workspace ID
}

// Default returns a Config with paths rooted in ~/.hadron/.
func Default() *Config {
	home, _ := os.UserHomeDir()
	if home == "" {
		home = "."
	}
	base := filepath.Join(home, ".hadron")
	return &Config{
		Addr:        DefaultAddr,
		DBPath:      filepath.Join(base, "state", "hadron.db"),
		LogsDir:     filepath.Join(base, "logs", "runs"),
		DataDir:     base,
		WorkspaceID: "default",
	}
}

// Ensure creates all required directories if they do not exist.
func (c *Config) Ensure() error {
	dirs := []string{
		filepath.Dir(c.DBPath),
		c.LogsDir,
		c.DataDir,
	}
	seen := map[string]bool{}
	for _, dir := range dirs {
		if seen[dir] {
			continue
		}
		seen[dir] = true
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("ensure dir %s: %w", dir, err)
		}
	}
	return nil
}
