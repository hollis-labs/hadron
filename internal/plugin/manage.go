package plugin

import (
	"fmt"
	"os"
	"path/filepath"
)

// DisablePlugin disables a plugin by renaming plugin.yaml to plugin.yaml.disabled.
func DisablePlugin(pluginsDir, name string) error {
	dir := filepath.Join(pluginsDir, name)
	active := filepath.Join(dir, "plugin.yaml")
	disabled := filepath.Join(dir, "plugin.yaml.disabled")

	if _, err := os.Stat(disabled); err == nil {
		return fmt.Errorf("plugin %q is already disabled", name)
	}
	if _, err := os.Stat(active); err != nil {
		return fmt.Errorf("plugin %q is not installed (no plugin.yaml found)", name)
	}
	if err := os.Rename(active, disabled); err != nil {
		return fmt.Errorf("disable plugin %q: %w", name, err)
	}
	return nil
}

// EnablePlugin enables a previously disabled plugin.
func EnablePlugin(pluginsDir, name string) error {
	dir := filepath.Join(pluginsDir, name)
	active := filepath.Join(dir, "plugin.yaml")
	disabled := filepath.Join(dir, "plugin.yaml.disabled")

	if _, err := os.Stat(active); err == nil {
		return fmt.Errorf("plugin %q is already enabled", name)
	}
	if _, err := os.Stat(disabled); err != nil {
		return fmt.Errorf("plugin %q has no disabled manifest", name)
	}
	if err := os.Rename(disabled, active); err != nil {
		return fmt.Errorf("enable plugin %q: %w", name, err)
	}
	return nil
}

// IsDisabled returns true if the plugin directory has plugin.yaml.disabled.
func IsDisabled(pluginsDir, name string) bool {
	disabled := filepath.Join(pluginsDir, name, "plugin.yaml.disabled")
	_, err := os.Stat(disabled)
	return err == nil
}

// PluginStatusString determines the runtime status of an installed plugin.
func PluginStatusString(pluginsDir, name string) string {
	dir := filepath.Join(pluginsDir, name)
	if _, err := os.Stat(filepath.Join(dir, "plugin.yaml.disabled")); err == nil {
		return "disabled"
	}
	if _, err := os.Stat(filepath.Join(dir, "plugin.yaml")); err != nil {
		return "not-installed"
	}
	if _, ok := LookupConstructor(name); !ok {
		manifest, err := ParseManifest(filepath.Join(dir, "plugin.yaml"))
		if err != nil {
			return "no-binary"
		}
		if _, ok := LookupConstructor(manifest.Name); !ok {
			return "no-binary"
		}
	}
	return "active"
}
