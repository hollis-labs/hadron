package plugin

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// PluginManifest represents the parsed plugin.yaml file.
type PluginManifest struct {
	Name         string                 `yaml:"name"`
	Version      string                 `yaml:"version"`
	Description  string                 `yaml:"description"`
	Author       string                 `yaml:"author"`
	URL          string                 `yaml:"url"`
	ShortDesc    string                 `yaml:"short_desc"`
	Config       map[string]ConfigEntry `yaml:"config"`
	Dependencies []string               `yaml:"dependencies"`
	Requires     map[string]interface{} `yaml:"requires"`
}

// ConfigEntry describes a single configuration value in plugin.yaml.
type ConfigEntry struct {
	Type        string `yaml:"type"`
	Required    bool   `yaml:"required"`
	EnvVar      string `yaml:"env_var"`
	Default     string `yaml:"default"`
	Description string `yaml:"description"`
}

// PluginConfig holds the resolved configuration for a single plugin.
type PluginConfig struct {
	pluginID  string
	schema    map[string]ConfigEntry
	overrides map[string]string
}

// NewPluginConfig builds a PluginConfig by parsing the plugin.yaml in the given directory.
func NewPluginConfig(pluginID, pluginDir string) (*PluginConfig, error) {
	pc := &PluginConfig{
		pluginID:  pluginID,
		schema:    make(map[string]ConfigEntry),
		overrides: make(map[string]string),
	}

	manifestPath := filepath.Join(pluginDir, "plugin.yaml")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return pc, nil
		}
		return nil, fmt.Errorf("read plugin.yaml: %w", err)
	}

	var manifest PluginManifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse plugin.yaml: %w", err)
	}
	pc.schema = manifest.Config

	configPath := filepath.Join(pluginDir, "config.yaml")
	if cfgData, err := os.ReadFile(configPath); err == nil {
		var overrides map[string]string
		if err := yaml.Unmarshal(cfgData, &overrides); err == nil {
			pc.overrides = overrides
		}
	}

	return pc, nil
}

// ParseManifest reads and parses a plugin.yaml file.
func ParseManifest(path string) (*PluginManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m PluginManifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &m, nil
}

// Get resolves a config value. Resolution order:
//  1. Environment variable (from schema env_var)
//  2. Override from config.yaml
//  3. Default from schema
//  4. Error if required
func (pc *PluginConfig) Get(key string) (string, error) {
	entry, ok := pc.schema[key]
	if !ok {
		return "", fmt.Errorf("unknown config key %q for plugin %s", key, pc.pluginID)
	}

	if entry.EnvVar != "" {
		if v := os.Getenv(entry.EnvVar); v != "" {
			return v, nil
		}
	}

	if v, ok := pc.overrides[key]; ok && v != "" {
		return v, nil
	}

	if entry.Default != "" {
		return entry.Default, nil
	}

	if entry.Required {
		return "", fmt.Errorf("required config key %q not set for plugin %s (env: %s)", key, pc.pluginID, entry.EnvVar)
	}

	return "", nil
}
