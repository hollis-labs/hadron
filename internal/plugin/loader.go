package plugin

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	fplugin "github.com/hollis-labs/fragments-engine/plugin"
)

// DiscoveredPlugin holds metadata parsed from a plugin.yaml plus the
// constructor looked up from the registry.
type DiscoveredPlugin struct {
	Manifest    *PluginManifest
	Dir         string
	Constructor PluginConstructor
}

// DiscoverPlugins scans the given directory for subdirectories containing
// plugin.yaml, parses each manifest, and looks up the registered constructor.
// Returns only plugins that have a matching constructor in the registry.
func DiscoverPlugins(pluginsDir string) ([]DiscoveredPlugin, error) {
	entries, err := os.ReadDir(pluginsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read plugins directory: %w", err)
	}

	var discovered []DiscoveredPlugin
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		dir := filepath.Join(pluginsDir, entry.Name())
		manifestPath := filepath.Join(dir, "plugin.yaml")

		if _, err := os.Stat(manifestPath); err != nil {
			continue // no plugin.yaml — skip
		}

		manifest, err := ParseManifest(manifestPath)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", manifestPath, err)
		}

		constructor, ok := LookupConstructor(manifest.Name)
		if !ok {
			continue
		}

		discovered = append(discovered, DiscoveredPlugin{
			Manifest:    manifest,
			Dir:         dir,
			Constructor: constructor,
		})
	}

	return discovered, nil
}

// LoadDiscovered instantiates and loads all discovered plugins into the host,
// respecting dependency order.
func LoadDiscovered(host *Host, discovered []DiscoveredPlugin) ([]fplugin.Plugin, []error) {
	sorted := sortByDeps(discovered)

	var loaded []fplugin.Plugin
	var errs []error

	for _, dp := range sorted {
		cfg, err := NewPluginConfig(dp.Manifest.Name, dp.Dir)
		if err != nil {
			errs = append(errs, fmt.Errorf("config for %s: %w", dp.Manifest.Name, err))
			continue
		}

		host.SetPluginConfig(dp.Manifest.Name, cfg)

		p := dp.Constructor()
		if err := host.LoadPlugin(p); err != nil {
			errs = append(errs, fmt.Errorf("load %s: %w", dp.Manifest.Name, err))
			continue
		}
		loaded = append(loaded, p)
	}

	return loaded, errs
}

func sortByDeps(plugins []DiscoveredPlugin) []DiscoveredPlugin {
	out := make([]DiscoveredPlugin, len(plugins))
	copy(out, plugins)
	sort.SliceStable(out, func(i, j int) bool {
		return len(out[i].Manifest.Dependencies) < len(out[j].Manifest.Dependencies)
	})
	return out
}
