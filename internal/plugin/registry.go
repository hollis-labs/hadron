package plugin

import (
	"fmt"
	"sync"

	fplugin "github.com/hollis-labs/fragments-engine/plugin"
)

// PluginConstructor creates a new plugin instance.
type PluginConstructor func() fplugin.Plugin

var (
	registryMu     sync.RWMutex
	pluginRegistry = map[string]PluginConstructor{}
)

// RegisterPlugin registers a plugin constructor by ID.
// Plugins call this (typically from init()) to make themselves discoverable.
func RegisterPlugin(id string, constructor PluginConstructor) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := pluginRegistry[id]; exists {
		panic(fmt.Sprintf("plugin %q already registered", id))
	}
	pluginRegistry[id] = constructor
}

// GetRegistered returns a copy of all registered plugin constructors.
func GetRegistered() map[string]PluginConstructor {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make(map[string]PluginConstructor, len(pluginRegistry))
	for k, v := range pluginRegistry {
		out[k] = v
	}
	return out
}

// LookupConstructor returns the constructor for a plugin ID, if registered.
func LookupConstructor(id string) (PluginConstructor, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	c, ok := pluginRegistry[id]
	return c, ok
}
