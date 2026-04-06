package plugin

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/hollis-labs/plugin"
)

// Host implements the plugin.Host interface for Hadron.
// It provides the runtime environment and services for plugins.
type Host struct {
	mu           sync.RWMutex
	plugins      map[string]plugin.Plugin
	eventHooks   map[string][]plugin.EventHook
	crudHandlers map[string]plugin.CRUDHandler
	uiComponents []plugin.UIComponent
	services     map[string]interface{}
	configs      map[string]*PluginConfig
	activePlugin string
	router       *http.ServeMux
	logger       plugin.Logger
	ctx          context.Context
	ctxCancel    context.CancelFunc
}

// NewHost creates a new plugin host for Hadron.
func NewHost(router *http.ServeMux, logger plugin.Logger) *Host {
	ctx, cancel := context.WithCancel(context.Background())
	return &Host{
		plugins:      make(map[string]plugin.Plugin),
		eventHooks:   make(map[string][]plugin.EventHook),
		crudHandlers: make(map[string]plugin.CRUDHandler),
		uiComponents: []plugin.UIComponent{},
		services:     make(map[string]interface{}),
		configs:      make(map[string]*PluginConfig),
		router:       router,
		logger:       logger,
		ctx:          ctx,
		ctxCancel:    cancel,
	}
}

// NewHostMinimal creates a minimal plugin host without an HTTP router.
// Used by CLI commands that need to run plugin lifecycle methods without a full server.
func NewHostMinimal() *Host {
	ctx, cancel := context.WithCancel(context.Background())
	return &Host{
		plugins:      make(map[string]plugin.Plugin),
		eventHooks:   make(map[string][]plugin.EventHook),
		crudHandlers: make(map[string]plugin.CRUDHandler),
		uiComponents: []plugin.UIComponent{},
		services:     make(map[string]interface{}),
		configs:      make(map[string]*PluginConfig),
		router:       http.NewServeMux(),
		logger:       NewLogger("plugin-cli"),
		ctx:          ctx,
		ctxCancel:    cancel,
	}
}

// GetPlugin retrieves another loaded plugin by ID.
func (h *Host) GetPlugin(id string) (plugin.Plugin, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	p, exists := h.plugins[id]
	return p, exists
}

// RegisterCRUDHandler registers a CRUD handler for a resource type.
// Hadron has an HTTP server so CRUD handlers are wired into the router.
func (h *Host) RegisterCRUDHandler(resourceType string, handler plugin.CRUDHandler) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, alreadyRegistered := h.crudHandlers[resourceType]
	h.crudHandlers[resourceType] = handler

	if alreadyRegistered {
		return nil
	}

	h.logger.Info("registered CRUD handler", "resourceType", resourceType)
	return nil
}

// RegisterEventHook registers an event hook for specific event types.
func (h *Host) RegisterEventHook(eventTypes []string, hook plugin.EventHook) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, eventType := range eventTypes {
		h.eventHooks[eventType] = append(h.eventHooks[eventType], hook)
	}

	h.logger.Info("registered event hook", "eventTypes", eventTypes)
	return nil
}

// RegisterUIComponent is a no-op for Hadron (no frontend).
func (h *Host) RegisterUIComponent(component plugin.UIComponent) error {
	h.logger.Warn("RegisterUIComponent called but Hadron has no frontend — ignoring", "id", component.ID)
	return nil
}

// SetConfig persists a configuration value for the calling plugin.
func (h *Host) SetConfig(key string, value string) error {
	h.logger.Warn("SetConfig not yet implemented in Hadron", "key", key)
	return nil
}

// RegisterConfigSchema registers config field definitions for the calling plugin.
func (h *Host) RegisterConfigSchema(fields []plugin.ConfigFieldDef) error {
	h.logger.Warn("RegisterConfigSchema not yet implemented in Hadron")
	return nil
}

// RegisterConnector registers a named connector (webhook, email sender, etc.).
func (h *Host) RegisterConnector(name string, connector plugin.Connector) error {
	h.logger.Warn("RegisterConnector not yet implemented in Hadron", "name", name)
	return nil
}

// RegisterProvider registers a runtime LLM provider.
func (h *Host) RegisterProvider(name string, provider interface{}) error {
	h.logger.Warn("RegisterProvider not yet implemented in Hadron", "name", name)
	return nil
}

// RegisterCLIAdapter registers a runtime CLI adapter for PTY/subprocess bridges.
func (h *Host) RegisterCLIAdapter(name string, adapter interface{}) error {
	h.logger.Warn("RegisterCLIAdapter not yet implemented in Hadron", "name", name)
	return nil
}

// GetService provides access to core services.
func (h *Host) GetService(name string) (interface{}, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	service, exists := h.services[name]
	if !exists {
		return nil, fmt.Errorf("service %q not found", name)
	}
	return service, nil
}

// RegisterService registers a core service for plugin access.
func (h *Host) RegisterService(name string, service interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.services[name] = service
	h.logger.Info("registered service", "name", name)
}

// Logger provides a logger instance for the plugin.
func (h *Host) Logger() plugin.Logger {
	return h.logger
}

// Context returns the plugin's execution context.
func (h *Host) Context() context.Context {
	return h.ctx
}

// SetPluginConfig stores configuration for a plugin.
func (h *Host) SetPluginConfig(pluginID string, cfg *PluginConfig) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.configs[pluginID] = cfg
}

// GetConfig returns a configuration value for the currently-loading plugin.
func (h *Host) GetConfig(key string) (string, error) {
	h.mu.RLock()
	id := h.activePlugin
	cfg := h.configs[id]
	h.mu.RUnlock()

	if cfg == nil {
		return "", fmt.Errorf("no config loaded for plugin %q", id)
	}
	return cfg.Get(key)
}

// LoadPlugin loads a plugin into the host.
func (h *Host) LoadPlugin(p plugin.Plugin) error {
	id := p.ID()

	h.mu.Lock()
	if _, exists := h.plugins[id]; exists {
		h.mu.Unlock()
		return fmt.Errorf("plugin %q already loaded", id)
	}
	for _, dep := range p.Dependencies() {
		if _, exists := h.plugins[dep]; !exists {
			h.mu.Unlock()
			return fmt.Errorf("plugin %q depends on %q which is not loaded", id, dep)
		}
	}
	h.activePlugin = id
	h.mu.Unlock()

	if err := p.Load(h); err != nil {
		return fmt.Errorf("failed to load plugin %q: %w", id, err)
	}

	h.mu.Lock()
	h.plugins[id] = p
	h.mu.Unlock()

	h.logger.Info("loaded plugin", "id", id, "name", p.Name(), "version", p.Version())
	return nil
}

// UnloadPlugin unloads a plugin from the host.
func (h *Host) UnloadPlugin(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	p, exists := h.plugins[id]
	if !exists {
		return fmt.Errorf("plugin %q not found", id)
	}

	for _, other := range h.plugins {
		if other.ID() == id {
			continue
		}
		for _, dep := range other.Dependencies() {
			if dep == id {
				return fmt.Errorf("cannot unload plugin %q: plugin %q depends on it", id, other.ID())
			}
		}
	}

	if err := p.Unload(); err != nil {
		return fmt.Errorf("failed to unload plugin %q: %w", id, err)
	}

	delete(h.plugins, id)
	h.logger.Info("unloaded plugin", "id", id)
	return nil
}

// EmitEvent emits an event to all registered hooks.
func (h *Host) EmitEvent(event plugin.Event) {
	h.mu.RLock()
	hooks, exists := h.eventHooks[event.Type]
	h.mu.RUnlock()

	if !exists {
		return
	}

	var wg sync.WaitGroup
	for _, hook := range hooks {
		wg.Add(1)
		go func(hook plugin.EventHook) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
			defer cancel()

			if err := hook.Handle(ctx, event); err != nil {
				h.logger.Error("event hook failed", "eventType", event.Type, "error", err)
			}
		}(hook)
	}
	wg.Wait()
}

// ListPlugins returns all loaded plugins.
func (h *Host) ListPlugins() []plugin.Plugin {
	h.mu.RLock()
	defer h.mu.RUnlock()

	plugins := make([]plugin.Plugin, 0, len(h.plugins))
	for _, p := range h.plugins {
		plugins = append(plugins, p)
	}
	return plugins
}

// Shutdown gracefully shuts down the plugin host and all loaded plugins.
func (h *Host) Shutdown() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var errors []string
	for id, p := range h.plugins {
		if err := p.Unload(); err != nil {
			errors = append(errors, fmt.Sprintf("failed to unload plugin %q: %v", id, err))
		}
	}

	h.ctxCancel()

	if len(errors) > 0 {
		return fmt.Errorf("shutdown errors: %v", errors)
	}

	h.logger.Info("plugin host shutdown complete")
	return nil
}
