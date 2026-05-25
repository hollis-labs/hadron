package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hollis-labs/hadron/internal/blueprint"
	hadronpipeline "github.com/hollis-labs/hadron/internal/pipeline"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/hadron/internal/telemetry"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

func closeBody(body io.Closer) {
	_ = body.Close()
}

// FileEntry represents a file or directory in the browser.
type FileEntry struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	IsDir bool   `json:"isDir"`
}

// BlueprintInput represents a single input parameter from a blueprint.
type BlueprintInput struct {
	Name        string   `json:"name"`
	Label       string   `json:"label"`
	Description string   `json:"description"`
	Type        string   `json:"type"` // string|number|boolean|array
	Required    bool     `json:"required"`
	Default     string   `json:"default"` // stringified for simplicity
	Enum        []string `json:"enum"`    // stringified
}

// ValidateResult is the response from blueprint validation.
type ValidateResult struct {
	Valid bool   `json:"valid"`
	Error string `json:"error,omitempty"`
}

// App is the Wails application struct. It manages the hadrond child process.
type App struct {
	ctx        context.Context
	daemonCmd  *exec.Cmd
	daemonAddr string
	dataDir    string
	status     string
	mu         sync.Mutex
}

// NewApp creates a new App instance.
func NewApp() *App {
	home, _ := os.UserHomeDir()
	if home == "" {
		home = "."
	}
	return &App{
		dataDir: filepath.Join(home, ".hadron"),
		status:  "stopped",
	}
}

// startup is called by Wails when the app starts. It launches hadrond.
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	go a.startDaemon()
}

// shutdown is called by Wails when the app closes. It kills hadrond only if
// the GUI spawned it (not in external mode).
func (a *App) shutdown(ctx context.Context) {
	a.mu.Lock()
	cmd := a.daemonCmd
	a.mu.Unlock()

	// Only kill daemon if we spawned it (daemonCmd is nil in external/adopt mode)
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
}

// GetDaemonAddr returns the current daemon address (exposed to frontend).
func (a *App) GetDaemonAddr() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.daemonAddr
}

// GetDaemonStatus returns the daemon status: "running", "stopped", or "error" (exposed to frontend).
func (a *App) GetDaemonStatus() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

// OpenDirectoryDialog opens a file picker filtered to .yaml/.yml files and returns
// the parent directory of the chosen file. This lets users navigate to and click
// a .yaml file directly rather than hunting for the containing folder.
func (a *App) OpenDirectoryDialog() string {
	file, err := runtime.OpenFileDialog(a.ctx, runtime.OpenDialogOptions{
		Title: "Select a Blueprint File",
		Filters: []runtime.FileFilter{
			{DisplayName: "Blueprint Files (*.yaml, *.yml)", Pattern: "*.yaml;*.yml"},
		},
	})
	if err != nil || file == "" {
		return ""
	}
	return filepath.Dir(file)
}

// SelectBlueprintFile opens a file picker and returns the absolute path of the
// chosen file (not its parent directory). Used by the Scheduler form's Browse button.
func (a *App) SelectBlueprintFile() string {
	file, _ := runtime.OpenFileDialog(a.ctx, runtime.OpenDialogOptions{
		Title: "Select Blueprint File",
		Filters: []runtime.FileFilter{
			{DisplayName: "Blueprint Files (*.yaml, *.yml)", Pattern: "*.yaml;*.yml"},
		},
	})
	return file
}

// SelectDirectoryDialog opens a native directory picker and returns the chosen path (exposed to frontend).
func (a *App) SelectDirectoryDialog() string {
	dir, err := runtime.OpenDirectoryDialog(a.ctx, runtime.OpenDialogOptions{
		Title: "Select Directory",
	})
	if err != nil {
		return ""
	}
	return dir
}

// ListFilesInDir lists files and directories in the given path.
func (a *App) ListFilesInDir(dir string) ([]FileEntry, error) {
	return a.listFilesInDir(dir, fileKindAny)
}

// ListBlueprintFilesInDir lists directories and blueprint YAML files in the given path.
func (a *App) ListBlueprintFilesInDir(dir string) ([]FileEntry, error) {
	return a.listFilesInDir(dir, fileKindBlueprint)
}

// ListPipelineFilesInDir lists directories and pipeline YAML files in the given path.
func (a *App) ListPipelineFilesInDir(dir string) ([]FileEntry, error) {
	return a.listFilesInDir(dir, fileKindPipeline)
}

type fileKind int

const (
	fileKindAny fileKind = iota
	fileKindBlueprint
	fileKindPipeline
)

func (a *App) listFilesInDir(dir string, kind fileKind) ([]FileEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
	}

	var items []FileEntry
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		entry := FileEntry{
			Name:  name,
			Path:  filepath.Join(dir, name),
			IsDir: e.IsDir(),
		}
		if e.IsDir() {
			items = append(items, entry)
			continue
		}

		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		if kind != fileKindAny {
			matches, matchErr := fileMatchesKind(entry.Path, kind)
			if matchErr != nil || !matches {
				continue
			}
		}
		items = append(items, entry)
	}
	return items, nil
}

func fileMatchesKind(path string, kind fileKind) (bool, error) {
	switch kind {
	case fileKindBlueprint:
		if _, err := blueprint.ParseFile(path); err != nil {
			return false, err
		}
		return true, nil
	case fileKindPipeline:
		if _, err := hadronpipeline.ParseFile(path); err != nil {
			return false, err
		}
		return true, nil
	default:
		return true, nil
	}
}

// ValidateBlueprintFile reads the file at path and validates it via hadrond.
func (a *App) ValidateBlueprintFile(path string) ValidateResult {
	a.mu.Lock()
	addr := a.daemonAddr
	a.mu.Unlock()

	if addr == "" {
		return ValidateResult{Valid: false, Error: "daemon not running"}
	}

	data, err := os.ReadFile(path) // #nosec G304 -- desktop app validates the user-selected blueprint file.
	if err != nil {
		return ValidateResult{Valid: false, Error: "read file: " + err.Error()}
	}

	url := fmt.Sprintf("http://%s/v1/blueprints/validate", addr)
	// #nosec G107 -- addr is the local daemon address configured by the desktop app.
	resp, err := http.Post(url, "application/x-yaml", bytes.NewReader(data))
	if err != nil {
		return ValidateResult{Valid: false, Error: "call api: " + err.Error()}
	}
	defer closeBody(resp.Body)

	body, _ := io.ReadAll(resp.Body)
	var result struct {
		Valid bool   `json:"valid"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return ValidateResult{Valid: false, Error: "parse response: " + err.Error()}
	}
	return ValidateResult{Valid: result.Valid, Error: result.Error}
}

// prefsPath returns the path to the preferences file.
func (a *App) prefsPath() string {
	return filepath.Join(a.dataDir, "preferences.json")
}

// GetPreference returns the value for a preference key (exposed to frontend).
func (a *App) GetPreference(key string) string {
	data, _ := os.ReadFile(a.prefsPath())
	var m map[string]string
	_ = json.Unmarshal(data, &m)
	return m[key]
}

// SetPreference sets a preference key/value pair and persists it (exposed to frontend).
func (a *App) SetPreference(key, value string) {
	p := a.prefsPath()
	data, _ := os.ReadFile(p) // #nosec G304 -- preferences path is derived from the app data directory.
	m := map[string]string{}
	_ = json.Unmarshal(data, &m)
	m[key] = value
	out, _ := json.MarshalIndent(m, "", "  ")
	_ = os.WriteFile(p, out, 0o600)
}

// BlueprintMetaSummary is a lightweight summary of a blueprint's metadata.
type BlueprintMetaSummary struct {
	Name         string   `json:"name"`
	Slug         string   `json:"slug"`
	Title        string   `json:"title"`
	Description  string   `json:"description"`
	Tags         []string `json:"tags"`
	Version      string   `json:"version"`
	InputCount   int      `json:"input_count"`
	StepCount    int      `json:"step_count"`
	SectionCount int      `json:"section_count"`
	HasImports   bool     `json:"has_imports"`
}

// ReadBlueprintFile reads raw YAML content from disk (exposed to frontend).
func (a *App) ReadBlueprintFile(path string) (string, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- desktop app reads the user-selected blueprint file.
	if err != nil {
		return "", fmt.Errorf("read blueprint: %w", err)
	}
	return string(data), nil
}

// ParseBlueprintFull parses a blueprint file and returns the full structure as JSON (exposed to frontend).
func (a *App) ParseBlueprintFull(path string) (string, error) {
	bp, err := blueprint.ParseFile(path)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(bp)
	if err != nil {
		return "", fmt.Errorf("marshal blueprint: %w", err)
	}
	return string(data), nil
}

// SaveBlueprintFile writes YAML content to an existing file (exposed to frontend).
func (a *App) SaveBlueprintFile(path string, content string) error {
	// Preserve the existing file's permissions — saving must not silently
	// strip group/world readability from a shared, source-controlled blueprint.
	mode := os.FileMode(0o644)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	}
	return os.WriteFile(path, []byte(content), mode)
}

// CreateBlueprintFile creates a new blueprint file and returns its full path (exposed to frontend).
func (a *App) CreateBlueprintFile(dir string, filename string, content string) (string, error) {
	if !strings.HasSuffix(filename, ".yaml") && !strings.HasSuffix(filename, ".yml") {
		filename = filename + ".yaml"
	}
	fullPath := filepath.Join(dir, filename)
	if _, err := os.Stat(fullPath); err == nil {
		return "", fmt.Errorf("file already exists: %s", fullPath)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0o600); err != nil {
		return "", fmt.Errorf("create blueprint: %w", err)
	}
	return fullPath, nil
}

// DeleteBlueprintFile deletes a blueprint file from disk (exposed to frontend).
func (a *App) DeleteBlueprintFile(path string) error {
	return os.Remove(path)
}

// MoveBlueprintFile moves a blueprint file to a new directory (exposed to frontend).
func (a *App) MoveBlueprintFile(srcPath, destDir string) (string, error) {
	name := filepath.Base(srcPath)
	dest := filepath.Join(destDir, name)
	if _, err := os.Stat(dest); err == nil {
		return "", fmt.Errorf("file already exists at destination: %s", dest)
	}
	if err := os.Rename(srcPath, dest); err != nil {
		return "", fmt.Errorf("move file: %w", err)
	}
	return dest, nil
}

// CopyBlueprintFile copies a blueprint file to a new directory (exposed to frontend).
func (a *App) CopyBlueprintFile(srcPath, destDir string) (string, error) {
	name := filepath.Base(srcPath)
	dest := filepath.Join(destDir, name)
	if _, err := os.Stat(dest); err == nil {
		return "", fmt.Errorf("file already exists at destination: %s", dest)
	}
	data, err := os.ReadFile(srcPath) // #nosec G304 -- desktop app copies the user-selected source file.
	if err != nil {
		return "", fmt.Errorf("read source: %w", err)
	}
	if err := os.WriteFile(dest, data, 0o600); err != nil { // #nosec G703 -- destination directory is selected by the desktop user.
		return "", fmt.Errorf("write destination: %w", err)
	}
	return dest, nil
}

// ArchiveBlueprintFile moves a blueprint file to ~/.hadron/archive/ (exposed to frontend).
func (a *App) ArchiveBlueprintFile(srcPath string) error {
	archiveDir := filepath.Join(a.dataDir, "archive")
	if err := os.MkdirAll(archiveDir, 0o750); err != nil {
		return fmt.Errorf("create archive dir: %w", err)
	}
	name := filepath.Base(srcPath)
	dest := filepath.Join(archiveDir, name)
	// If file exists in archive, add a timestamp suffix
	if _, err := os.Stat(dest); err == nil {
		ext := filepath.Ext(name)
		base := strings.TrimSuffix(name, ext)
		dest = filepath.Join(archiveDir, fmt.Sprintf("%s_%d%s", base, time.Now().Unix(), ext))
	}
	return os.Rename(srcPath, dest)
}

// CreateDirectory creates a new subdirectory inside parentDir (exposed to frontend).
func (a *App) CreateDirectory(parentDir, name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("directory name is required")
	}
	full := filepath.Join(parentDir, name)
	if _, err := os.Stat(full); err == nil {
		return "", fmt.Errorf("directory already exists: %s", full)
	}
	if err := os.MkdirAll(full, 0o750); err != nil {
		return "", fmt.Errorf("create directory: %w", err)
	}
	return full, nil
}

// GetBlueprintMetadata returns a lightweight metadata summary as JSON (exposed to frontend).
func (a *App) GetBlueprintMetadata(path string) (string, error) {
	bp, err := blueprint.ParseFile(path)
	if err != nil {
		return "", err
	}
	totalTasks := 0
	for _, s := range bp.Steps {
		totalTasks += len(s.Steps)
	}
	meta := BlueprintMetaSummary{
		Name:         bp.Spec.Name,
		Slug:         bp.Spec.Slug,
		Title:        bp.Spec.Title,
		Description:  bp.Spec.Description,
		Tags:         bp.Spec.Tags,
		Version:      bp.Version,
		InputCount:   len(bp.Inputs),
		StepCount:    totalTasks,
		SectionCount: len(bp.Steps),
		HasImports:   len(bp.Imports) > 0,
	}
	data, _ := json.Marshal(meta)
	return string(data), nil
}

// ParseBlueprintInputs reads a blueprint file and returns its declared inputs (exposed to frontend).
func (a *App) ParseBlueprintInputs(path string) ([]BlueprintInput, error) {
	bp, err := blueprint.ParseFile(path)
	if err != nil {
		return nil, err
	}
	out := make([]BlueprintInput, 0, len(bp.Inputs))
	for _, inp := range bp.Inputs {
		bi := BlueprintInput{
			Name:        inp.Name,
			Label:       inp.Label,
			Description: inp.Description,
			Type:        inp.Type,
			Required:    inp.Required,
		}
		if inp.Default != nil {
			bi.Default = fmt.Sprintf("%v", inp.Default)
		}
		for _, e := range inp.Enum {
			bi.Enum = append(bi.Enum, fmt.Sprintf("%v", e))
		}
		out = append(out, bi)
	}
	return out, nil
}

// GetBlueprintDir returns the configured blueprint directory from settings.json (exposed to frontend).
func (a *App) GetBlueprintDir() string {
	sett, err := settings.Load(a.dataDir)
	if err != nil {
		return settings.DefaultBlueprintDir()
	}
	return sett.BlueprintDir
}

// SetBlueprintDir updates the blueprint_dir in settings.json (exposed to frontend).
func (a *App) SetBlueprintDir(dir string) error {
	sett, err := settings.Load(a.dataDir)
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}
	sett.BlueprintDir = dir
	return sett.Save(a.dataDir)
}

// GetSettings reads settings.json and returns it as JSON (exposed to frontend).
func (a *App) GetSettings() (string, error) {
	sett, err := settings.Load(a.dataDir)
	if err != nil {
		return "", fmt.Errorf("load settings: %w", err)
	}
	data, err := json.MarshalIndent(sett, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal settings: %w", err)
	}
	return string(data), nil
}

// SaveSettings writes JSON settings to settings.json (exposed to frontend).
func (a *App) SaveSettings(jsonStr string) error {
	var sett settings.Settings
	if err := json.Unmarshal([]byte(jsonStr), &sett); err != nil {
		return fmt.Errorf("parse settings: %w", err)
	}
	return sett.Save(a.dataDir)
}

// TelemetryRunSummary represents a single telemetry log file on disk.
type TelemetryRunSummary struct {
	RunID      string `json:"run_id"`
	FileSize   int64  `json:"file_size"`
	ModifiedAt string `json:"modified_at"`
	EventCount int    `json:"event_count"`
}

// TelemetryLogEntry mirrors the JSONL schema.
type TelemetryLogEntry struct {
	TS      string `json:"ts"`
	Level   string `json:"level"`
	Event   string `json:"event"`
	RunID   string `json:"run_id,omitempty"`
	Section string `json:"section,omitempty"`
	Step    string `json:"step,omitempty"`
	Msg     string `json:"msg,omitempty"`
}

// ListTelemetryRuns returns a JSON array of telemetry log file summaries (exposed to frontend).
func (a *App) ListTelemetryRuns() (string, error) {
	logsDir := filepath.Join(a.dataDir, "logs", "runs")
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "[]", nil
		}
		return "", fmt.Errorf("read logs dir: %w", err)
	}

	var summaries []TelemetryRunSummary
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		runID := strings.TrimSuffix(e.Name(), ".jsonl")
		// Count lines (events) by reading file
		data, err := os.ReadFile(filepath.Join(logsDir, e.Name())) // #nosec G304 -- e.Name comes from enumerating logsDir.
		eventCount := 0
		if err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.TrimSpace(line) != "" {
					eventCount++
				}
			}
		}
		summaries = append(summaries, TelemetryRunSummary{
			RunID:      runID,
			FileSize:   info.Size(),
			ModifiedAt: info.ModTime().UTC().Format(time.RFC3339),
			EventCount: eventCount,
		})
	}

	if summaries == nil {
		return "[]", nil
	}

	// Sort newest first
	for i := 0; i < len(summaries); i++ {
		for j := i + 1; j < len(summaries); j++ {
			if summaries[j].ModifiedAt > summaries[i].ModifiedAt {
				summaries[i], summaries[j] = summaries[j], summaries[i]
			}
		}
	}

	data, _ := json.Marshal(summaries)
	return string(data), nil
}

// ReadTelemetryLog reads and returns parsed JSONL entries for a run (exposed to frontend).
func (a *App) ReadTelemetryLog(runID string) (string, error) {
	if err := telemetry.ValidateRunID(runID); err != nil {
		return "", err
	}
	logsDir := filepath.Join(a.dataDir, "logs", "runs")
	path := filepath.Join(logsDir, runID+".jsonl")
	data, err := os.ReadFile(path) // #nosec G304 -- runID validated by telemetry.ValidateRunID; path stays within logsDir.
	if err != nil {
		return "", fmt.Errorf("read log file: %w", err)
	}

	entries := make([]TelemetryLogEntry, 0)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry TelemetryLogEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue // skip malformed lines
		}
		entries = append(entries, entry)
	}

	out, _ := json.Marshal(entries)
	return string(out), nil
}

// DeleteTelemetryLog removes a single telemetry log file (exposed to frontend).
func (a *App) DeleteTelemetryLog(runID string) error {
	if err := telemetry.ValidateRunID(runID); err != nil {
		return err
	}
	logsDir := filepath.Join(a.dataDir, "logs", "runs")
	path := filepath.Join(logsDir, runID+".jsonl")
	return os.Remove(path) // #nosec G304 -- runID validated by telemetry.ValidateRunID; path stays within logsDir.
}

// startDaemon locates hadrond, picks a port, starts it, and polls health.
// If a daemon is already running on port 8095 (e.g. via make run-daemon in dev mode),
// it adopts that instance rather than spawning a new one.
//
// When HADRON_DAEMON_EXTERNAL=true (e.g. set by Cerberus), the GUI never
// spawns its own daemon — it only adopts an existing one on port 8095,
// polling until it appears or timing out after 15 seconds.
func (a *App) startDaemon() {
	preferredAddr := "127.0.0.1:8095"
	external := os.Getenv("HADRON_DAEMON_EXTERNAL") == "true" || os.Getenv("HADRON_DAEMON_EXTERNAL") == "1"

	if external {
		// External mode: wait for an externally managed daemon (Cerberus, manual, etc.)
		if err := waitForHealth(preferredAddr, 15*time.Second); err != nil {
			a.setStatus("error")
			runtime.EventsEmit(a.ctx, "daemon:status", map[string]string{
				"status": "error",
				"error":  "external daemon not found on " + preferredAddr + " (HADRON_DAEMON_EXTERNAL=true)",
			})
			return
		}
		a.mu.Lock()
		a.daemonAddr = preferredAddr
		a.mu.Unlock()
		a.setStatus("running")
		runtime.EventsEmit(a.ctx, "daemon:status", map[string]string{
			"status": "running",
			"addr":   preferredAddr,
		})
		return
	}

	// Default mode: adopt existing daemon or spawn a new one.
	if err := waitForHealth(preferredAddr, 500*time.Millisecond); err == nil {
		a.mu.Lock()
		a.daemonAddr = preferredAddr
		a.mu.Unlock()
		a.setStatus("running")
		runtime.EventsEmit(a.ctx, "daemon:status", map[string]string{
			"status": "running",
			"addr":   preferredAddr,
		})
		return
	}

	bin, err := findHadrond()
	if err != nil {
		a.setStatus("error")
		runtime.EventsEmit(a.ctx, "daemon:status", map[string]string{
			"status": "error",
			"error":  "hadrond not found: " + err.Error(),
		})
		return
	}

	port, err := freePort(8095)
	if err != nil {
		a.setStatus("error")
		runtime.EventsEmit(a.ctx, "daemon:status", map[string]string{
			"status": "error",
			"error":  "no free port: " + err.Error(),
		})
		return
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	home, _ := os.UserHomeDir()
	if home == "" {
		home = "."
	}
	base := filepath.Join(home, ".hadron")
	dbPath := filepath.Join(base, "state", "hadron.db")
	logsDir := filepath.Join(base, "logs", "runs")
	dataDir := base

	// #nosec G204 -- bin is resolved from the bundled executable location or PATH.
	cmd := exec.Command(bin, "serve",
		"-addr", addr,
		"-db", dbPath,
		"-logs", logsDir,
		"-data", dataDir,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		a.setStatus("error")
		runtime.EventsEmit(a.ctx, "daemon:status", map[string]string{
			"status": "error",
			"error":  "start hadrond: " + err.Error(),
		})
		return
	}

	a.mu.Lock()
	a.daemonCmd = cmd
	a.daemonAddr = addr
	a.mu.Unlock()

	// Poll health up to 5s
	if err := waitForHealth(addr, 5*time.Second); err != nil {
		a.setStatus("error")
		runtime.EventsEmit(a.ctx, "daemon:status", map[string]string{
			"status": "error",
			"error":  "health check failed: " + err.Error(),
		})
		return
	}

	a.setStatus("running")
	runtime.EventsEmit(a.ctx, "daemon:status", map[string]string{
		"status": "running",
		"addr":   addr,
	})
}

func (a *App) setStatus(s string) {
	a.mu.Lock()
	a.status = s
	a.mu.Unlock()
}

// findHadrond looks for hadrond next to the current executable, then in PATH.
func findHadrond() (string, error) {
	execPath, err := os.Executable()
	if err == nil {
		candidate := filepath.Join(filepath.Dir(execPath), "hadrond")
		if _, statErr := os.Stat(candidate); statErr == nil {
			return candidate, nil
		}
	}

	path, err := exec.LookPath("hadrond")
	if err == nil {
		return path, nil
	}

	return "", fmt.Errorf("hadrond not found in executable dir or PATH")
}

// freePort tries the preferred port first, then asks the OS for any free port.
func freePort(preferred int) (int, error) {
	addr := fmt.Sprintf("127.0.0.1:%d", preferred)
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		_ = ln.Close()
		return preferred, nil
	}

	ln, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port, nil
}

// waitForHealth polls GET /v1/health until it returns 200 or the timeout expires.
func waitForHealth(addr string, timeout time.Duration) error {
	url := fmt.Sprintf("http://%s/v1/health", addr)
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 500 * time.Millisecond}

	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			closeBody(resp.Body)
			return nil
		}
		if resp != nil {
			closeBody(resp.Body)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for hadrond at %s", addr)
}
