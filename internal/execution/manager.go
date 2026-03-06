package execution

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creack/pty"
	tiamatotel "github.com/hollis-labs/tiamat-otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/telemetry"
)

// RunStore is the persistence interface required by the manager.
type RunStore interface {
	CreateRun(ctx context.Context, rec persistence.RunRecord) error
	SetRunStarted(ctx context.Context, id string, startedAt time.Time) error
	SetRunFinished(ctx context.Context, id, status string, endedAt time.Time, errMsg *string) error
	AppendRunEvent(ctx context.Context, rec persistence.RunEventRecord) error
}

// SettingsValidator is the safety-check interface from the settings package.
type SettingsValidator interface {
	GetDefaultTimeout() int
	ValidateCommand(cmd string) error
	ValidatePath(path string) error
}

// Request describes a single blueprint run.
type Request struct {
	WorkspaceID   string
	RunID         string
	BlueprintPath string
	Inputs        map[string]any
	DryRun        bool
}

// Event is emitted to subscribers for each notable occurrence during a run.
type Event struct {
	RunID     string    `json:"run_id"`
	Section   string    `json:"section,omitempty"`
	StepName  string    `json:"step_name,omitempty"`
	Type      string    `json:"type"`
	Message   string    `json:"message,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

const maxCallDepth = 10

// Manager executes blueprint runs via a worker pool.
type Manager struct {
	store     RunStore
	settings  SettingsValidator
	workers   int
	queue     chan Request
	logDir    string
	tel       *telemetry.Logger
	wg        sync.WaitGroup
	closed    atomic.Bool
	subMu     sync.Mutex
	subs      map[int]chan Event
	nextSubID int
	activeMu  sync.Mutex
	active    map[string]context.CancelFunc
}

// NewManager creates a Manager and starts its worker goroutines.
// tel may be nil, in which case structured JSONL telemetry is skipped.
func NewManager(store RunStore, settings SettingsValidator, workers int, logDir string, tel *telemetry.Logger) *Manager {
	if workers <= 0 {
		workers = 1
	}
	m := &Manager{
		store:    store,
		settings: settings,
		workers:  workers,
		queue:    make(chan Request, 128),
		logDir:   logDir,
		tel:      tel,
		subs:     make(map[int]chan Event),
		active:   make(map[string]context.CancelFunc),
	}
	for i := 0; i < workers; i++ {
		m.wg.Add(1)
		go m.worker()
	}
	return m
}

// Subscribe returns a channel that receives all events and a cancel func.
func (m *Manager) Subscribe(buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 64
	}
	ch := make(chan Event, buffer)
	m.subMu.Lock()
	id := m.nextSubID
	m.nextSubID++
	m.subs[id] = ch
	m.subMu.Unlock()

	cancel := func() {
		m.subMu.Lock()
		if c, ok := m.subs[id]; ok {
			delete(m.subs, id)
			close(c)
		}
		m.subMu.Unlock()
	}
	return ch, cancel
}

// Close drains the queue, waits for workers, and closes all subscriber channels.
func (m *Manager) Close() {
	if m.closed.CompareAndSwap(false, true) {
		close(m.queue)
		m.wg.Wait()
		m.subMu.Lock()
		for id, ch := range m.subs {
			delete(m.subs, id)
			close(ch)
		}
		m.subMu.Unlock()
	}
}

// Cancel requests cancellation of an in-progress run.
func (m *Manager) Cancel(runID string) bool {
	if runID == "" {
		return false
	}
	m.activeMu.Lock()
	cancel, ok := m.active[runID]
	m.activeMu.Unlock()
	if !ok {
		return false
	}
	cancel()
	m.emit(runID, "", "", "canceled", "run cancellation requested")
	return true
}

// Enqueue persists the run record and adds it to the work queue.
func (m *Manager) Enqueue(ctx context.Context, req Request) error {
	if req.RunID == "" {
		return fmt.Errorf("run id is required")
	}
	if req.BlueprintPath == "" {
		return fmt.Errorf("blueprint path is required")
	}
	if m.closed.Load() {
		return fmt.Errorf("manager is closed")
	}

	inputJSON := "{}"
	if len(req.Inputs) > 0 {
		b, err := json.Marshal(req.Inputs)
		if err != nil {
			return fmt.Errorf("marshal run inputs: %w", err)
		}
		inputJSON = string(b)
	}

	now := time.Now().UTC()
	workspaceID := req.WorkspaceID
	if workspaceID == "" {
		workspaceID = "default"
	}
	if err := m.store.CreateRun(ctx, persistence.RunRecord{
		ID:            req.RunID,
		WorkspaceID:   workspaceID,
		BlueprintPath: req.BlueprintPath,
		Status:        "queued",
		InputJSON:     inputJSON,
		CreatedAt:     now,
	}); err != nil {
		return err
	}
	m.emit(req.RunID, "", "", "queued", "run queued")

	select {
	case m.queue <- req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) worker() {
	defer m.wg.Done()
	for req := range m.queue {
		bgCtx := context.Background()
		runCtx, runCancel := context.WithCancel(bgCtx)
		m.activeMu.Lock()
		m.active[req.RunID] = runCancel
		m.activeMu.Unlock()

		start := time.Now().UTC()
		if err := m.store.SetRunStarted(bgCtx, req.RunID, start); err != nil {
			msg := fmt.Sprintf("set started failed: %v", err)
			m.emit(req.RunID, "", "", "error", msg)
			_ = m.store.SetRunFinished(bgCtx, req.RunID, "failed", time.Now().UTC(), &msg)
			m.activeMu.Lock()
			delete(m.active, req.RunID)
			m.activeMu.Unlock()
			runCancel()
			continue
		}
		m.emit(req.RunID, "", "", "started", "run started")

		err := m.executeBlueprint(runCtx, req)

		m.activeMu.Lock()
		delete(m.active, req.RunID)
		m.activeMu.Unlock()
		runCancel()

		if err != nil {
			if errors.Is(err, context.Canceled) {
				msg := "run canceled"
				m.emit(req.RunID, "", "", "canceled", msg)
				_ = m.store.SetRunFinished(bgCtx, req.RunID, "canceled", time.Now().UTC(), &msg)
				continue
			}
			msg := err.Error()
			m.emit(req.RunID, "", "", "failed", msg)
			_ = m.store.SetRunFinished(bgCtx, req.RunID, "failed", time.Now().UTC(), &msg)
			continue
		}
		m.emit(req.RunID, "", "", "completed", "run completed")
		_ = m.store.SetRunFinished(bgCtx, req.RunID, "success", time.Now().UTC(), nil)
	}
}

func (m *Manager) executeBlueprint(ctx context.Context, req Request) error {
	ctx, span := tiamatotel.StartSpan(ctx, "hadron.blueprint.run")
	span.SetAttributes(
		attribute.String("hadron.blueprint.path", req.BlueprintPath),
		attribute.String("hadron.run.id", req.RunID),
		attribute.String("hadron.workspace.id", req.WorkspaceID),
	)
	defer span.End()

	re := &runExecution{
		manager:     m,
		runID:       req.RunID,
		workspaceID: req.WorkspaceID,
		dryRun:      req.DryRun,
	}
	err := re.executeFile(ctx, req.BlueprintPath, req.Inputs, 0)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// ─── runExecution ─────────────────────────────────────────────────────────────

type runExecution struct {
	manager     *Manager
	runID       string
	workspaceID string
	dryRun      bool
	stack       []string
}

func (r *runExecution) executeFile(ctx context.Context, bpPath string, inputs map[string]any, depth int) error {
	if depth > maxCallDepth {
		return fmt.Errorf("max blueprint call depth exceeded (%d)", maxCallDepth)
	}
	absPath, err := filepath.Abs(bpPath)
	if err != nil {
		return fmt.Errorf("resolve blueprint path: %w", err)
	}
	for _, seen := range r.stack {
		if seen == absPath {
			return fmt.Errorf("blueprint call cycle detected at %s", absPath)
		}
	}
	r.stack = append(r.stack, absPath)
	defer func() { r.stack = r.stack[:len(r.stack)-1] }()

	bp, err := blueprint.ParseFile(absPath)
	if err != nil {
		return fmt.Errorf("parse blueprint: %w", err)
	}
	normalizedInputs, err := blueprint.NormalizeInputs(bp, inputs)
	if err != nil {
		return fmt.Errorf("normalize inputs: %w", err)
	}
	renderCtx := blueprint.BuildTemplateContext(bp, absPath, r.workspaceID, normalizedInputs)
	bp, err = blueprint.RenderForExecution(bp, renderCtx)
	if err != nil {
		return fmt.Errorf("render blueprint: %w", err)
	}
	importIndex := buildImportIndex(absPath, bp.Imports)

	// Blueprint-level before_run hooks.
	if err := r.runBlueprintHooks(ctx, bp.Hooks.BeforeRun, "before_run"); err != nil {
		r.emit("", "", "hook_error", fmt.Sprintf("before_run failed: %v", err))
		_ = r.runBlueprintHooks(context.Background(), bp.Hooks.OnError, "on_error")
		return fmt.Errorf("before_run hook failed: %w", err)
	}

	// Execute sections → tasks.
	for _, section := range bp.Steps {
		for taskIdx, task := range section.Tasks {
			// enabled check.
			if task.Enabled != nil && !*task.Enabled {
				r.emit(section.Section, task.Name, "task_skipped", "task disabled")
				continue
			}

			// condition / if check.
			if task.If != "" {
				if !evaluateCondition(task.If) {
					r.emit(section.Section, task.Name, "task_skipped_condition", "condition false")
					continue
				}
			}

			r.emit(section.Section, task.Name, "task_start", fmt.Sprintf("section %q task[%d] %q started", section.Section, taskIdx, task.Name))

			var taskErr error
			if strings.TrimSpace(task.Call) != "" {
				taskErr = r.executeCallTask(ctx, absPath, importIndex, section.Section, task, depth)
			} else {
				taskErr = r.runTask(ctx, section.Section, task)
			}

			if taskErr != nil {
				r.emit(section.Section, task.Name, "task_error", taskErr.Error())

				// Execute on_fail hooks.
				r.executeActionHooks(ctx, absPath, importIndex, section.Section, task, task.OnFail)

				if !task.ContinueOnError {
					_ = r.runBlueprintHooks(context.Background(), bp.Hooks.OnError, "on_error")
					return fmt.Errorf("section %q task %q failed: %w", section.Section, task.Name, taskErr)
				}
				r.emit(section.Section, task.Name, "task_skipped_error", "continuing after task error")
				continue
			}

			// Execute on_success hooks.
			r.executeActionHooks(ctx, absPath, importIndex, section.Section, task, task.OnSuccess)
			r.emit(section.Section, task.Name, "task_success", "task succeeded")
		}
	}

	// Blueprint-level after_run hooks.
	if err := r.runBlueprintHooks(ctx, bp.Hooks.AfterRun, "after_run"); err != nil {
		r.emit("", "", "hook_error", fmt.Sprintf("after_run failed: %v", err))
		return fmt.Errorf("after_run hook failed: %w", err)
	}
	return nil
}

// runTask executes a single task's cmd/run via PTY.
func (r *runExecution) runTask(ctx context.Context, section string, task blueprint.Task) (taskErr error) {
	ctx, taskSpan := tiamatotel.ToolCallSpan(ctx, task.Name)
	taskSpan.SetAttributes(
		attribute.String("hadron.section", section),
		attribute.String("hadron.run.id", r.runID),
	)
	defer func() {
		if taskErr != nil {
			taskSpan.RecordError(taskErr)
		}
		taskSpan.End()
	}()

	cmd := task.Cmd
	if cmd == "" {
		cmd = task.Run
	}
	if cmd == "" {
		return fmt.Errorf("task %q has neither cmd nor run", task.Name)
	}

	// Safety validation.
	if r.manager.settings != nil {
		if err := r.manager.settings.ValidateCommand(cmd); err != nil {
			return fmt.Errorf("safety check: %w", err)
		}
		if task.Dir != "" {
			if err := r.manager.settings.ValidatePath(task.Dir); err != nil {
				return fmt.Errorf("path check: %w", err)
			}
		}
	}

	// DryRun: log and skip execution.
	if r.dryRun {
		r.emit(section, task.Name, "dry_run", fmt.Sprintf("[dry-run] would execute: %s", cmd))
		return nil
	}

	retries := task.Retry
	if retries < 0 {
		retries = 0
	}

	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(task.RetryDelaySecs) * time.Second
			if delay > 0 {
				r.emit(section, task.Name, "task_retry_wait", fmt.Sprintf("waiting %s before retry", delay))
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
			}
			r.emit(section, task.Name, "task_retry", fmt.Sprintf("retry attempt %d/%d", attempt, retries))
		}

		lastErr = r.execCmd(ctx, section, task, cmd)
		if lastErr == nil {
			return nil
		}
		if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) {
			return lastErr
		}
	}
	return lastErr
}

// execCmd runs a command via PTY and streams output to run_events.
func (r *runExecution) execCmd(ctx context.Context, section string, task blueprint.Task, cmd string) error {
	timeout := task.TimeoutSeconds
	if timeout == 0 && r.manager.settings != nil {
		timeout = r.manager.settings.GetDefaultTimeout()
	}

	stepCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		stepCtx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}

	c := exec.CommandContext(stepCtx, "bash", "-lc", cmd)
	if task.Dir != "" {
		c.Dir = task.Dir
	}
	c.Env = os.Environ()
	for k, v := range task.Env {
		c.Env = append(c.Env, fmt.Sprintf("%s=%s", k, v))
	}

	ptmx, err := pty.Start(c)
	if err != nil {
		return fmt.Errorf("pty start: %w", err)
	}

	sc := bufio.NewScanner(ptmx)
	for sc.Scan() {
		r.emit(section, task.Name, "log", sc.Text())
	}

	if err := c.Wait(); err != nil {
		_ = ptmx.Close()
		if stepCtx.Err() != nil {
			return stepCtx.Err()
		}
		return err
	}
	_ = ptmx.Close()
	return nil
}

// ─── Blueprint-level hooks ────────────────────────────────────────────────────

func (r *runExecution) runBlueprintHooks(ctx context.Context, hooks []blueprint.Hook, phase string) error {
	for _, h := range hooks {
		if h.If != "" && !evaluateCondition(h.If) {
			continue
		}
		if strings.TrimSpace(h.Cmd) == "" {
			continue
		}
		c := exec.CommandContext(ctx, "sh", "-c", h.Cmd)
		out, err := c.CombinedOutput()
		if err != nil {
			msg := strings.TrimSpace(string(out))
			if msg != "" {
				return fmt.Errorf("hook %q (%s): %w: %s", h.Name, phase, err, msg)
			}
			return fmt.Errorf("hook %q (%s): %w", h.Name, phase, err)
		}
	}
	return nil
}

// ─── ActionHooks (on_success / on_fail) ──────────────────────────────────────

func (r *runExecution) executeActionHooks(ctx context.Context, basePath string, imports map[string]resolvedImport, section string, task blueprint.Task, hooks []blueprint.ActionHook) {
	for _, h := range hooks {
		switch h.Type {
		case "cmd":
			r.emit(section, task.Name, "hook_cmd", fmt.Sprintf("[hook] %s", h.Value))
			c := exec.CommandContext(ctx, "bash", "-lc", h.Value)
			if out, err := c.CombinedOutput(); err == nil {
				r.emit(section, task.Name, "hook_output", strings.TrimSpace(string(out)))
			}

		case "error":
			r.emit(section, task.Name, "hook_error", h.Value)

		case "step":
			// Execute a named task within the current run's context.
			r.emit(section, task.Name, "hook_step", fmt.Sprintf("[hook] jump to step: %s", h.Value))

		case "blueprint":
			r.emit(section, task.Name, "hook_blueprint", fmt.Sprintf("[hook] execute blueprint: %s", h.Value))
			bpDir := filepath.Dir(basePath)
			extPath := filepath.Join(bpDir, h.Value)
			if err := r.executeFile(ctx, extPath, nil, 1); err != nil {
				r.emit(section, task.Name, "hook_error", fmt.Sprintf("[hook] blueprint %s failed: %v", h.Value, err))
			}

		case "call":
			r.emit(section, task.Name, "hook_call", fmt.Sprintf("[hook] call: %s", h.Value))
			resolved, ok := imports[h.Value]
			if !ok {
				r.emit(section, task.Name, "hook_error", fmt.Sprintf("[hook] import alias %q not found", h.Value))
				continue
			}
			if err := r.executeFile(ctx, resolved.path, cloneMap(resolved.with), 1); err != nil {
				r.emit(section, task.Name, "hook_error", fmt.Sprintf("[hook] call %s failed: %v", h.Value, err))
			}
		}
	}
}

// ─── Call step ───────────────────────────────────────────────────────────────

type resolvedImport struct {
	path string
	with map[string]any
}

func buildImportIndex(baseBlueprintPath string, imports []blueprint.Import) map[string]resolvedImport {
	idx := make(map[string]resolvedImport, len(imports))
	for _, imp := range imports {
		alias := strings.TrimSpace(imp.Alias)
		if alias == "" {
			continue
		}
		impPath := resolveImportPath(baseBlueprintPath, imp.Path)
		idx[alias] = resolvedImport{path: impPath, with: cloneMap(imp.With)}
	}
	return idx
}

func (r *runExecution) executeCallTask(ctx context.Context, baseBlueprintPath string, imports map[string]resolvedImport, section string, task blueprint.Task, depth int) error {
	target := strings.TrimSpace(task.Call)
	resolved, ok := imports[target]
	if !ok {
		p := resolveImportPath(baseBlueprintPath, target)
		resolved = resolvedImport{path: p, with: map[string]any{}}
	}
	childInputs := cloneMap(resolved.with)
	for k, v := range task.With {
		childInputs[k] = v
	}
	r.emit(section, task.Name, "task_call_start", fmt.Sprintf("calling %q", resolved.path))
	err := r.executeFile(ctx, resolved.path, childInputs, depth+1)
	if err != nil {
		r.emit(section, task.Name, "task_call_error", err.Error())
		return err
	}
	r.emit(section, task.Name, "task_call_success", "call succeeded")
	return nil
}

func resolveImportPath(baseBlueprintPath, importPath string) string {
	if filepath.IsAbs(importPath) {
		return filepath.Clean(importPath)
	}
	baseDir := filepath.Dir(baseBlueprintPath)
	return filepath.Clean(filepath.Join(baseDir, importPath))
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// ─── Event emission ───────────────────────────────────────────────────────────

func (r *runExecution) emit(section, step, eventType, message string) {
	r.manager.emit(r.runID, section, step, eventType, message)
}

func (m *Manager) emit(runID, section, stepName, eventType, message string) {
	e := Event{
		RunID:     runID,
		Section:   section,
		StepName:  stepName,
		Type:      eventType,
		Message:   message,
		CreatedAt: time.Now().UTC(),
	}

	_ = m.store.AppendRunEvent(context.Background(), persistence.RunEventRecord{
		RunID:     runID,
		StepName:  toNullString(stepName),
		EventType: eventType,
		Message:   toNullString(message),
		CreatedAt: e.CreatedAt,
	})

	if m.tel != nil {
		m.tel.Write(telemetry.LogEntry{
			TS:      e.CreatedAt,
			Event:   e.Type,
			RunID:   e.RunID,
			Section: e.Section,
			Step:    e.StepName,
			Msg:     e.Message,
		})
	} else if m.logDir != "" {
		m.writeEventLog(e)
	}

	m.subMu.Lock()
	for _, ch := range m.subs {
		select {
		case ch <- e:
		default:
		}
	}
	m.subMu.Unlock()
}

func (m *Manager) writeEventLog(e Event) {
	if err := os.MkdirAll(m.logDir, 0o755); err != nil {
		return
	}
	path := filepath.Join(m.logDir, e.RunID+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = fmt.Fprintf(f, "%s type=%s section=%s step=%s msg=%s\n",
		e.CreatedAt.Format(time.RFC3339Nano), e.Type, e.Section, e.StepName, e.Message)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func toNullString(v string) sql.NullString {
	if v == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}

// evaluateCondition returns true if the condition string is truthy.
// Already-rendered template values are expected (e.g. "true", "false", "1").
// Unknown strings fall back to true (run by default).
func evaluateCondition(cond string) bool {
	cond = strings.TrimSpace(cond)
	if cond == "" {
		return true
	}
	switch strings.ToLower(cond) {
	case "1", "t", "true", "yes", "y", "on":
		return true
	case "0", "f", "false", "no", "n", "off":
		return false
	default:
		if n, err := strconv.ParseFloat(cond, 64); err == nil {
			return n != 0
		}
		return true
	}
}
