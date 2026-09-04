package appworkflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
	"github.com/hollis-labs/go-workflow/verification"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

const defaultRecoveryInterval = 5 * time.Second

var initialKindRefs = []KindRef{
	{Name: "call", Version: "v1"}, {Name: "cmd", Version: "v1"},
	{Name: "http", Version: "v1"}, {Name: "human_gate", Version: "v1"},
	{Name: "mcp", Version: "v1"}, {Name: "message_wait", Version: "v1"},
	{Name: "script", Version: "v1"}, {Name: "sleep", Version: "v1"},
	{Name: "transform", Version: "v1"}, {Name: "wait_for", Version: "v1"},
}

// InitialKinds returns the exact Wave 04 adapter identities required by the
// default host profile. The returned slice is a defensive copy.
func InitialKinds() []KindRef { return sortedKindRefs(initialKindRefs) }

// Host is Hadron's graph-native application service and daemon-lifecycle
// boundary. Durable state remains authoritative; in-memory fields contain only
// health, shutdown, and immutable collaborator references.
type Host struct {
	state              runtime.StateStore
	journal            hoststate.Journal
	definitions        DefinitionProvider
	identity           IdentityProvider
	policy             PolicyEvaluator
	dryRun             DryRunSupport
	activations        workflowwait.ActivationScheduler
	reactorActivations hoststate.ActivationStore
	reactors           runtime.ReactorStore
	failureHooks       hoststate.FailureHookJournal
	failureHandler     *FailureHandlerConfig
	waits              *runtime.WaitCoordinator
	cancellation       *runtime.CancellationCoordinator
	coreRecovery       RecoveryHook
	hooks              []RecoveryHook
	childSource        ChildRunRecoverySource
	childDefs          ChildRunDefinitionSource
	childRuns          ChildRunMaterializer
	telemetry          TelemetrySink
	artifacts          values.ArtifactStore
	clock              Clock
	registry           *stepkind.MemoryRegistry
	verifiers          *verification.MemoryRegistry
	dispatcher         *runtime.StepDispatcher
	plans              runtime.RecoveryPlanSource
	pins               *runtime.PinCoordinator
	interval           time.Duration
	batchLimit         int

	mu     sync.RWMutex
	health HealthStatus
	cancel context.CancelFunc
	done   chan struct{}
}

func New(options Options) (*Host, error) {
	if nilInterface(options.State) || nilInterface(options.Journal) || nilInterface(options.Definitions) ||
		nilInterface(options.Identity) || nilInterface(options.Policy) || nilInterface(options.Activations) ||
		nilInterface(options.Artifacts) {
		return nil, fmt.Errorf("%w: state, journal, definitions, identity, policy, activations, and artifacts are required", ErrInvalidHost)
	}
	clock := options.Clock
	if nilInterface(clock) {
		clock = ClockFunc(func() time.Time { return time.Now().UTC() })
	}
	interval := options.RecoveryInterval
	if interval == 0 {
		interval = defaultRecoveryInterval
	}
	if interval < 0 || options.RecoveryBatchLimit < 0 {
		return nil, fmt.Errorf("%w: recovery interval and batch limit must not be negative", ErrInvalidHost)
	}
	for index, hook := range options.RecoveryHooks {
		if nilInterface(hook) {
			return nil, fmt.Errorf("%w: recovery hook[%d] is nil", ErrInvalidHost, index)
		}
	}
	if options.RecoveryRepeatPolicy != nil && nilInterface(options.RecoveryRepeatPolicy) {
		return nil, fmt.Errorf("%w: recovery repeat policy must not be typed nil", ErrInvalidHost)
	}
	if options.RecoveryRetryAuthorizer != nil && nilInterface(options.RecoveryRetryAuthorizer) {
		return nil, fmt.Errorf("%w: recovery retry authorizer must not be typed nil", ErrInvalidHost)
	}
	if options.ReuseAuthorizer != nil && nilInterface(options.ReuseAuthorizer) {
		return nil, fmt.Errorf("%w: reuse authorizer must not be typed nil", ErrInvalidHost)
	}
	if options.ActivationStore != nil && nilInterface(options.ActivationStore) {
		return nil, fmt.Errorf("%w: activation registration store must not be typed nil", ErrInvalidHost)
	}
	if options.OnRunFailed != nil {
		if err := validateFailureHandlerDefinition(options.OnRunFailed.Definition); err != nil || options.OnRunFailed.MaximumDepth < 1 || options.OnRunFailed.MaximumDepth > 16 {
			return nil, fmt.Errorf("%w: on_run_failed requires an exact definition and maximum depth between 1 and 16", ErrInvalidHost)
		}
	}
	childSource, hasChildSource := options.Journal.(ChildRunRecoverySource)
	childDefs, _ := options.Journal.(ChildRunDefinitionSource)
	if hasChildSource && nilInterface(options.ChildRuns) {
		return nil, fmt.Errorf("%w: child-run materializer is required for the SQLite call recovery source", ErrInvalidHost)
	}
	registry := stepkind.NewRegistry()
	for _, kind := range options.Kinds {
		if err := registry.Register(kind); err != nil {
			return nil, fmt.Errorf("%w: register step kind: %w", ErrInvalidHost, err)
		}
	}
	required := options.RequiredKinds
	if required == nil {
		required = InitialKinds()
	}
	if err := requireKinds(registry, required); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidHost, err)
	}
	verifiers, err := freezeHostVerifiers(options.Definitions, options.Verifiers)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidHost, err)
	}
	waits := options.Waits
	if waits == nil {
		if waitStore, ok := options.State.(runtime.WaitStore); ok {
			waits = &runtime.WaitCoordinator{Store: waitStore, Scheduler: options.Activations}
		}
	}
	dispatcher, err := runtime.NewStepDispatcher(runtime.DispatcherOptions{
		Store: options.State, Registry: registry, Now: clock.Now, WaitCoordinator: waits, Verifiers: verifiers,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: construct dispatcher: %w", ErrInvalidHost, err)
	}
	cancellation := options.Cancellation
	if cancellation == nil {
		cancellation = &runtime.CancellationCoordinator{Store: options.State, Registry: registry, Now: clock.Now}
	} else {
		copyCoordinator := *cancellation
		copyCoordinator.Store, copyCoordinator.Registry = options.State, registry
		cancellation = &copyCoordinator
	}
	recoveryStore, recoveryOK := options.State.(runtime.RecoveryStore)
	inputStore, inputOK := options.State.(runtime.NodeInputStore)
	controlStore, controlOK := options.State.(runtime.ControlFlowStore)
	policyStore, policyOK := options.State.(runtime.RunPolicyStore)
	reactorStore, reactorOK := options.State.(runtime.ReactorStore)
	if !recoveryOK || nilInterface(recoveryStore) || !inputOK || nilInterface(inputStore) ||
		!controlOK || nilInterface(controlStore) || !policyOK || nilInterface(policyStore) {
		return nil, fmt.Errorf("%w: state must provide recovery, input-binding, control-flow, and run-policy stores", ErrInvalidHost)
	}
	dependencyOptions := compileDependencyOptions(options.Definitions)
	planSource := PinnedRecoveryPlanSource{Roots: options.Journal, Children: childDefs, State: options.State,
		Replays: recoveryStore, DependencyOptions: dependencyOptions}
	var pinCoordinator *runtime.PinCoordinator
	if options.ReuseAuthorizer != nil {
		pinStore, pinsOK := options.State.(runtime.PinStore)
		valueStore, valuesOK := options.State.(runtime.ValueRecordStore)
		if !pinsOK || nilInterface(pinStore) || !valuesOK || nilInterface(valueStore) {
			return nil, fmt.Errorf("%w: pin-enabled state must provide pin and value-record stores", ErrInvalidHost)
		}
		pinCoordinator = &runtime.PinCoordinator{Store: options.State, Pins: pinStore, Values: valueStore, Plans: planSource, Registry: registry, Authorizer: options.ReuseAuthorizer}
	}
	coreRecovery := CoreRecoveryHook{Coordinator: &runtime.RecoveryCoordinator{
		Store: options.State, Recovery: recoveryStore, Inputs: inputStore, Control: controlStore,
		Plans:    planSource,
		Registry: registry, Policy: options.RecoveryRepeatPolicy, RetryAuthorizer: options.RecoveryRetryAuthorizer,
		Policies: policyStore, Waits: waits,
	}, Limit: options.RecoveryBatchLimit}
	host := &Host{
		state: options.State, journal: options.Journal, definitions: options.Definitions,
		identity: options.Identity, policy: options.Policy, dryRun: options.DryRun,
		activations: options.Activations, reactorActivations: options.ActivationStore, waits: waits, cancellation: cancellation, coreRecovery: coreRecovery,
		hooks: append([]RecoveryHook(nil), options.RecoveryHooks...), telemetry: options.Telemetry,
		childSource: childSource, childDefs: childDefs, childRuns: options.ChildRuns,
		artifacts: options.Artifacts, clock: clock, registry: registry, verifiers: verifiers, dispatcher: dispatcher, pins: pinCoordinator,
		plans:    planSource,
		interval: interval, batchLimit: options.RecoveryBatchLimit,
	}
	if hooks, ok := options.Journal.(hoststate.FailureHookJournal); ok && !nilInterface(hooks) {
		host.failureHooks = hooks
	}
	if options.OnRunFailed != nil {
		if host.failureHooks == nil {
			return nil, fmt.Errorf("%w: on_run_failed requires durable failure-hook journal support", ErrInvalidHost)
		}
		config := *options.OnRunFailed
		host.failureHandler = &config
	}
	if options.ActivationStore != nil {
		if !reactorOK || nilInterface(reactorStore) {
			return nil, fmt.Errorf("%w: activation registration recovery requires reactor state support", ErrInvalidHost)
		}
		host.reactors = reactorStore
	}
	return host, nil
}

func validateFailureHandlerDefinition(ref graph.DefinitionRef) error {
	for _, field := range []string{ref.Authority, ref.Kind, ref.ID, ref.Version} {
		if hoststate.ValidatePublicText(field, hoststate.MaximumActivationTextBytes, true) != nil {
			return errors.New("failure handler definition reference is incomplete or unsafe")
		}
	}
	if ref.Locator != "" && hoststate.ValidatePublicText(ref.Locator, hoststate.MaximumActivationTextBytes, false) != nil {
		return errors.New("failure handler definition locator is unsafe")
	}
	if values.ValidateDigest(ref.Digest) != nil {
		return errors.New("failure handler definition digest is invalid")
	}
	return nil
}

type definitionVerifierCatalog interface {
	Verifiers() verification.Registry
}

func freezeHostVerifiers(definitions DefinitionProvider, supplied verification.Registry) (*verification.MemoryRegistry, error) {
	if supplied != nil && nilInterface(supplied) {
		return nil, errors.New("verification registry must not be typed nil")
	}
	var suppliedSnapshot *verification.MemoryRegistry
	var err error
	if supplied != nil {
		suppliedSnapshot, err = verification.SnapshotRegistry(supplied)
		if err != nil {
			return nil, fmt.Errorf("snapshot supplied verification registry: %w", err)
		}
	}
	if provider, ok := definitions.(definitionVerifierCatalog); ok {
		resolved := provider.Verifiers()
		if nilInterface(resolved) {
			return nil, errors.New("definition provider returned a nil verification registry")
		}
		definitionSnapshot, snapshotErr := verification.SnapshotRegistry(resolved)
		if snapshotErr != nil {
			return nil, fmt.Errorf("snapshot definition verification registry: %w", snapshotErr)
		}
		if suppliedSnapshot != nil && !reflect.DeepEqual(suppliedSnapshot.List(), definitionSnapshot.List()) {
			return nil, errors.New("host and definition verifier catalogs differ")
		}
		return definitionSnapshot, nil
	}
	if suppliedSnapshot != nil {
		return suppliedSnapshot, nil
	}
	return verification.SnapshotRegistry(verification.NewDefaultRegistry())
}

func requireKinds(registry *stepkind.MemoryRegistry, required []KindRef) error {
	seen := make(map[KindRef]struct{}, len(required))
	for _, ref := range required {
		if ref.Name == "" || ref.Version == "" {
			return errors.New("required step kind identity is incomplete")
		}
		if _, duplicate := seen[ref]; duplicate {
			return fmt.Errorf("required step kind %s@%s is duplicated", ref.Name, ref.Version)
		}
		seen[ref] = struct{}{}
		if _, ok := registry.Lookup(ref.Name, ref.Version); !ok {
			return fmt.Errorf("required step kind %s@%s is not registered", ref.Name, ref.Version)
		}
	}
	return nil
}

// Registry returns the immutable-after-construction registry as its read-only
// interface. Callers must not assume Host accepts late registrations.
func (h *Host) Registry() stepkind.Registry {
	if h == nil {
		return nil
	}
	return h.registry
}

// Verifiers returns the frozen read-only catalog used by start validation and
// dispatch. Worker composition can pass this exact catalog to
// runtime.ExternalOperationOptions without constructing an unused coordinator
// inside Host.
func (h *Host) Verifiers() verification.Registry {
	if h == nil {
		return nil
	}
	return h.verifiers
}

// Dispatcher exposes the core dispatcher already bound to the host registry
// and state adapter for Hadron-owned worker composition.
func (h *Host) Dispatcher() *runtime.StepDispatcher {
	if h == nil {
		return nil
	}
	return h.dispatcher
}

func (h *Host) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidHost)
	}
	h.mu.Lock()
	if h.health.Started {
		ready := h.health.Ready && !h.health.Recovering
		h.mu.Unlock()
		if ready {
			return nil
		}
		return ErrHostNotReady
	}
	recoveryCtx, cancel := context.WithCancel(context.Background())
	h.cancel, h.done = cancel, make(chan struct{})
	h.health.Started, h.health.Recovering = true, true
	h.mu.Unlock()
	startupCtx, cancelStartup := context.WithCancel(recoveryCtx)
	stopCallerCancellation := context.AfterFunc(ctx, cancelStartup)
	recoveryErr := h.recover(startupCtx, true)
	stopCallerCancellation()
	cancelStartup()
	if recoveryErr != nil {
		cancel()
		h.mu.Lock()
		h.health.Started, h.health.Ready, h.health.Recovering = false, false, false
		h.health.LastRecoveryError = recoveryErr.Error()
		close(h.done)
		h.cancel, h.done = nil, nil
		h.mu.Unlock()
		return recoveryErr
	}
	h.mu.Lock()
	if recoveryCtx.Err() != nil {
		recoveryErr = recoveryCtx.Err()
		h.health.Started, h.health.Ready, h.health.Recovering = false, false, false
		h.health.LastRecoveryError = recoveryErr.Error()
		close(h.done)
		h.cancel, h.done = nil, nil
		h.mu.Unlock()
		return recoveryErr
	}
	go h.recoveryLoop(recoveryCtx)
	h.health.Ready, h.health.Recovering = true, false
	h.health.LastRecoveryError = ""
	h.mu.Unlock()
	return nil
}

func (h *Host) recoveryLoop(ctx context.Context) {
	defer close(h.done)
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recoveryErr := h.recover(ctx, false)
			h.mu.Lock()
			if ctx.Err() != nil {
				h.health.Ready = false
				h.mu.Unlock()
				return
			}
			if recoveryErr != nil {
				h.health.LastRecoveryError, h.health.Ready = recoveryErr.Error(), false
			} else {
				h.health.LastRecoveryError, h.health.Ready = "", true
			}
			h.mu.Unlock()
		}
	}
}

func (h *Host) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidHost)
	}
	h.mu.Lock()
	if !h.health.Started {
		h.mu.Unlock()
		return nil
	}
	cancel, done := h.cancel, h.done
	h.health.Ready = false
	cancel()
	h.mu.Unlock()
	select {
	case <-done:
		h.mu.Lock()
		h.health.Started, h.health.Ready, h.health.Recovering = false, false, false
		h.cancel, h.done = nil, nil
		h.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Host) Health() HealthStatus {
	if h == nil {
		return HealthStatus{}
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.health
}

func (h *Host) requireReady() error {
	if h == nil {
		return ErrInvalidHost
	}
	h.mu.RLock()
	ready := h.health.Started && h.health.Ready && !h.health.Recovering
	h.mu.RUnlock()
	if !ready {
		return ErrHostNotReady
	}
	return nil
}

func (h *Host) recover(ctx context.Context, startup bool) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidHost)
	}
	now := h.now()
	if startup {
		h.mu.Lock()
		h.health.Recovering = true
		h.mu.Unlock()
	}
	defer func() {
		h.mu.Lock()
		if startup {
			h.health.Recovering = false
		}
		h.health.LastRecoveryAt = now
		h.mu.Unlock()
	}()
	for {
		starts, err := h.journal.ListIncompleteStarts(ctx, h.batchLimit)
		if err != nil {
			return fmt.Errorf("recover workflow starts: %w", err)
		}
		h.mu.Lock()
		h.health.IncompleteStarts = len(starts)
		h.mu.Unlock()
		if len(starts) == 0 {
			break
		}
		for _, start := range starts {
			if _, err := h.materializeStart(ctx, start); err != nil {
				return fmt.Errorf("recover run %s: %w", start.Record.Run.ID, err)
			}
		}
	}
	if h.childSource != nil {
		seen := make(map[string]struct{})
		for {
			children, err := h.childSource.RecoverPendingChildRuns(ctx, h.batchLimit)
			if err != nil {
				return fmt.Errorf("recover pending child runs: %w", err)
			}
			if len(children) == 0 {
				break
			}
			for _, child := range children {
				identity := string(child.ChildRunID) + "\x00" + child.IdempotencyKey
				if _, duplicate := seen[identity]; duplicate {
					return fmt.Errorf("materialize child run %s: recovery made no durable progress", child.ChildRunID)
				}
				seen[identity] = struct{}{}
				if err := h.childRuns.MaterializeChildRun(ctx, child); err != nil {
					return fmt.Errorf("materialize child run %s: %w", child.ChildRunID, err)
				}
			}
		}
	}
	// A durable child start can exist without materialized node invocations
	// after a crash. Materialize those exact pinned starts before planning a
	// cancellation tree so descendant finalizers are never skipped or left in
	// an unclosable pending run.
	for {
		cancellations, err := h.journal.ListPendingCancellations(ctx, h.batchLimit)
		if err != nil {
			return fmt.Errorf("recover host cancellations: %w", err)
		}
		if len(cancellations) == 0 {
			break
		}
		for _, cancellation := range cancellations {
			if _, _, err := h.applyCancellation(ctx, cancellation); err != nil {
				return fmt.Errorf("recover host cancellation %s: %w", cancellation.Intent.IdempotencyKey, err)
			}
		}
	}
	if h.cancellation != nil {
		if _, failures, err := h.cancellation.Recover(ctx, runtime.CancellationIntentQuery{Limit: h.batchLimit}); err != nil {
			return fmt.Errorf("recover cancellation: %w", err)
		} else if len(failures) != 0 {
			return fmt.Errorf("recover cancellation intents: %w", errors.Join(failures...))
		}
	}
	if err := h.recoverRunUpdates(ctx); err != nil {
		return fmt.Errorf("recover tracked workflow updates: %w", err)
	}
	// Core recovery is installed for every Host and runs after exact child and
	// cancellation restoration but before extension hooks can observe or admit
	// ordinary work.
	if nilInterface(h.coreRecovery) {
		return fmt.Errorf("recover workflow core: %w", ErrInvalidHost)
	}
	if err := h.coreRecovery.RecoverWorkflow(ctx, runtime.RecoverySnapshot{}, now); err != nil {
		return fmt.Errorf("recover workflow core: %w", err)
	}
	if err := h.recoverChildTerminalWaits(ctx); err != nil {
		return fmt.Errorf("recover child terminal waits: %w", err)
	}
	if h.reactors != nil {
		service := ReactorService{Host: h, Activations: h.reactorActivations, Store: h.reactors, Clock: h.clock}
		if err := service.Recover(ctx, h.batchLimit); err != nil {
			return fmt.Errorf("recover workflow reactors: %w", err)
		}
	}
	if err := h.recoverFailureHooks(ctx); err != nil {
		return fmt.Errorf("recover workflow failure hooks: %w", err)
	}
	snapshot, err := h.state.Recovery(ctx, runtime.RecoveryQuery{Now: now, Limit: h.batchLimit})
	if err != nil {
		return fmt.Errorf("recover runtime state: %w", err)
	}
	for _, hook := range h.hooks {
		if err := hook.RecoverWorkflow(ctx, snapshot, now); err != nil {
			return fmt.Errorf("recover host worker: %w", err)
		}
	}
	h.mu.Lock()
	h.health.IncompleteStarts = 0
	h.mu.Unlock()
	return nil
}

func (h *Host) now() time.Time { return h.clock.Now().UTC() }

func (h *Host) observe(runID runtime.RunID, event string, attributes map[string]string) {
	if nilInterface(h.telemetry) {
		return
	}
	h.telemetry.ObserveWorkflow(context.Background(), TelemetryObservation{RunID: runID, Event: event, OccurredAt: h.now(), Attributes: cloneStringMap(attributes)})
}

func (h *Host) Schedule(ctx context.Context, activation workflowwait.Activation) error {
	if h == nil || nilInterface(h.activations) {
		return ErrInvalidHost
	}
	if err := activation.Validate(); err != nil {
		return err
	}
	return h.activations.Schedule(ctx, activation)
}

func (h *Host) Cancel(ctx context.Context, id workflowwait.ActivationID) error {
	if h == nil || nilInterface(h.activations) {
		return ErrInvalidHost
	}
	return h.activations.Cancel(ctx, id)
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func sortedKindRefs(input []KindRef) []KindRef {
	result := append([]KindRef(nil), input...)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name == result[j].Name {
			return result[i].Version < result[j].Version
		}
		return result[i].Name < result[j].Name
	})
	return result
}

var _ workflowwait.ActivationScheduler = (*Host)(nil)
