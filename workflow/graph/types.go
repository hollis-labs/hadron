package graph

// Duration is a source-preserving duration such as "30s" or "24h". Runtime
// packages are responsible for parsing it at the appropriate binding boundary.
type Duration string

// Schema carries an inline JSON Schema. A reference-only schema uses the
// standard "$ref" key. The graph package does not interpret schema keywords.
type Schema map[string]any

// Config is opaque step-kind or host-extension configuration. Its schema and
// semantics belong to the registered adapter or extension implementation.
type Config map[string]any

// Metadata contains application-neutral descriptive data that does not change
// graph execution semantics.
type Metadata map[string]any

// Graph is the canonical in-memory workflow graph shared by all engine
// frontends and hosts.
type Graph struct {
	ID          string                      `json:"id" yaml:"id"`
	Namespace   string                      `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Version     string                      `json:"version" yaml:"version"`
	Digest      string                      `json:"digest" yaml:"digest"`
	Provenance  Provenance                  `json:"provenance,omitempty" yaml:"provenance,omitempty"`
	Inputs      []InputSpec                 `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Outputs     []OutputSpec                `json:"outputs,omitempty" yaml:"outputs,omitempty"`
	Nodes       []Node                      `json:"nodes" yaml:"nodes"`
	Edges       []Edge                      `json:"edges,omitempty" yaml:"edges,omitempty"`
	Activations []ActivationDeclaration     `json:"activations,omitempty" yaml:"activations,omitempty"`
	Source      *SourceRef                  `json:"source,omitempty" yaml:"source,omitempty"`
	SourceMap   SourceMap                   `json:"source_map,omitempty" yaml:"source_map,omitempty"`
	Concurrency ConcurrencySpec             `json:"concurrency,omitempty" yaml:"concurrency,omitempty"`
	Completion  *RunCompletionPolicy        `json:"completion,omitempty" yaml:"completion,omitempty"`
	Durability  *DurabilitySpec             `json:"durability,omitempty" yaml:"durability,omitempty"`
	Policy      []PolicyRequirement         `json:"policy_requirements,omitempty" yaml:"policy_requirements,omitempty"`
	Target      ExecutionTargetRequirements `json:"execution_target,omitempty" yaml:"execution_target,omitempty"`
	Extensions  map[string]Extension        `json:"extensions,omitempty" yaml:"extensions,omitempty"`
	Metadata    Metadata                    `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// InputSpec declares a named, schema-bearing workflow input.
type InputSpec struct {
	Name        string     `json:"name" yaml:"name"`
	Description string     `json:"description,omitempty" yaml:"description,omitempty"`
	Schema      Schema     `json:"schema" yaml:"schema"`
	Required    bool       `json:"required,omitempty" yaml:"required,omitempty"`
	Default     *Binding   `json:"default,omitempty" yaml:"default,omitempty"`
	Source      *SourceRef `json:"source,omitempty" yaml:"source,omitempty"`
	Metadata    Metadata   `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// OutputSpec declares a named, schema-bearing node or workflow output. Workflow
// outputs require Value. Node outputs omit Value for exact same-name adapter
// passthrough or set it to project the registered kind's raw outputs root.
type OutputSpec struct {
	Name        string     `json:"name" yaml:"name"`
	Description string     `json:"description,omitempty" yaml:"description,omitempty"`
	Schema      Schema     `json:"schema" yaml:"schema"`
	Value       *Binding   `json:"value,omitempty" yaml:"value,omitempty"`
	Source      *SourceRef `json:"source,omitempty" yaml:"source,omitempty"`
	Metadata    Metadata   `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// Node is one executable or control-flow unit in a workflow graph.
type Node struct {
	ID            string                      `json:"id" yaml:"id"`
	DisplayName   string                      `json:"display_name,omitempty" yaml:"display_name,omitempty"`
	Kind          string                      `json:"kind" yaml:"kind"`
	KindVersion   string                      `json:"kind_version,omitempty" yaml:"kind_version,omitempty"`
	Needs         []Need                      `json:"needs,omitempty" yaml:"needs,omitempty"`
	ReadyWhen     ReadyRule                   `json:"ready_when,omitempty" yaml:"ready_when,omitempty"`
	If            *Expression                 `json:"if,omitempty" yaml:"if,omitempty"`
	ForEach       *ForEachSpec                `json:"for_each,omitempty" yaml:"for_each,omitempty"`
	Config        Config                      `json:"config,omitempty" yaml:"config,omitempty"`
	InputBindings map[string]Binding          `json:"with,omitempty" yaml:"with,omitempty"`
	Outputs       []OutputSpec                `json:"outputs,omitempty" yaml:"outputs,omitempty"`
	Effects       EffectSet                   `json:"effects,omitempty" yaml:"effects,omitempty"`
	Retry         *RetryPolicy                `json:"retry,omitempty" yaml:"retry,omitempty"`
	Idempotency   *IdempotencySpec            `json:"idempotency,omitempty" yaml:"idempotency,omitempty"`
	Timeout       *TimeoutPolicy              `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Catch         []CatchRule                 `json:"catch,omitempty" yaml:"catch,omitempty"`
	Finally       *FinallySpec                `json:"finally,omitempty" yaml:"finally,omitempty"`
	Switch        *SwitchSpec                 `json:"switch,omitempty" yaml:"switch,omitempty"`
	Call          *CallSpec                   `json:"call,omitempty" yaml:"call,omitempty"`
	Concurrency   []ConcurrencyClaim          `json:"concurrency,omitempty" yaml:"concurrency,omitempty"`
	Verification  *VerificationSpec           `json:"verify,omitempty" yaml:"verify,omitempty"`
	Memoization   *MemoizationSpec            `json:"memoize,omitempty" yaml:"memoize,omitempty"`
	Durability    *DurabilitySpec             `json:"durability,omitempty" yaml:"durability,omitempty"`
	Service       *ServiceNodeSpec            `json:"service,omitempty" yaml:"service,omitempty"`
	Compensation  *CompensationSpec           `json:"compensation,omitempty" yaml:"compensation,omitempty"`
	Policy        []PolicyRequirement         `json:"policy_requirements,omitempty" yaml:"policy_requirements,omitempty"`
	Target        ExecutionTargetRequirements `json:"execution_target,omitempty" yaml:"execution_target,omitempty"`
	Source        *SourceRef                  `json:"source,omitempty" yaml:"source,omitempty"`
	Provenance    Provenance                  `json:"provenance,omitempty" yaml:"provenance,omitempty"`
	Extensions    map[string]Extension        `json:"extensions,omitempty" yaml:"extensions,omitempty"`
	Metadata      Metadata                    `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// Need declares an explicit ordering or data dependency on another node.
type Need struct {
	Node   string     `json:"node" yaml:"node"`
	Kind   EdgeKind   `json:"kind,omitempty" yaml:"kind,omitempty"`
	Source *SourceRef `json:"source,omitempty" yaml:"source,omitempty"`
}

// Edge is a normalized graph edge. Compilers may derive edges from Needs and
// bindings while preserving the declaration source.
type Edge struct {
	From     string     `json:"from" yaml:"from"`
	To       string     `json:"to" yaml:"to"`
	Kind     EdgeKind   `json:"kind" yaml:"kind"`
	Source   *SourceRef `json:"source,omitempty" yaml:"source,omitempty"`
	Metadata Metadata   `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// Expression is unevaluated expression-language source.
type Expression struct {
	Text   string     `json:"text" yaml:"text"`
	Source *SourceRef `json:"source,omitempty" yaml:"source,omitempty"`
}

// Binding provides a typed place for a literal, raw expression, or string
// interpolation without evaluating it in the graph package.
type Binding struct {
	Kind          BindingKind `json:"kind" yaml:"kind"`
	Literal       any         `json:"literal,omitempty" yaml:"literal,omitempty"`
	Expression    *Expression `json:"expression,omitempty" yaml:"expression,omitempty"`
	Interpolation string      `json:"interpolation,omitempty" yaml:"interpolation,omitempty"`
	Source        *SourceRef  `json:"source,omitempty" yaml:"source,omitempty"`
}

// ForEachSpec describes runtime fan-out while keeping the graph static.
type ForEachSpec struct {
	Items          Expression `json:"items" yaml:"items"`
	ItemName       string     `json:"item_name,omitempty" yaml:"item_name,omitempty"`
	IndexName      string     `json:"index_name,omitempty" yaml:"index_name,omitempty"`
	MaxConcurrency int        `json:"max_concurrency,omitempty" yaml:"max_concurrency,omitempty"`
	// FailFast prevents fan-out items that have not started from being admitted
	// after the first non-tolerated terminal item failure. Already-running items
	// still converge through their ordinary attempt lifecycle.
	FailFast bool                    `json:"fail_fast,omitempty" yaml:"fail_fast,omitempty"`
	Tolerate *ToleratedFailurePolicy `json:"tolerate,omitempty" yaml:"tolerate,omitempty"`
}

// ToleratedFailurePolicy permits an explicit count or percentage of fan-out
// item failures. A nil policy means unhandled item failure fails the fan-out.
type ToleratedFailurePolicy struct {
	Count      int     `json:"count,omitempty" yaml:"count,omitempty"`
	Percentage float64 `json:"percentage,omitempty" yaml:"percentage,omitempty"`
}

// RetryPolicy describes retry eligibility and backoff, independent of any
// concrete executor.
type RetryPolicy struct {
	Attempts int           `json:"attempts" yaml:"attempts"`
	Backoff  BackoffPolicy `json:"backoff,omitempty" yaml:"backoff,omitempty"`
	On       []string      `json:"on,omitempty" yaml:"on,omitempty"`
}

// BackoffPolicy describes how delays grow between attempts.
type BackoffPolicy struct {
	Strategy     BackoffStrategy `json:"strategy" yaml:"strategy"`
	InitialDelay Duration        `json:"initial_delay,omitempty" yaml:"initial_delay,omitempty"`
	MaxDelay     Duration        `json:"max_delay,omitempty" yaml:"max_delay,omitempty"`
	Multiplier   float64         `json:"multiplier,omitempty" yaml:"multiplier,omitempty"`
}

// IdempotencySpec records the declaration needed to reason about safe retry and
// recovery. Hosts and executors decide whether the claim is sufficient.
type IdempotencySpec struct {
	Mode       IdempotencyMode `json:"mode" yaml:"mode"`
	Key        *Expression     `json:"key,omitempty" yaml:"key,omitempty"`
	Scope      string          `json:"scope,omitempty" yaml:"scope,omitempty"`
	Extensions Config          `json:"extensions,omitempty" yaml:"extensions,omitempty"`
}

// TimeoutPolicy distinguishes queue, execution, wait, heartbeat, and total
// schedule-to-close deadlines.
type TimeoutPolicy struct {
	Queue           Duration `json:"queue,omitempty" yaml:"queue,omitempty"`
	Execution       Duration `json:"execution,omitempty" yaml:"execution,omitempty"`
	Wait            Duration `json:"wait,omitempty" yaml:"wait,omitempty"`
	Heartbeat       Duration `json:"heartbeat,omitempty" yaml:"heartbeat,omitempty"`
	ScheduleToClose Duration `json:"schedule_to_close,omitempty" yaml:"schedule_to_close,omitempty"`
}

// CatchRule routes a matching structured error to ordinary graph nodes. An
// empty Errors list deterministically matches every error; When may narrow it.
// BindAs is an expression-local lower-snake identifier, not a graph node ID.
type CatchRule struct {
	Errors  []string    `json:"errors,omitempty" yaml:"errors,omitempty"`
	When    *Expression `json:"when,omitempty" yaml:"when,omitempty"`
	Targets []string    `json:"targets" yaml:"targets"`
	BindAs  string      `json:"bind_as,omitempty" yaml:"bind_as,omitempty"`
	Source  *SourceRef  `json:"source,omitempty" yaml:"source,omitempty"`
}

// CatchAllErrors is the canonical catch selector for every structured failure.
const CatchAllErrors = "*"

// ContinueOnError reports whether this rule is the compiler's exact policy
// sugar for handling every failure without selecting a handler node. Keeping
// the lowering in CatchRule means the runtime has one error-routing model.
func (r CatchRule) ContinueOnError() bool {
	return len(r.Errors) == 1 && r.Errors[0] == CatchAllErrors && r.When == nil && len(r.Targets) == 0 && r.BindAs == ""
}

// FinallySpec marks an ordinary node as cleanup for a declared graph scope.
// Empty Scope means the whole workflow.
type FinallySpec struct {
	Scope []string `json:"scope,omitempty" yaml:"scope,omitempty"`
}

// SwitchSpec evaluates arms in order and selects the first matching branch.
type SwitchSpec struct {
	Arms    []SwitchArm `json:"arms" yaml:"arms"`
	Default []string    `json:"default,omitempty" yaml:"default,omitempty"`
}

// SwitchArm is one ordered branch in a SwitchSpec.
type SwitchArm struct {
	When    Expression `json:"when" yaml:"when"`
	Targets []string   `json:"targets" yaml:"targets"`
	Source  *SourceRef `json:"source,omitempty" yaml:"source,omitempty"`
}

// DefinitionRef is an application-neutral reference to workflow source. A
// compiler or host resolves Locator and Version to Digest before execution.
type DefinitionRef struct {
	Authority  string      `json:"authority,omitempty" yaml:"authority,omitempty"`
	Kind       string      `json:"kind,omitempty" yaml:"kind,omitempty"`
	ID         string      `json:"id,omitempty" yaml:"id,omitempty"`
	Locator    string      `json:"locator,omitempty" yaml:"locator,omitempty"`
	Version    string      `json:"version,omitempty" yaml:"version,omitempty"`
	Digest     string      `json:"digest,omitempty" yaml:"digest,omitempty"`
	Provenance *Provenance `json:"provenance,omitempty" yaml:"provenance,omitempty"`
}

// CallSpec invokes another workflow inline or as a separately identified run.
type CallSpec struct {
	Definition DefinitionRef `json:"definition,omitempty" yaml:"definition,omitempty"`
	// DefinitionInput names a typed invocation input containing an immutable,
	// exact-digest DefinitionRef. It is mutually exclusive with Definition and
	// is resolved by the runtime before the ordinary call adapter executes.
	DefinitionInput string            `json:"definition_input,omitempty" yaml:"definition_input,omitempty"`
	Mode            CallMode          `json:"mode" yaml:"mode"`
	OnParentClose   ParentClosePolicy `json:"on_parent_close,omitempty" yaml:"on_parent_close,omitempty"`
}

// Provenance explains the authority and immutable origin of graph material
// without depending on a host principal or registry type.
type Provenance struct {
	Authority string          `json:"authority,omitempty" yaml:"authority,omitempty"`
	Origin    string          `json:"origin,omitempty" yaml:"origin,omitempty"`
	Locator   string          `json:"locator,omitempty" yaml:"locator,omitempty"`
	Revision  string          `json:"revision,omitempty" yaml:"revision,omitempty"`
	Digest    string          `json:"digest,omitempty" yaml:"digest,omitempty"`
	Parents   []ProvenanceRef `json:"parents,omitempty" yaml:"parents,omitempty"`
	Metadata  Metadata        `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// ProvenanceRef is a compact link to parent provenance.
type ProvenanceRef struct {
	Authority string `json:"authority,omitempty" yaml:"authority,omitempty"`
	Locator   string `json:"locator,omitempty" yaml:"locator,omitempty"`
	Digest    string `json:"digest,omitempty" yaml:"digest,omitempty"`
}

// ActivationDeclaration is an immutable, source-declared request for a host to
// materialize an operational activation registration. Provenance records the
// source authority; Config remains host-adapter opaque.
type ActivationDeclaration struct {
	ID         string             `json:"id" yaml:"id"`
	Kind       string             `json:"kind" yaml:"kind"`
	Config     Config             `json:"config,omitempty" yaml:"config,omitempty"`
	Inputs     map[string]Binding `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Policy     ActivationPolicy   `json:"policy,omitempty" yaml:"policy,omitempty"`
	Provenance Provenance         `json:"provenance,omitempty" yaml:"provenance,omitempty"`
	Source     *SourceRef         `json:"source,omitempty" yaml:"source,omitempty"`
	Metadata   Metadata           `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// ActivationPolicy carries portable overlap, deduplication, and missed-fire
// declarations. A host resolves them before creating a bound run.
type ActivationPolicy struct {
	Overlap          OverlapPolicy    `json:"overlap,omitempty" yaml:"overlap,omitempty"`
	StartingDeadline Duration         `json:"starting_deadline,omitempty" yaml:"starting_deadline,omitempty"`
	Catchup          bool             `json:"catchup,omitempty" yaml:"catchup,omitempty"`
	DeduplicationKey *Expression      `json:"deduplication_key,omitempty" yaml:"deduplication_key,omitempty"`
	RunIDReuse       RunIDReusePolicy `json:"run_id_reuse,omitempty" yaml:"run_id_reuse,omitempty"`
}

// SourceRef locates graph material in source authored by any supported
// frontend. Legacy source formats are reference-only formats.
type SourceRef struct {
	Format      SourceFormat `json:"format" yaml:"format"`
	Locator     string       `json:"locator" yaml:"locator"`
	StartLine   int          `json:"start_line,omitempty" yaml:"start_line,omitempty"`
	StartColumn int          `json:"start_column,omitempty" yaml:"start_column,omitempty"`
	EndLine     int          `json:"end_line,omitempty" yaml:"end_line,omitempty"`
	EndColumn   int          `json:"end_column,omitempty" yaml:"end_column,omitempty"`
	Section     string       `json:"section,omitempty" yaml:"section,omitempty"`
	StepName    string       `json:"step_name,omitempty" yaml:"step_name,omitempty"`
	StageName   string       `json:"stage_name,omitempty" yaml:"stage_name,omitempty"`
	Path        []string     `json:"path,omitempty" yaml:"path,omitempty"`
}

// SourceMap gathers source locations by semantic graph element. ExecutionPlan
// compaction and persistence are compiler concerns, not graph behavior.
type SourceMap struct {
	Graph       *SourceRef           `json:"graph,omitempty" yaml:"graph,omitempty"`
	Inputs      map[string]SourceRef `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Outputs     map[string]SourceRef `json:"outputs,omitempty" yaml:"outputs,omitempty"`
	Nodes       map[string]SourceRef `json:"nodes,omitempty" yaml:"nodes,omitempty"`
	Edges       map[string]SourceRef `json:"edges,omitempty" yaml:"edges,omitempty"`
	Activations map[string]SourceRef `json:"activations,omitempty" yaml:"activations,omitempty"`
}
