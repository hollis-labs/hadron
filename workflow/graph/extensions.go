package graph

// Extension reserves a versioned, application-neutral location for semantics
// implemented by a later engine or host capability.
type Extension struct {
	Version  string     `json:"version,omitempty" yaml:"version,omitempty"`
	Config   Config     `json:"config,omitempty" yaml:"config,omitempty"`
	Source   *SourceRef `json:"source,omitempty" yaml:"source,omitempty"`
	Metadata Metadata   `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// ConcurrencySpec declares named resources whose claims can be enforced across
// nodes and runs by a host scheduler.
type ConcurrencySpec struct {
	Resources []ConcurrencyResource `json:"resources,omitempty" yaml:"resources,omitempty"`
	MaxRun    int                   `json:"max_run,omitempty" yaml:"max_run,omitempty"`
	Extension Extension             `json:"extension,omitempty" yaml:"extension,omitempty"`
}

// ConcurrencyResource defines a portable named semaphore.
type ConcurrencyResource struct {
	Name     string   `json:"name" yaml:"name"`
	Limit    int      `json:"limit" yaml:"limit"`
	Metadata Metadata `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// ConcurrencyClaim requests units from a named concurrency resource.
type ConcurrencyClaim struct {
	Resource string `json:"resource" yaml:"resource"`
	Amount   int    `json:"amount,omitempty" yaml:"amount,omitempty"`
}

// RunCompletionPolicy selects fail-fast or run-to-completion behavior while
// retaining a versioned extension location for later details.
type RunCompletionPolicy struct {
	Mode      RunCompletionMode `json:"mode" yaml:"mode"`
	Extension Extension         `json:"extension,omitempty" yaml:"extension,omitempty"`
}

// VerificationSpec describes post-execution checks without importing verifier
// or provider implementations.
type VerificationSpec struct {
	Checks    []VerificationCheck `json:"checks,omitempty" yaml:"checks,omitempty"`
	Extension Extension           `json:"extension,omitempty" yaml:"extension,omitempty"`
}

// VerificationCheck identifies a registered verifier and keeps its config
// opaque to the graph package.
type VerificationCheck struct {
	Kind   string     `json:"kind" yaml:"kind"`
	Config Config     `json:"config,omitempty" yaml:"config,omitempty"`
	Source *SourceRef `json:"source,omitempty" yaml:"source,omitempty"`
}

// MemoizationSpec reserves the accepted cache-key and freshness concepts while
// leaving cache storage and authorization to later runtime work.
type MemoizationSpec struct {
	Key          Expression `json:"key" yaml:"key"`
	MaxAge       Duration   `json:"max_age,omitempty" yaml:"max_age,omitempty"`
	OutputDigest string     `json:"output_digest,omitempty" yaml:"output_digest,omitempty"`
	Extension    Extension  `json:"extension,omitempty" yaml:"extension,omitempty"`
}

// DurabilitySpec selects the portable persistence mode. Runtime storage and
// continue-as-new mechanics remain outside the graph package.
type DurabilitySpec struct {
	Mode      DurabilityMode `json:"mode" yaml:"mode"`
	Extension Extension      `json:"extension,omitempty" yaml:"extension,omitempty"`
}

// ServiceNodeSpec reserves service readiness, heartbeat observation, and
// teardown references without introducing a separate scheduler.
type ServiceNodeSpec struct {
	ReadyCheck       *VerificationSpec `json:"ready_check,omitempty" yaml:"ready_check,omitempty"`
	HeartbeatTimeout Duration          `json:"heartbeat_timeout,omitempty" yaml:"heartbeat_timeout,omitempty"`
	TeardownNodes    []string          `json:"teardown_nodes,omitempty" yaml:"teardown_nodes,omitempty"`
	Extension        Extension         `json:"extension,omitempty" yaml:"extension,omitempty"`
}

// CompensationSpec is intentionally opaque until the compensation ADR selects
// registration, ordering, persistence, replay, and cancellation semantics.
type CompensationSpec struct {
	Extension Extension `json:"extension" yaml:"extension"`
}

// PolicyRequirement names a host-resolved policy or grant requirement without
// importing a concrete policy authority.
type PolicyRequirement struct {
	Name       string `json:"name" yaml:"name"`
	Version    string `json:"version,omitempty" yaml:"version,omitempty"`
	Parameters Config `json:"parameters,omitempty" yaml:"parameters,omitempty"`
}

// ExecutionTargetRequirements describes compute capabilities and labels
// required by a graph or node. It never identifies a Hadron run scope or
// workspace record.
type ExecutionTargetRequirements struct {
	Kinds        []string          `json:"kinds,omitempty" yaml:"kinds,omitempty"`
	Capabilities []string          `json:"capabilities,omitempty" yaml:"capabilities,omitempty"`
	Labels       map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Constraints  Config            `json:"constraints,omitempty" yaml:"constraints,omitempty"`
}
