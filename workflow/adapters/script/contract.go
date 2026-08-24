package script

import (
	"errors"
	"fmt"
	"time"
)

const (
	// Name is the canonical registered script kind name.
	Name = "script"
	// Version is the immutable initial Goja script contract version.
	Version = "v1"
	// RuntimeGoja is the only runtime accepted by script@v1.
	RuntimeGoja = "goja"

	CodeInvalidConfig      = "script_invalid_config"
	CodeCapabilityDenied   = "script_capability_denied"
	CodeInputInvalid       = "script_input_invalid"
	CodeOutputInvalid      = "script_output_invalid"
	CodeResourceLimit      = "script_resource_limit"
	CodeExecutionFailed    = "script_execution_failed"
	CodeExecutionTimeout   = "script_execution_timeout"
	CodeExecutionCanceled  = "script_execution_canceled"
	CodeEntrypointInvalid  = "script_entrypoint_invalid"
	CodeRuntimeUnavailable = "script_runtime_unavailable"
)

const (
	DefaultMaxSourceBytes = 64 << 10
	DefaultMaxInputBytes  = 1 << 20
	DefaultMaxOutputBytes = 1 << 20
	DefaultMaxDepth       = 64
	DefaultMaxItems       = 16_384
	DefaultMaxStringBytes = 256 << 10
	DefaultMaxCallStack   = 128
	DefaultWallTime       = time.Second

	MaximumSourceBytes = 1 << 20
	MaximumDataBytes   = 16 << 20
	MaximumDepth       = 128
	MaximumItems       = 1_000_000
	MaximumStringBytes = 16 << 20
	MaximumCallStack   = 1_024
	MaximumWallTime    = time.Minute
)

var (
	ErrInvalidLimits      = errors.New("invalid script resource limits")
	ErrCapabilityDenied   = errors.New("script capability denied")
	ErrResourceLimit      = errors.New("script resource limit exceeded")
	ErrInvalidInput       = errors.New("invalid script input")
	ErrInvalidOutput      = errors.New("invalid script output")
	ErrUnsafeJSONNumber   = errors.New("JSON number cannot be represented exactly by JavaScript")
	ErrRuntimeUnavailable = errors.New("script runtime unavailable")
)

// ResourceLimits are hard per-invocation structural and execution bounds.
// They deliberately do not claim a per-Goja-runtime heap quota.
type ResourceLimits struct {
	MaxSourceBytes int           `json:"max_source_bytes"`
	MaxInputBytes  int           `json:"max_input_bytes"`
	MaxOutputBytes int           `json:"max_output_bytes"`
	MaxDepth       int           `json:"max_depth"`
	MaxItems       int           `json:"max_items"`
	MaxStringBytes int           `json:"max_string_bytes"`
	MaxCallStack   int           `json:"max_call_stack"`
	WallTime       time.Duration `json:"wall_time"`
}

// DefaultResourceLimits returns conservative extraction-safe limits.
func DefaultResourceLimits() ResourceLimits {
	return ResourceLimits{
		MaxSourceBytes: DefaultMaxSourceBytes,
		MaxInputBytes:  DefaultMaxInputBytes,
		MaxOutputBytes: DefaultMaxOutputBytes,
		MaxDepth:       DefaultMaxDepth,
		MaxItems:       DefaultMaxItems,
		MaxStringBytes: DefaultMaxStringBytes,
		MaxCallStack:   DefaultMaxCallStack,
		WallTime:       DefaultWallTime,
	}
}

// Validate rejects disabled or misleadingly broad resource declarations.
func (l ResourceLimits) Validate() error {
	checks := []struct {
		name  string
		value int
		max   int
	}{
		{"max_source_bytes", l.MaxSourceBytes, MaximumSourceBytes},
		{"max_input_bytes", l.MaxInputBytes, MaximumDataBytes},
		{"max_output_bytes", l.MaxOutputBytes, MaximumDataBytes},
		{"max_depth", l.MaxDepth, MaximumDepth},
		{"max_items", l.MaxItems, MaximumItems},
		{"max_string_bytes", l.MaxStringBytes, MaximumStringBytes},
		{"max_call_stack", l.MaxCallStack, MaximumCallStack},
	}
	for _, check := range checks {
		if check.value < 1 || check.value > check.max {
			return fmt.Errorf("%w: %s must be between 1 and %d", ErrInvalidLimits, check.name, check.max)
		}
	}
	if l.WallTime <= 0 || l.WallTime > MaximumWallTime {
		return fmt.Errorf("%w: wall_time must be between 1ns and %s", ErrInvalidLimits, MaximumWallTime)
	}
	return nil
}

// Executor runs one fresh Goja runtime per invocation and is safe for
// concurrent use.
type Executor struct {
	limits ResourceLimits
}

// New returns an executor with conservative default limits.
func New() *Executor {
	return &Executor{limits: DefaultResourceLimits()}
}

// NewWithLimits returns an executor with explicit hard limits.
func NewWithLimits(limits ResourceLimits) (*Executor, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &Executor{limits: limits}, nil
}

// Limits returns a defensive value copy of the executor's limits.
func (e *Executor) Limits() ResourceLimits {
	if e == nil {
		return ResourceLimits{}
	}
	return e.limits
}
