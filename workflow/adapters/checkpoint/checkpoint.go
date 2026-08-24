package checkpoint

import (
	gateadapter "github.com/hollis-labs/hadron/workflow/adapters/gate"
	"github.com/hollis-labs/hadron/workflow/stepkind"
)

const (
	KindName    = "checkpoint"
	KindVersion = "v1"

	CapabilityRespond = "checkpoint.respond"
	CapabilityWait    = "wait.resume"

	CodeInvalidCheckpoint = "checkpoint_invalid"
	CodeAuthorityFailed   = "checkpoint_authority_failed"
	CodePayloadFailed     = "checkpoint_payload_failed"
	CodeContinuation      = "checkpoint_continuation_invalid"
)

// Options is the application-owned shared gate host boundary. Authority is
// the responder seam; escalation declarations remain immutable Checkpoint
// payload metadata and are not executed by the adapter.
type Options = gateadapter.Options

// Kind is checkpoint@v1. Its config parsing, wait identity, durable
// suspension, continuation schema validation, timeout, cancellation, and
// output classification are implemented by the same profiled executor as
// human_gate@v1, preventing the two shared-gate adapters from drifting.
type Kind struct {
	*gateadapter.Executor
}

// Spec returns checkpoint metadata even on a nil receiver, while execution is
// delegated to the constructed shared gate engine.
func (*Kind) Spec() stepkind.StepKindSpec { return profile().StepKindSpec() }

func profile() gateadapter.Profile {
	return gateadapter.Profile{
		Name: KindName, Version: KindVersion, Label: "checkpoint",
		RespondCapability: CapabilityRespond, WaitCapability: CapabilityWait,
		InvalidCode: CodeInvalidCheckpoint, AuthorityFailedCode: CodeAuthorityFailed,
		PayloadFailedCode: CodePayloadFailed, ContinuationCode: CodeContinuation,
		DecisionSchema: gateadapter.DecisionSchemaConfigured,
	}
}

// New constructs checkpoint@v1 with the shared gate engine.
func New(options Options) (*Kind, error) {
	executor, err := gateadapter.NewProfile(profile(), options)
	if err != nil {
		return nil, err
	}
	return &Kind{Executor: executor}, nil
}

// Register constructs and registers checkpoint@v1.
func Register(registry stepkind.Registry, options Options) (*Kind, error) {
	executor, err := New(options)
	if err != nil {
		return nil, err
	}
	if err := gateadapter.RegisterExecutor(registry, executor); err != nil {
		return nil, err
	}
	return executor, nil
}
