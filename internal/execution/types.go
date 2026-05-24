package execution

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/telemetry"
)

// RunStore is the persistence interface required by the manager.
type RunStore interface {
	CreateRun(ctx context.Context, rec persistence.RunRecord) error
	SetRunStarted(ctx context.Context, id string, startedAt time.Time) error
	SetRunFinished(ctx context.Context, id, status string, endedAt time.Time, errMsg *string) error
	AppendRunEvent(ctx context.Context, rec persistence.RunEventRecord) error
	CreateHumanGate(ctx context.Context, rec persistence.HumanGateRecord) error
	GetHumanGate(ctx context.Context, id string) (persistence.HumanGateRecord, error)
	ExpireHumanGate(ctx context.Context, id string, expiredAt time.Time) error
}

// SettingsValidator is the safety-check interface from the settings package.
type SettingsValidator interface {
	GetDefaultTimeout() int
	ValidateCommand(cmd string) error
	ValidatePath(path string) error
}

type MCPCaller interface {
	CallTool(ctx context.Context, server, tool string, arguments map[string]any) (any, error)
}

type MCPToolResult struct {
	Result   any
	Metadata MCPCallMetadata
}

type MCPCallMetadata struct {
	Server       string
	Transport    string
	ReusedClient bool
	HealthProbe  bool
	Reconnected  bool
	RetryCount   int
	AttemptCount int
}

type MessageQuery struct {
	Substrate     string
	To            string
	CorrelationID string
}

type Message struct {
	ID       string
	Body     string
	BodyJSON any
}

type MessageSource interface {
	PollMessage(ctx context.Context, query MessageQuery) (*Message, error)
}

type AgentLaunchRequest struct {
	Substrate      string
	LaunchID       string
	LogicalAgentID string
	PromptAppend   string
	BlueprintPath  string
	StepDir        string
	Injection      AgentInjection
	Metadata       map[string]any
}

type AgentInjection struct {
	NativeFiles []AgentNativeFile
}

type AgentNativeFile struct {
	RelPath string
	Source  string
}

type AgentLaunchResult struct {
	SessionID string
	Mailbox   string
	Handles   map[string]any
}

type AgentLauncher interface {
	LaunchAgent(ctx context.Context, req AgentLaunchRequest) (AgentLaunchResult, error)
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
	mcpCaller MCPCaller
	messages  MessageSource
	agents    AgentLauncher
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
