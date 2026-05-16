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
