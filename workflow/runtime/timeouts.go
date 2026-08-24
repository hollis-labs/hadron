package runtime

import (
	"fmt"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
)

// TimeoutKind distinguishes operational deadlines that have different retry
// and cancellation semantics.
type TimeoutKind string

const (
	TimeoutQueue           TimeoutKind = "queue"
	TimeoutExecution       TimeoutKind = "execution"
	TimeoutExternalWait    TimeoutKind = "external_wait"
	TimeoutHeartbeat       TimeoutKind = "heartbeat"
	TimeoutScheduleToClose TimeoutKind = "schedule_to_close"
)

// Valid reports whether k is a supported timeout category.
func (k TimeoutKind) Valid() bool {
	switch k {
	case TimeoutQueue, TimeoutExecution, TimeoutExternalWait, TimeoutHeartbeat, TimeoutScheduleToClose:
		return true
	default:
		return false
	}
}

// TimeoutAnchor contains durable timestamps used by timeout evaluation. Zero
// timestamps mean the corresponding executor phase has not begun.
type TimeoutAnchor struct {
	ScheduledAt     time.Time
	StartedAt       time.Time
	ExternalAt      time.Time
	LastHeartbeatAt time.Time
}

// TimeoutDeadline is one configured operational deadline.
type TimeoutDeadline struct {
	Kind     TimeoutKind
	Deadline time.Time
}

// TimeoutEvaluation lists all configured deadlines in chronological then kind
// order and identifies the first deadline due at Now.
type TimeoutEvaluation struct {
	Deadlines []TimeoutDeadline
	Due       *TimeoutDeadline
}

// EvaluateTimeouts resolves the graph duration vocabulary against durable
// phase anchors. Generic wait deadlines remain owned by WaitCoordinator.
func EvaluateTimeouts(policy *graph.TimeoutPolicy, anchor TimeoutAnchor, now time.Time) (TimeoutEvaluation, error) {
	if policy == nil {
		return TimeoutEvaluation{}, nil
	}
	if anchor.ScheduledAt.IsZero() || now.IsZero() {
		return TimeoutEvaluation{}, fmt.Errorf("timeout evaluation requires scheduled_at and now")
	}
	type timeoutField struct {
		kind TimeoutKind
		raw  graph.Duration
		base time.Time
	}
	configured := []timeoutField{
		{TimeoutScheduleToClose, policy.ScheduleToClose, anchor.ScheduledAt},
	}
	switch {
	case anchor.StartedAt.IsZero():
		configured = append(configured, timeoutField{TimeoutQueue, policy.Queue, anchor.ScheduledAt})
	case anchor.ExternalAt.IsZero():
		configured = append(configured, timeoutField{TimeoutExecution, policy.Execution, anchor.StartedAt})
	default:
		heartbeatBase := anchor.LastHeartbeatAt
		if heartbeatBase.IsZero() {
			heartbeatBase = anchor.ExternalAt
		}
		configured = append(configured,
			timeoutField{TimeoutExternalWait, policy.Wait, anchor.ExternalAt},
			timeoutField{TimeoutHeartbeat, policy.Heartbeat, heartbeatBase},
		)
	}
	result := TimeoutEvaluation{}
	for _, field := range configured {
		if field.raw == "" || field.base.IsZero() {
			continue
		}
		duration, err := time.ParseDuration(string(field.raw))
		if err != nil || duration <= 0 {
			return TimeoutEvaluation{}, fmt.Errorf("invalid %s timeout %q", field.kind, field.raw)
		}
		result.Deadlines = append(result.Deadlines, TimeoutDeadline{Kind: field.kind, Deadline: field.base.UTC().Add(duration)})
	}
	sort.Slice(result.Deadlines, func(i, j int) bool {
		if !result.Deadlines[i].Deadline.Equal(result.Deadlines[j].Deadline) {
			return result.Deadlines[i].Deadline.Before(result.Deadlines[j].Deadline)
		}
		return result.Deadlines[i].Kind < result.Deadlines[j].Kind
	})
	for index := range result.Deadlines {
		if !now.Before(result.Deadlines[index].Deadline) {
			due := result.Deadlines[index]
			result.Due = &due
			break
		}
	}
	return result, nil
}

// TimeoutFailure produces the stable failure used by retry and lifecycle
// policy. The exact category remains searchable in Details.
func TimeoutFailure(kind TimeoutKind, deadline time.Time) Failure {
	return Failure{
		Code: "timeout_" + string(kind), Message: "workflow operation exceeded its " + string(kind) + " timeout",
		Retryable: true, Details: map[string]string{"timeout_kind": string(kind), "deadline": deadline.UTC().Format(time.RFC3339Nano)},
	}
}
