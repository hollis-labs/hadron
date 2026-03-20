package plugin

import (
	"time"

	"github.com/hollis-labs/fragments-engine/plugin"
)

// Hadron Event Catalog
// These are the standard events that Hadron emits for plugins to listen to.
const (
	// Run lifecycle events
	EventRunStarted   = "run.started"
	EventRunCompleted = "run.completed"
	EventRunFailed    = "run.failed"

	// Stage lifecycle events
	EventStageStarted   = "stage.started"
	EventStageCompleted = "stage.completed"
)

// NewEvent creates a new plugin event with the given type, source, and data.
func NewEvent(eventType, source string, data map[string]interface{}) plugin.Event {
	return plugin.Event{
		Type:      eventType,
		Source:    source,
		Timestamp: time.Now(),
		Data:      data,
	}
}

// EmitRunStarted emits a run.started event.
func (h *Host) EmitRunStarted(runID, blueprintPath string) {
	h.EmitEvent(NewEvent(EventRunStarted, "hadron", map[string]interface{}{
		"run_id":         runID,
		"blueprint_path": blueprintPath,
	}))
}

// EmitRunCompleted emits a run.completed event.
func (h *Host) EmitRunCompleted(runID, blueprintPath string) {
	h.EmitEvent(NewEvent(EventRunCompleted, "hadron", map[string]interface{}{
		"run_id":         runID,
		"blueprint_path": blueprintPath,
	}))
}

// EmitRunFailed emits a run.failed event.
func (h *Host) EmitRunFailed(runID, blueprintPath, errMsg string) {
	h.EmitEvent(NewEvent(EventRunFailed, "hadron", map[string]interface{}{
		"run_id":         runID,
		"blueprint_path": blueprintPath,
		"error":          errMsg,
	}))
}

// EmitStageStarted emits a stage.started event.
func (h *Host) EmitStageStarted(runID, stageName string, stageIndex int) {
	h.EmitEvent(NewEvent(EventStageStarted, "hadron", map[string]interface{}{
		"run_id":      runID,
		"stage_name":  stageName,
		"stage_index": stageIndex,
	}))
}

// EmitStageCompleted emits a stage.completed event.
func (h *Host) EmitStageCompleted(runID, stageName string, stageIndex int) {
	h.EmitEvent(NewEvent(EventStageCompleted, "hadron", map[string]interface{}{
		"run_id":      runID,
		"stage_name":  stageName,
		"stage_index": stageIndex,
	}))
}
