package a2a

// TaskRequest is the inbound payload for POST /a2a/tasks.
type TaskRequest struct {
	ID       string         `json:"id"`                 // client-provided or auto-generated
	Skill    string         `json:"skill"`              // maps to blueprint name/slug
	Input    map[string]any `json:"input"`              // maps to blueprint inputs
	Metadata map[string]any `json:"metadata,omitempty"` // opaque caller metadata
}

// TaskResponse is returned from both submit and get endpoints.
type TaskResponse struct {
	ID     string      `json:"id"`
	Status TaskStatus  `json:"status"`
	Result *TaskResult `json:"result,omitempty"`
}

// TaskStatus describes the current state of an A2A task.
type TaskStatus struct {
	State   string `json:"state"`             // "submitted", "working", "completed", "failed", "canceled"
	Message string `json:"message,omitempty"` // human-readable detail
}

// TaskResult carries the output of a completed task.
type TaskResult struct {
	OutputType string `json:"outputType"` // e.g. "application/json"
	Output     any    `json:"output"`     // captured outputs from the run
}
