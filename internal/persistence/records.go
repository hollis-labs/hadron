package persistence

import (
	"database/sql"
	"time"
)

type RunRecord struct {
	ID            string
	WorkspaceID   string
	BlueprintPath string
	Status        string
	InputJSON     string
	ErrorMessage  sql.NullString
	CreatedAt     time.Time
	StartedAt     sql.NullString
	EndedAt       sql.NullString
}

type ScheduleRecord struct {
	ID            string
	WorkspaceID   string
	Name          string
	BlueprintPath string
	CronExpr      string
	Enabled       bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
	LastRunAt     sql.NullString
	NextRunAt     sql.NullString
}

type RunEventRecord struct {
	ID        int64
	RunID     string
	StepName  sql.NullString
	EventType string
	Message   sql.NullString
	CreatedAt time.Time
}

type PipelineRunRecord struct {
	ID           string
	WorkspaceID  string
	PipelinePath string
	Status       string
	ErrorMessage sql.NullString
	CreatedAt    time.Time
	StartedAt    sql.NullString
	EndedAt      sql.NullString
}

type PipelineStageRunRecord struct {
	ID            int64
	WorkspaceID   string
	PipelineRunID string
	StageIndex    int
	StageName     string
	RunID         string
	Status        string
	OutputsJSON   string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type TriggerRecord struct {
	ID              string
	Type            string
	Name            string
	Path            string
	BlueprintPath   string
	WorkspaceID     string
	SecretHash      sql.NullString
	ExtractInputs   sql.NullString
	Enabled         bool
	OneShot         bool
	TTLExpiresAt    sql.NullString
	CreatedAt       time.Time
	UpdatedAt       time.Time
	FiredCount      int
	LastFiredAt     sql.NullString
	DebounceSeconds int
	CreatedBy       string
}

type WorkspaceRecord struct {
	ID        string
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type HumanGateRecord struct {
	ID          string
	WorkspaceID string
	RunID       string
	StepName    string
	Prompt      string
	OptionsJSON string
	Status      string
	Decision    sql.NullString
	CreatedAt   time.Time
	UpdatedAt   time.Time
	ExpiresAt   sql.NullString
}

type MessageRecord struct {
	ID            string
	Substrate     string
	Kind          string
	Channel       string
	FromURN       string
	ToURN         string
	ThreadID      string
	InReplyTo     string
	CorrelationID string
	PayloadJSON   string
	ContentType   string
	MetadataJSON  string
	CreatedAt     time.Time
	DeliveredAt   sql.NullString
	ConsumedAt    sql.NullString
	CanceledAt    sql.NullString
}
