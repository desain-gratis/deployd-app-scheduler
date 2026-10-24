package entity

import (
	"time"

	"github.com/desain-gratis/common/delivery/mycontent-api/mycontent"
)

var _ mycontent.Data = &TaskExecution{}

// TaskExecution represents raft TaskExecution configuration for a particular service
type TaskExecution struct {
	Ns string `json:"namespace"`

	Id       string  `json:"id"`
	ParentId *string `json:"parent_id"`

	QueueID string `json:"queue_id"`

	ScheduledRunAt time.Time       `json:"scheduled_run_at"`
	ActualRunAt    *time.Time      `json:"actual_run_at"`
	CompletedAt    *time.Time      `json:"completed_at"`
	UserTime       time.Time       `json:"user_time"`
	Status         ExecutionStatus `json:"status"`

	Input  any `json:"input,omitempty"`
	Output any `json:"output,omitempty"`

	AssignmentStatus *AssignmentState `json:"assignment_status"`

	ExecutionMode TaskExecutionMode `json:"execution_mode"` // todo: naming

	RecurringTaskID *string `json:"recurring_task_id"`

	PublishedAt time.Time `json:"published_at"`
	URLx        string    `json:"url"`
}

type TaskExecutionMode string

const (
	TaskExecutionModeRecurring TaskExecutionMode = "RECURRING"
	TaskExecutionModeImmediate TaskExecutionMode = "IMMEDIATE"
	TaskExecutionModeScheduled TaskExecutionMode = "SCHEDULED"
)

type ReccurenceState struct {
	RecurrenceID string `json:"recurrence_id"`
}

type AssignmentState struct {
	AssignedTo      uint64    `json:"assigned_to"` // Who to execute this task, as assigned by the Leader
	AssignedBy      uint64    `json:"assigned_by"`
	AssignmentTime  time.Time `json:"assignment_time"`
	AssignmentCount int       `json:"assignment_count"`
}

type ExecutionStatus string

// -- materialized task only concern with this ---
// PENDING -> IN PROGRESS -> SUCCEED       (done as expected without error)
//         |              |_> FAILED       (finished with error)     --> leader will determine solution (retry)
//         |              |_> INTERRUPTED  (server shutdown etc..)   --> leader will determine solution (retry)
//         |              |_> CANCELLED    (user cancel)
//         |_> CANCELLED

const (
	ExecutionStatusMissed      ExecutionStatus = "MISSED"
	ExecutionStatusPending     ExecutionStatus = "PENDING"
	ExecutionStatusInProgress  ExecutionStatus = "IN_PROGRESS"
	ExecutionStatusTimeout     ExecutionStatus = "TIMEOUT"
	ExecutionStatusInterrupted ExecutionStatus = "INTERRUPTED"
	ExecutionStatusCancelled   ExecutionStatus = "CANCELLED"
	ExecutionStatusFailed      ExecutionStatus = "FAILED"
	ExecutionStatusSucceed     ExecutionStatus = "SUCCEED"
)

func (a *TaskExecution) CreatedTime() time.Time {
	return a.PublishedAt
}

func (a *TaskExecution) ID() string {
	return a.Id
}

func (a *TaskExecution) Namespace() string {
	return a.Ns
}

func (a *TaskExecution) RefIDs() []string {
	return []string{a.QueueID} // not include task ID expected
}

func (a *TaskExecution) URL() string {
	return a.URLx
}

func (a *TaskExecution) Validate() error {
	return nil
}

func (a *TaskExecution) WithCreatedTime(t time.Time) mycontent.Data {
	a.PublishedAt = t
	return a
}

func (a *TaskExecution) WithID(id string) mycontent.Data {
	a.Id = id
	return a
}

func (a *TaskExecution) WithNamespace(id string) mycontent.Data {
	a.Ns = id
	return a
}

func (a *TaskExecution) WithURL(url string) mycontent.Data {
	a.URLx = url
	return a
}

func (a *TaskExecution) WithVersion(ver uint64) mycontent.Data {
	return a
}

func (a *TaskExecution) DGVersion() *uint64 {
	return nil
}
