package entity

import (
	"time"

	"github.com/desain-gratis/common/delivery/mycontent-api/mycontent"
)

var _ mycontent.Data = &TaskExecution{}

// TaskExecution represents raft TaskExecution configuration for a particular service
type TaskExecution struct {
	Ns string `json:"namespace"`

	Id string `json:"id"`

	TaskID  string `json:"task_id"`
	QueueID string `json:"queue_id"`

	ScheduledRunAt time.Time       `json:"scheduled_run_at"`
	ActualRunAt    *time.Time      `json:"actual_run_at"`
	CompletedAt    *time.Time      `json:"completed_at"`
	UserTime       time.Time       `json:"user_time"`
	Status         ExecutionStatus `json:"status"`

	Input  any `json:"input,omitempty"`
	Output any `json:"output,omitempty"`

	State *TaskExecutionState `json:"state"`

	PublishedAt time.Time `json:"published_at"`
	URLx        string    `json:"url"`
}

type TaskExecutionState struct {
	AssignedWorker string `json:"assigned_worker"`
	ManagingNodeID uint64 `json:"managing_node_id"` // which node the worker communicates with
}

type ExecutionStatus string

const (
	ExecutionStatusMissed     ExecutionStatus = "MISSED"
	ExecutionStatusPending    ExecutionStatus = "PENDING"
	ExecutionStatusInProgress ExecutionStatus = "IN_PROGRESS"
	ExecutionStatusTimeout    ExecutionStatus = "TIMEOUT"
	ExecutionStatusFailed     ExecutionStatus = "FAILED"
	ExecutionStatusDone       ExecutionStatus = "DONE"
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
