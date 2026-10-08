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

	TaskID string `json:"task_id"`

	ScheduledRunAt time.Time       `json:"scheduled_run_at"`
	Status         ExecutionStatus `json:"status"`

	PublishedAt time.Time `json:"published_at"`
	URLx        string    `json:"url"`
}

type ExecutionStatus string

const (
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
	return []string{a.TaskID}
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
