package entity

import (
	"time"

	"github.com/desain-gratis/common/delivery/mycontent-api/mycontent"
)

var _ mycontent.Data = &Task{}

// Task represents an asynchronous task
// It's the main entry point for executing a task
type Task struct {
	Ns string `json:"namespace"`

	Id string `json:"id"`

	// task queue name, eg. sendInvoice()
	QueueID    string `json:"queue_id"`
	QueueParam any    `json:"queue_param,omitempty"`

	// to get user timezone
	UserTime time.Time `json:"user_time"` // for timezone

	// how will this task be executed?
	ExecutionStrategy      ExecutionStrategy       `json:"execution_strategy"`
	ExecutionStrategyInput *ExecutionStrategyInput `json:"execution_strategy_input,omitempty"`

	// used by server
	State *TaskState `json:"state,omitempty"`

	PublishedAt time.Time `json:"published_at"`
	URLx        string    `json:"url"`
}

type ExecutionStrategy string

const (
	// Immediate execution, as long as there is worker ready;
	ExecutionStrategyImmediate = "IMMEDIATE"

	// Scheduled execution
	ExecutionStrategyScheduled = "SCHEDULED"

	// Execute when parent task finished
	ExecutionStrategyParent = "PARENT"
)

type ExecutionStrategyInput struct {
	Scheduled *ExecutionStrategyInputScheduled `json:"scheduled,omitempty"`
	Parent    *ExecutionStrategyInputParent    `json:"parent,omitempty"`
}

type ExecutionStrategyInputScheduled struct {
	Cron string `json:"cron"`
}

type ExecutionStrategyInputParent struct {
	ParentTaskIDs []string `json:"parent_task_ids"`
}

type TaskState struct {

	// for scheduled tasks
	LastGenerateStart time.Time `json:"last_generate_start"`
	LastGenerateEnd   time.Time `json:"last_generate_end"`
}

func (a *Task) CreatedTime() time.Time {
	return a.PublishedAt
}

func (a *Task) ID() string {
	return a.Id
}

func (a *Task) Namespace() string {
	return a.Ns
}

func (a *Task) RefIDs() []string {
	return nil
}

func (a *Task) URL() string {
	return a.URLx
}

func (a *Task) Validate() error {
	return nil
}

func (a *Task) WithCreatedTime(t time.Time) mycontent.Data {
	a.PublishedAt = t
	return a
}

func (a *Task) WithID(id string) mycontent.Data {
	a.Id = id
	return a
}

func (a *Task) WithNamespace(id string) mycontent.Data {
	a.Ns = id
	return a
}

func (a *Task) WithURL(url string) mycontent.Data {
	a.URLx = url
	return a
}

func (a *Task) WithVersion(ver uint64) mycontent.Data {
	return a
}

func (a *Task) DGVersion() *uint64 {
	return nil
}
