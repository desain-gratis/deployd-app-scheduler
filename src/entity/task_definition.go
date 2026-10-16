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

	// task queue/channel name, eg. sendInvoice()
	// is the task queue that worker can pickup
	QueueID string `json:"queue_id"`

	Input any `json:"input,omitempty"`

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

type Queue struct {
	ID    string `json:"id"`
	Param any    `json:"param"`
}

type ExecutionStrategy string

const (
	// Immediate execution, as long as there is worker ready;
	ExecutionStrategyImmediate = "IMMEDIATE"

	// Scheduled execution
	ExecutionStrategyScheduled = "SCHEDULED"

	// Scheduled execution for a particular point in the future
	ExecutionStrategyPointInTime = "POINT_IN_TIME"
)

type ExecutionStrategyInput struct {
	Scheduled   *ExecutionStrategyInputScheduled   `json:"scheduled,omitempty"`
	PointInTime *ExecutionStrategyInputPointInTime `json:"point_in_time,omitempty"`
}

type ExecutionStrategyInputScheduled struct {
	Cron string `json:"cron"`
}

type ExecutionStrategyInputPointInTime struct {
	Time time.Time `json:"time"`
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
