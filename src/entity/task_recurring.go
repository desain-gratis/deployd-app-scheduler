package entity

import (
	"time"

	"github.com/desain-gratis/common/delivery/mycontent-api/mycontent"
)

var _ mycontent.Data = &RecurringTask{}

// RecurringTask represents a recurring task
// It's the main entry point for executing a task
type RecurringTask struct {
	Ns string `json:"namespace"`

	Id string `json:"id"`

	// task queue/channel name, eg. sendInvoice()
	// is the task queue that worker can pickup
	QueueID string `json:"queue_id"`

	Input any `json:"input,omitempty"`

	// to get user timezone
	UserTime time.Time `json:"user_time"` // for timezone

	// how will this task be executed?
	Cron string `json:"cron"`

	// used by server
	State *RecurringTaskState `json:"state,omitempty"`

	PublishedAt time.Time `json:"published_at"`
	URLx        string    `json:"url"`
}

type Queue struct {
	ID    string `json:"id"`
	Param any    `json:"param"`
}

type RecurringTaskState struct {

	// for scheduled tasks
	LastGenerateStart time.Time `json:"last_generate_start"`
	LastGenerateEnd   time.Time `json:"last_generate_end"`
}

func (a *RecurringTask) CreatedTime() time.Time {
	return a.PublishedAt
}

func (a *RecurringTask) ID() string {
	return a.Id
}

func (a *RecurringTask) Namespace() string {
	return a.Ns
}

func (a *RecurringTask) RefIDs() []string {
	return nil
}

func (a *RecurringTask) URL() string {
	return a.URLx
}

func (a *RecurringTask) Validate() error {
	return nil
}

func (a *RecurringTask) WithCreatedTime(t time.Time) mycontent.Data {
	a.PublishedAt = t
	return a
}

func (a *RecurringTask) WithID(id string) mycontent.Data {
	a.Id = id
	return a
}

func (a *RecurringTask) WithNamespace(id string) mycontent.Data {
	a.Ns = id
	return a
}

func (a *RecurringTask) WithURL(url string) mycontent.Data {
	a.URLx = url
	return a
}

func (a *RecurringTask) WithVersion(ver uint64) mycontent.Data {
	return a
}

func (a *RecurringTask) DGVersion() *uint64 {
	return nil
}
