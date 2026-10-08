package entity

import (
	"time"

	"github.com/desain-gratis/common/delivery/mycontent-api/mycontent"
)

var _ mycontent.Data = &TaskDefinition{}

// TaskDefinition represents raft TaskDefinition configuration for a particular service
type TaskDefinition struct {
	Ns string `json:"namespace"`

	Id string `json:"id"`

	Name        string `json:"name"`
	Description string `json:"descprition"`

	Cron     string    `json:"cron"`
	UserTime time.Time `json:"user_time"` // for timezone

	PublishedAt time.Time `json:"published_at"`
	URLx        string    `json:"url"`
}

func (a *TaskDefinition) CreatedTime() time.Time {
	return a.PublishedAt
}

func (a *TaskDefinition) ID() string {
	return a.Id
}

func (a *TaskDefinition) Namespace() string {
	return a.Ns
}

func (a *TaskDefinition) RefIDs() []string {
	return nil
}

func (a *TaskDefinition) URL() string {
	return a.URLx
}

func (a *TaskDefinition) Validate() error {
	return nil
}

func (a *TaskDefinition) WithCreatedTime(t time.Time) mycontent.Data {
	a.PublishedAt = t
	return a
}

func (a *TaskDefinition) WithID(id string) mycontent.Data {
	a.Id = id
	return a
}

func (a *TaskDefinition) WithNamespace(id string) mycontent.Data {
	a.Ns = id
	return a
}

func (a *TaskDefinition) WithURL(url string) mycontent.Data {
	a.URLx = url
	return a
}

func (a *TaskDefinition) WithVersion(ver uint64) mycontent.Data {
	return a
}

func (a *TaskDefinition) DGVersion() *uint64 {
	return nil
}
