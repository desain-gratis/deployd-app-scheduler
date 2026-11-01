package scheduler

import (
	"context"
	"time"

	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

// TODO: maybe no need interface, intentional high-coupling with clickhouse
type TaskExecutionStorage interface {
	// Post stores the task execution data
	Post(ctx context.Context, te *entity.TaskExecution, meta any) (*entity.TaskExecution, error)

	// Get individual task execution
	GetByID(ctx context.Context, namespace string, queueID, id string) (*entity.TaskExecution, error)

	// Get all task
	GetAll(ctx context.Context, indexes ...string) (chan *entity.TaskExecution, chan error)

	// Get all task with specified filter
	GetAllWithFilter(
		ctx context.Context,
		filter TaskExecutionFilter,
		userLocation string,
		indexes ...string,
	) (chan *entity.TaskExecution, chan error)

	// Get only pending tasks
	GetPendingTasksCount(ctx context.Context, assignedID uint64, before time.Time) (int, error)

	// Get aggregated count based on day
	GetDailyCount(
		ctx context.Context,
		filter TaskExecutionFilter,
		userLocation string,
		indexes ...string,
	) (chan *TaskExecutionDailyCount, chan error)

	GetHourlyCount(
		ctx context.Context,
		filter TaskExecutionFilter,
		userLocation string,
		indexes ...string,
	) (chan *TaskExecutionHourlyCount, chan error)
}

type TaskExecutionFilter struct {
	Before           *time.Time
	Date             *time.Time
	StartDate        *time.Time
	EndDate          *time.Time
	AssignmentStatus *entity.AssignmentStatus
	ExecutionStatus  []entity.ExecutionStatus
	AssignedNode     []uint64
}

type TaskExecutionDailyCount struct {
	Day     string `json:"day"` // YYYY-MM-DD in UserLocation
	QueueID string `json:"queue_id"`
	Count   uint64 `json:"count"`
}

type TaskExecutionHourlyCount struct {
	Hour    string `json:"hour"` // YYYY-MM-DD HH:00:00
	QueueID string `json:"queue_id"`
	Count   uint64 `json:"count"`
}
