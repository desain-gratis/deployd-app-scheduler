package scheduler

import (
	"context"
	"time"

	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

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
		indexes ...string,
	) (chan *entity.TaskExecution, chan error)

	// Get only pending tasks
	GetPendingTasksCount(ctx context.Context, assignedID uint64, before time.Time) (int, error)
}
