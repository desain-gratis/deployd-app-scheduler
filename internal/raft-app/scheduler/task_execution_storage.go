package scheduler

import (
	"context"

	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

type TaskExecutionStorage interface {
	// Post stores the task execution data
	Post(ctx context.Context, te *entity.TaskExecution, meta any) (*entity.TaskExecution, error)

	// Get individual task execution
	GetByID(ctx context.Context, namespace string, queueID, id string) (*entity.TaskExecution, error)

	GetAll(ctx context.Context) error
}
