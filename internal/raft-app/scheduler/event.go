package scheduler

import (
	"time"

	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

type Event string

type EventRecurringTaskCreated *entity.RecurringTask
type EventRecurringTaskUpdated *entity.RecurringTask
type EventRecurringTaskNeedRegenerate *entity.RecurringTask
type EventWorkerRequestedWork UpdateAvailability

type EventTaskExecutionCreated *entity.TaskExecution

type EventTaskExecutionAssignmentUpdated struct{}

type EventWorkCreatedRecurringTask struct {
	ScheduledRunAt time.Time
	Queue          string
	Tasks          []*entity.TaskExecution
}
