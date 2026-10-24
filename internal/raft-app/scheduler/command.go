package scheduler

import (
	"time"

	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

type Command string

const (
	// Subscribe to events related to raft app
	Command_Subscribe Command = "subscribe"

	// Create a recurring task
	Command_User_CreateRecurringTask Command = "user.recurring-task.create"

	// Update a recurring task
	Command_Leader_UpdateRecurringTask Command = "leader.recurring-task.update"

	// Generate recurring task's execution
	Command_Leader_GenerateRecurringTaskExecution Command = "leader.recurring-task.generate"

	// Create task execution (for leader & user)
	Command_CreateTaskExecution Command = "task-execution.create"

	// Worker update task execution
	Command_Worker_UpdateTaskExecution Command = "worker.task-execution.update"

	// Worker update availability
	Command_Worker_UpdateAvailability Command = "worker.availability.update"
)

type CreateRecurringTask struct {
	Task       entity.RecurringTask `json:"task_definition"`
	ServerTime time.Time            `json:"server_time"`
}

type UpdateRecurringTask CreateRecurringTask

type RequestRegenerateTaskInstances struct {
	RefIDs []string
	ID     string
}

// TaskExecution Instance
type CreateTaskExecution *entity.TaskExecution
type UpdateTaskExecution *entity.TaskExecution

type UpdateAvailability struct {
	NodeID   uint64
	QueueIDs []string
}

type ReassignNode struct {
	FromNode uint64
	ToNode   uint64
	QueueIDs map[string][]NodeQueuePair
}

type NodeQueuePair struct {
	Queue  *entity.QueueState
	NodeID uint64
}

type TimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type UpdateLastSchedulingTime struct {
	Time time.Time `json:"time"`
}
