package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	mycontent_base "github.com/desain-gratis/common/delivery/mycontent-api/mycontent/base"
	content_badger "github.com/desain-gratis/common/delivery/mycontent-api/storage/content/badger"
	"github.com/desain-gratis/common/lib/notifier"
	notifier_impl "github.com/desain-gratis/common/lib/notifier/impl"
	"github.com/desain-gratis/common/lib/raft"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
	"github.com/dgraph-io/badger/v4"
)

type Command string
type Event string

const (
	TableRecurringTask = "scheduled_task_definition"
	TableTaskExecution = "task_execution"

	// Subscribe to events related to raft app
	Command_Subscribe Command = "subscribe"

	// Create a scheduled task
	Command_User_CreateRecurringTask Command = "user.recurring-task.create"

	// Update a scheduled task
	Command_Leader_UpdateTask Command = "leader.update-task"

	Command_Leader_RequestRegenerateTaskExecution       Command = "leader.request-regenerate-task-execution"
	Command_Leader_CreateTaskExecution                  Command = "leader.task-execution.create"
	Command_Leader_NotifyTaskExecutionAssignmentUpdated Command = "leader.task-execution.notify-assignment-updated"

	Command_Worker_UpdateTaskExecution         Command = "worker.task-execution.update"
	Command_Worker_UpdateAvailability          Command = "worker.request-work"
	Command_Worker_UpdateAvailabilityImmediate Command = "worker.request-work-immediate"
	// When leader started, it will submit this command
	Command_Leader_UpdateLastSchedulingTime Command = "leader.update-last-scheduling-time"

	// When task is triggered
	Command_Worker_ExecuteTask Command = "worker.execute-task"
	Command_Worker_UpdateTask  Command = "worker.update-task"
)

var _ raft.ApplicationV2 = (*scheduler)(nil)
var _ raft.ApplicationV2WithSnapshot = (*scheduler)(nil)

type scheduler struct {
	namespace string
	topic     notifier.Topic

	scheduledTaskUsecase *mycontent_base.Handler[*entity.RecurringTask]
	taskExecutionUsecase *mycontent_base.Handler[*entity.TaskExecution]
}

type CommandWrapper struct {
	Name  Command         `json:"name"`
	Value json.RawMessage `json:"value"`
}

type ApplyResult func() (any, error)

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

var ErrRetryable = errors.New("retryable")

func New(namespace string, topic notifier.Topic, dbJob *badger.DB) *scheduler {
	scheduledTaskStorage := content_badger.New(dbJob, TableRecurringTask, 0)
	taskExecutionStorage := content_badger.NewAutoIncrement(dbJob, TableTaskExecution, 1) // refer to queue

	scheduledTaskUsecase := mycontent_base.New[*entity.RecurringTask](scheduledTaskStorage)
	taskExecutionUsecase := mycontent_base.New[*entity.TaskExecution](taskExecutionStorage)

	return &scheduler{
		namespace:            namespace,
		topic:                topic,
		scheduledTaskUsecase: scheduledTaskUsecase,
		taskExecutionUsecase: taskExecutionUsecase,
	}
}

// todo all make read only.. for the exposed reader
func (m *scheduler) GetRecurringTaskStore() *mycontent_base.Handler[*entity.RecurringTask] {
	return m.scheduledTaskUsecase
}

func (m *scheduler) GetTaskExecutionStore() *mycontent_base.Handler[*entity.TaskExecution] {
	return m.taskExecutionUsecase
}

func (m *scheduler) WriteSnapshot(writer io.Writer) (int, error) {
	return fmt.Fprintf(writer, "\n\nTHIS IS MY SNAPSHOT~ %v\n\n", time.Now())
}

func (m *scheduler) OnUpdateV2(ctx context.Context, entry raft.EntryV2) (any, error) {
	cmd, err := parseAs[CommandWrapper](entry.Data)
	if err != nil {
		return nil, err
	}
	switch cmd.Name {

	case Command_Subscribe:
		// Subscribe to raft events in the scheduler
		subs, err := m.topic.Subscribe(
			context.Background(),
			notifier_impl.NewStandardSubscriber(nil),
		)
		if err != nil {
			return nil, err
		}
		subs.Start()

		return subs, nil
	case Command_User_CreateRecurringTask:
		// Create a scheduled task
		data, err := parseAs[CreateRecurringTask](cmd.Value)
		if err != nil {
			return nil, err
		}
		return m.userCreateTask(ctx, data, "create")
	case Command_Leader_UpdateTask:
		// Update the scheduled task
		data, err := parseAs[UpdateRecurringTask](cmd.Value)
		if err != nil {
			return nil, err
		}
		return m.userCreateTask(ctx, CreateRecurringTask(data), "update")

	case Command_Leader_CreateTaskExecution:
		data, err := parseAs[CreateTaskExecution](cmd.Value)
		if err != nil {
			return nil, err
		}

		t := (*entity.TaskExecution)(data)
		result, err := m.taskExecutionUsecase.Post(ctx, t, nil)
		if err != nil {
			return nil, err
		}

		m.topic.Broadcast(ctx, EventTaskExecutionCreated(result))

		return result, nil
	case Command_Worker_UpdateTaskExecution:
		data, err := parseAs[UpdateTaskExecution](cmd.Value)
		if err != nil {
			return nil, err
		}

		t := (*entity.TaskExecution)(data)
		getData, err := m.taskExecutionUsecase.Get(ctx, t.Namespace(), t.RefIDs(), t.ID()) // simple check if it's found or not
		if err != nil {
			return nil, err
		}
		if len(getData) != 1 {
			return nil, fmt.Errorf("task execution does not exist")
		}

		result, err := m.taskExecutionUsecase.Post(ctx, t, nil)
		if err != nil {
			return nil, err
		}

		return result, nil

	case Command_Leader_RequestRegenerateTaskExecution:
		data, err := parseAs[RequestRegenerateTaskInstances](cmd.Value)
		if err != nil {
			return nil, err
		}

		ch, err := m.scheduledTaskUsecase.Stream(ctx, m.namespace, data.RefIDs, data.ID)
		if err != nil {
			return nil, err
		}

		for task := range ch {
			m.topic.Broadcast(ctx, EventRecurringTaskNeedRegenerate(task))
		}

		return "SUCCESS", nil
	case Command_Worker_UpdateAvailability:
		rw, err := parseAs[UpdateAvailability](cmd.Value)
		if err != nil {
			return nil, err
		}

		rw.NodeID = entry.SourceNodeID

		// broadcast to all worker that this node needs work
		// leader will then process this to allocate task
		m.topic.Broadcast(ctx, EventWorkerRequestedWork(rw))
		return "SUCCESS", nil
	case Command_Leader_NotifyTaskExecutionAssignmentUpdated:
		// NOTIFY WORKERS, NEED TO UPDATE ALL OF THEIR IN MEMORY CRON (eg. CREATE / DELETE TIMERS, etc..), ETC
		m.topic.Broadcast(
			ctx,
			EventTaskExecutionAssignmentUpdated{}, // or all tasks is evaluated by raft; worker can consume their already updated DB
		)

		return "SUCCESS", nil

	default:
		return nil, fmt.Errorf("%w command: %s", errors.ErrUnsupported, cmd.Name)
	}
}

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

// Notifies worker that task execution tables have been updated
// Worker should update their in-memory schedule
type NotifyTaskExecutionAssignmentUpdated struct{}

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

func (m *scheduler) userCreateTask(ctx context.Context, data CreateRecurringTask, mode string) (any, error) {
	// TODO: check previous, and check their execution instances
	result, err := m.scheduledTaskUsecase.Post(ctx, &data.Task, nil)
	if err != nil {
		return nil, err
	}

	switch mode {
	case "create": // TODO: use enums
		m.topic.Broadcast(ctx, EventRecurringTaskCreated(result))
	case "update":
		m.topic.Broadcast(ctx, EventRecurringTaskUpdated(result))
	}

	return result, nil
}

func parseAs[T any](payload json.RawMessage) (T, error) {
	var t T
	err := json.Unmarshal(payload, &t)
	return t, err
}
