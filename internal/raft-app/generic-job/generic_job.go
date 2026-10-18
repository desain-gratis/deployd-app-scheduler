package genericjob

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
	TableTask          = "task_definition"
	TableTaskExecution = "task_execution"

	Command_Subscribe Command = "subscribe" // subscribe after TODO: move it to dedicated raft app

	Command_User_CreateTask                       Command = "user.schedule-task" // TODO: create task
	Command_Leader_InitiateTask                   Command = "leader.initiate-task-execution"
	Command_Leader_UpdateTask                     Command = "leader.update-task"
	Command_Leader_RequestRegenerateTaskExecution Command = "leader.request-regenerate-task-execution"
	Command_Leader_CreateTaskExecution            Command = "leader.task-execution.create"
	Command_Leader_NotifyTaskUpdated              Command = "leader.task-execution.assign"

	Command_Worker_UpdateTaskExecution         Command = "worker.task-execution.update"
	Command_Worker_UpdateAvailability          Command = "worker.request-work"
	Command_Worker_UpdateAvailabilityImmediate Command = "worker.request-work-immediate"
	// When leader started, it will submit this command
	Command_Leader_UpdateLastSchedulingTime Command = "leader.update-last-scheduling-time"

	// When task is triggered
	Command_Worker_ExecuteTask Command = "worker.execute-task"
	Command_Worker_UpdateTask  Command = "worker.update-task"
)

var _ raft.ApplicationV2 = (*RaftApp)(nil)
var _ raft.ApplicationV2WithSnapshot = (*RaftApp)(nil)

type RaftApp struct {
	namespace string
	topic     notifier.Topic

	taskDefinitionUsecase *mycontent_base.Handler[*entity.Task]
	taskExecutionUsecase  *mycontent_base.Handler[*entity.TaskExecution]

	// temporary var just for testing
	// since this is in memory, will resets after restart
	scheduledTaskCount int
}

type CommandWrapper struct {
	Name  Command         `json:"name"`
	Value json.RawMessage `json:"value"`
}

type ApplyResult func() (any, error)

type EventTaskCreated *entity.Task
type EventTaskUpdated *entity.Task
type EventTaskNeedRegenerate *entity.Task
type EventWorkerRequestedWork UpdateAvailability

type EventTaskExecutionCreated *entity.TaskExecution

type EventTaskExecutionUpdated struct{}

type EventWorkCreatedScheduledTask struct {
	ScheduledRunAt time.Time
	Queue          string
	Tasks          []*entity.TaskExecution
}

var ErrRetryable = errors.New("retryable")

func New(namespace string, topic notifier.Topic, dbJob *badger.DB) *RaftApp {
	taskDefinitionStorage := content_badger.New(dbJob, TableTask, 0)
	taskExecutionStorage := content_badger.NewAutoIncrement(dbJob, TableTaskExecution, 1) // refer to task definition

	taskDefinitionUsecase := mycontent_base.New[*entity.Task](taskDefinitionStorage)
	taskExecutionUsecase := mycontent_base.New[*entity.TaskExecution](taskExecutionStorage)

	return &RaftApp{
		namespace:             namespace,
		topic:                 topic,
		taskDefinitionUsecase: taskDefinitionUsecase,
		taskExecutionUsecase:  taskExecutionUsecase,
	}
}

// todo all make read only.. for the exposed reader
func (m *RaftApp) GetTaskStore() *mycontent_base.Handler[*entity.Task] {
	return m.taskDefinitionUsecase
}

func (m *RaftApp) GetTaskExecutionStore() *mycontent_base.Handler[*entity.TaskExecution] {
	return m.taskExecutionUsecase
}

func (m *RaftApp) WriteSnapshot(writer io.Writer) (int, error) {
	return fmt.Fprintf(writer, "\n\nTHIS IS MY SNAPSHOT~ %v\n\n", time.Now())
}

func (m *RaftApp) OnUpdateV2(ctx context.Context, entry raft.EntryV2) (any, error) {
	cmd, err := parseAs[CommandWrapper](entry.Data)
	if err != nil {
		return nil, err
	}
	switch cmd.Name {

	case Command_Subscribe:
		// TODO: move this to it's own dedicated raft-app
		subs, err := m.topic.Subscribe(
			context.Background(),
			notifier_impl.NewStandardSubscriber(nil),
		)
		if err != nil {
			return nil, err
		}
		subs.Start()

		return subs, nil
	case Command_User_CreateTask:
		data, err := parseAs[CreateTask](cmd.Value)
		if err != nil {
			return nil, err
		}
		return m.userCreateTask(ctx, data, entry.Index, "create")
	case Command_Leader_UpdateTask:
		data, err := parseAs[UpdateTask](cmd.Value)
		if err != nil {
			return nil, err
		}
		return m.userCreateTask(ctx, CreateTask(data), entry.Index, "update")
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
			return nil, fmt.Errorf("task does not exist")
		}

		// TODO: change detection / delta

		// simple check terminal status
		switch t.Status {
		case entity.ExecutionStatusDone, entity.ExecutionStatusFailed:
			// m.getNodeQueueState(entry.SourceNodeID, t.QueueID).TaskAssigned--
		}

		result, err := m.taskExecutionUsecase.Post(ctx, t, nil)
		if err != nil {
			return nil, err
		}

		// TODO: if changes detected, what is it, and publish respective event.

		return result, nil
	case Command_Leader_InitiateTask:
		// 1. get task definition and the task execution id that will be executed (if not found we can err)
		// 2. generate next execution id with status "PENDING"
		// 3. broadcast leader_execute_task event for follower node / executor node to get a lock for this task_execution
		data, err := parseAs[InitiateTaskExecution](cmd.Value)
		if err != nil {
			return nil, err
		}
		return m.leaderInitiateTask(ctx, data)
	case Command_Leader_RequestRegenerateTaskExecution:
		data, err := parseAs[RequestRegenerateTaskInstances](cmd.Value)
		if err != nil {
			return nil, err
		}

		ch, err := m.taskDefinitionUsecase.Stream(ctx, m.namespace, data.RefIDs, data.ID)
		if err != nil {
			return nil, err
		}

		for task := range ch {
			m.topic.Broadcast(ctx, EventTaskNeedRegenerate(task))
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
	case Command_Leader_NotifyTaskUpdated:
		// NOTIFY WORKERS, NEED TO UPDATE ALL OF THEIR IN MEMORY CRON (eg. CREATE / DELETE TIMERS, etc..), ETC
		m.topic.Broadcast(
			ctx,
			EventTaskExecutionUpdated{}, // or all tasks is evaluated by raft; worker can consume their already updated DB
		)

		return "SUCCESS BRUV", nil
	case Command_Worker_ExecuteTask:
		// 1. each worker race to take the lease; one winning or any other algorithm, it can start to run the task
	case Command_Worker_UpdateTask:
		// 1. Update task status & struct according to the job; also task completion checks, timeout checks, etc happened here.
		// 2. Broadcast task execution update event
	default:
		return nil, fmt.Errorf("%w command: %s", errors.ErrUnsupported, cmd.Name)
	}

	return nil, fmt.Errorf("%w command: %s", errors.ErrUnsupported, cmd.Name)
}

type CreateTask struct {
	Task       entity.Task `json:"task_definition"`
	ServerTime time.Time   `json:"server_time"`
}

type UpdateTask CreateTask

type RequestRegenerateTaskInstances struct {
	RefIDs []string
	ID     string
}

type InitiateTaskExecution struct {
	Task entity.Task `json:"task_definition"`

	// generated by leader
	ExecutionID  string               `json:"execution_id"`
	InitialState entity.TaskExecution `json:"initial_state"`
}

// TaskExecution Instance
type CreateTaskExecution *entity.TaskExecution
type UpdateTaskExecution *entity.TaskExecution

type UpdateAvailability struct {
	NodeID   uint64
	QueueIDs []string
	Count    []uint64 // arbitrary unit
}

// Notifies worker that task execution tables have been updated
// Worker should update their in-memory schedule
type NotifyTaskUpdated struct{}

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

func (m *RaftApp) userCreateTask(ctx context.Context, data CreateTask, idx uint64, mode string) (any, error) {
	// THIS ONE JUST TO PLAY WITH RAFT

	// overwrite ID // just for testing
	m.scheduledTaskCount++
	// data.Task.Id = fmt.Sprintf("%d", m.scheduledTaskCount)

	result, err := m.taskDefinitionUsecase.Post(ctx, &data.Task, nil)
	if err != nil {
		return nil, err
	}

	switch mode {
	case "create": // TODO: use enums
		m.topic.Broadcast(ctx, EventTaskCreated(result))
	case "update":
		m.topic.Broadcast(ctx, EventTaskUpdated(result))
	}

	return result, nil
}
func (m *RaftApp) leaderInitiateTask(ctx context.Context, data InitiateTaskExecution) (any, error) {
	dummy := &entity.TaskExecution{Ns: "hello"}
	dummy.Id = data.ExecutionID
	dummy.Ns = data.Task.Ns
	dummy.QueueID = data.Task.Id
	dummy.Status = entity.ExecutionStatusPending
	result, err := m.taskExecutionUsecase.Post(ctx, dummy, nil)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func parseAs[T any](payload json.RawMessage) (T, error) {
	var t T
	err := json.Unmarshal(payload, &t)
	return t, err
}
