package genericjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	mycontent_base "github.com/desain-gratis/common/delivery/mycontent-api/mycontent/base"
	content_badger "github.com/desain-gratis/common/delivery/mycontent-api/storage/content/badger"
	"github.com/desain-gratis/common/lib/notifier"
	"github.com/desain-gratis/common/lib/raft"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
	"github.com/dgraph-io/badger/v4"
)

type Command string
type Event string

const (
	TableTaskDefinition = "task_definition"
	TableTaskExecution  = "task_execution"

	Command_User_ScheduleTask Command = "user.schedule-task"

	Command_Leader_InitiateTask Command = "leader.initiate-task-execution"
	Command_Worker_ExecuteTask  Command = "worker.execute-task"
	Command_Worker_UpdateTask   Command = "worker.update-task"
)

var _ raft.ApplicationV2 = (*RaftApp)(nil)

type RaftApp struct {
	topic notifier.Topic

	taskDefinitionUsecase *mycontent_base.Handler[*entity.TaskDefinition]
	taskExecutionUsecase  *mycontent_base.Handler[*entity.TaskExecution]
}

type CommandWrapper struct {
	Name  Command         `json:"name"`
	Value json.RawMessage `json:"value"`
}

type ApplyResult func() (any, error)

var ErrRetryable = errors.New("retryable")

func New(topic notifier.Topic, dbJob *badger.DB) *RaftApp {
	taskDefinitionStorage := content_badger.New(dbJob, TableTaskDefinition, 0)
	taskExecutionStorage := content_badger.NewAutoIncrement(dbJob, TableTaskExecution, 1) // refer to task definition

	taskDefinitionUsecase := mycontent_base.New[*entity.TaskDefinition](taskDefinitionStorage)
	taskExecutionUsecase := mycontent_base.New[*entity.TaskExecution](taskExecutionStorage)

	return &RaftApp{
		topic:                 topic,
		taskDefinitionUsecase: taskDefinitionUsecase,
		taskExecutionUsecase:  taskExecutionUsecase,
	}
}

func (m *RaftApp) GetTaskDefinitionStore() *mycontent_base.Handler[*entity.TaskDefinition] {
	return m.taskDefinitionUsecase
}

func (m *RaftApp) GetTaskExecution() *mycontent_base.Handler[*entity.TaskExecution] {
	return m.taskExecutionUsecase
}

func (m *RaftApp) OnUpdateV2(ctx context.Context, entry raft.EntryV2) (any, error) {
	cmd, err := parseAs[CommandWrapper](entry.Data)
	if err != nil {
		return nil, err
	}

	switch cmd.Name {
	case Command_User_ScheduleTask:
		data, err := parseAs[ScheduleTask](cmd.Value)
		if err != nil {
			return nil, err
		}
		return m.userScheduleTask(ctx, data)
	case Command_Leader_InitiateTask:
		// 1. get task definition and the task execution id that will be executed (if not found we can err)
		// 2. generate next execution id with status "PENDING"
		// 3. broadcast leader_execute_task event for follower node / executor node to get a lock for this task_execution
		data, err := parseAs[InitiateTaskExecution](cmd.Value)
		if err != nil {
			return nil, err
		}
		return m.leaderInitiateTask(ctx, data)
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

type ScheduleTask struct {
	TaskDefinition entity.TaskDefinition `json:"task_definition"`
	ServerTime     time.Time             `json:"server_time"`
}
type InitiateTaskExecution struct {
	ExecutionID    string                `json:"execution_id"`
	TaskDefinition entity.TaskDefinition `json:"task_definition"`
}

func (m *RaftApp) userScheduleTask(ctx context.Context, data ScheduleTask) (any, error) {
	result, err := m.taskDefinitionUsecase.Post(ctx, &data.TaskDefinition, nil)
	if err != nil {
		return nil, err
	}

	m.topic.Broadcast(ctx, result)

	return result, nil
}
func (m *RaftApp) leaderInitiateTask(ctx context.Context, data InitiateTaskExecution) (any, error) {
	dummy := &entity.TaskExecution{Ns: "hello"}
	dummy.Id = data.ExecutionID
	dummy.Ns = data.TaskDefinition.Ns
	dummy.TaskID = data.TaskDefinition.Id
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
