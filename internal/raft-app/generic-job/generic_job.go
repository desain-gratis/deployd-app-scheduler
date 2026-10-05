package genericjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	Name  Command `json:"name"`
	Value []byte  `json:"value"`
}

type ApplyResult func() (any, error)

var ErrRetryable = errors.New("retryable")

func New(topic notifier.Topic, dbJob *badger.DB) *RaftApp {
	taskDefinitionStorage := content_badger.NewAutoIncrement(dbJob, TableTaskDefinition, 0)
	taskExecutionStorage := content_badger.NewAutoIncrement(dbJob, TableTaskExecution, 1) // refer to task definition

	taskDefinitionUsecase := mycontent_base.New[*entity.TaskDefinition](taskDefinitionStorage)
	taskExecutionUsecase := mycontent_base.New[*entity.TaskExecution](taskExecutionStorage)

	return &RaftApp{
		topic:                 topic,
		taskDefinitionUsecase: taskDefinitionUsecase,
		taskExecutionUsecase:  taskExecutionUsecase,
	}
}

func OnLeader(ctx context.Context, term uint64, leaderID uint64) error {
	ticker := time.NewTicker(1 * time.Second)

	// Remember, this is outside of the state machine
	go func(term, leaderID uint64) {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf("I'm ZA LEADER. IM POLLING FOR JOBSS SCHEDULE! term=%v leaderID=%v", term, leaderID)
			case <-ctx.Done():
				return
			}
		}
	}(term, leaderID)

	return nil
}

func (m *RaftApp) GetTaskDefinitionStore() *mycontent_base.Handler[*entity.TaskDefinition] {
	return m.taskDefinitionUsecase
}

func (m *RaftApp) GetTaskExecution() *mycontent_base.Handler[*entity.TaskExecution] {
	return m.taskExecutionUsecase
}

type CommandScheduleTask struct {
	Cron   string `json:"cron"`
	TaskID string `json:"task_id"`
}

func (m *RaftApp) OnUpdateV2(ctx context.Context, entry raft.EntryV2) (any, error) {
	cmd, err := parseAs[CommandWrapper](entry.Data)
	if err != nil {
		return nil, err
	}

	switch cmd.Name {
	case Command_User_ScheduleTask:
		// 1. Of course write to table "task_definition"
		// 2. Parse cron & get next scheduled execution time; write to "task_execution" table, with status "PENDING"
	case Command_Leader_InitiateTask:
		// 1. get task definition and the task execution id that will be executed (if not found we can err)
		// 2. generate next execution id with status "PENDING"
		// 3. broadcast leader_execute_task event for follower node / executor node to get a lock for this task_execution
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

func parseAs[T any](payload []byte) (T, error) {
	var t T
	err := json.Unmarshal(payload, &t)
	return t, err
}
