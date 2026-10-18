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
	"github.com/desain-gratis/common/lib/raft"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
	"github.com/dgraph-io/badger/v4"
)

type Command string
type Event string

const (
	TableTask          = "task_definition"
	TableTaskExecution = "task_execution"

	Command_User_CreateTask                       Command = "user.schedule-task" // TODO: create task
	Command_Leader_InitiateTask                   Command = "leader.initiate-task-execution"
	Command_Leader_UpdateTask                     Command = "leader.update-task"
	Command_Leader_RequestRegenerateTaskExecution Command = "leader.request-regenerate-task-execution"
	Command_Leader_CreateTaskExecution            Command = "leader.task-execution.create"
	Command_Leader_AssignTaskExecution            Command = "leader.task-execution.assign"

	Command_Worker_UpdateTaskExecution  Command = "worker.task-execution.update"
	Command_Worker_RequestWork          Command = "worker.request-work"
	Command_Worker_RequestWorkImmediate Command = "worker.request-work-immediate"
	Command_Worker_Ping                 Command = "worker.ping"
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
type EventWorkerRequestedWork RequestWork

type EventTaskExecutionCreated *entity.TaskExecution

type EventWorkAssigned struct{}

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
	case Command_Worker_RequestWork:
		rw, err := parseAs[RequestWork](cmd.Value)
		if err != nil {
			return nil, err
		}

		rw.SourceNodeID = entry.SourceNodeID

		// broadcast to all worker that this node needs work
		// leader will then process this to allocate task
		m.topic.Broadcast(ctx, EventWorkerRequestedWork(rw))
		return "SUCCESS", nil
	case Command_Leader_AssignTaskExecution:
		// Right now we implement Request -> Give work strategy (first come first serve), later can extend / decouple with many logic..
		// Before submitting it via. raft, we can filter it from worker so no need to send many RequestWork msg

		rw, err := parseAs[AssignTaskExecution](cmd.Value)
		if err != nil {
			return nil, err
		}

		// a worker request some wok
		// for now, let's just give work immediately
		// but later we can provide many strategy depending on worker state/preferences & the task itself (matching)
		// store worker's state & availability & task splitting logic, etc..

		// maybe we only filter pending; we only filter

		// for now, we use "First-Come First-Serve" strategy
		// we should use different storage (not my-content), better used for queue for this
		// to be able to easily filter PENDING task, etc.
		// or filter TIMEOUT tasks from worker, and release it from that worker and assigned to this new worker etc..

		// GET ALL AVAILABLE QUEUE IDs from all healthy nodes

		var countRebalance, countNew int

		// We can have various algorithm here to distribute the tasks into the "healthy" node's queue for later..
		// Right now, we just assign it all to the requesting node.

		for _, queue := range rw.Queue {
			taskExecs, err := m.taskExecutionUsecase.Get(ctx, m.namespace, []string{queue}, "")
			if err != nil {
				return nil, err
			}

			// ping - end

			// todo: in storage layer directly by query (if possible)~
			for _, pendingTaskExec := range taskExecs {
				if pendingTaskExec.Status != entity.ExecutionStatusPending {
					continue
				}

				if pendingTaskExec.State != nil {
					// already have worker there

					if pendingTaskExec.State.AssignedNodeID == entry.SourceNodeID {
						// it's us :) no need to do anything, and just wait for execution
						// since we're sending request work, we're available
						continue
					}

					if _, ok := rw.HealthyUIDs[pendingTaskExec.State.AssignedNodeID]; ok {
						// if it's still healthy no need rebalance
						continue
					}

					countRebalance++
				} else {
					countNew++
				}

				// THEN we do the assignment / re-assignment logic.

				// filter only in the next 1 hour maybe~~~

				pendingTaskExec.State = &entity.TaskExecutionState{
					// AssignedNodeID: entry.SourceNodeID,
					AssignedNodeID: rw.ToNode, // select the first
				}

				// todo: in storage layer directly~
				_, err := m.taskExecutionUsecase.Post(ctx, pendingTaskExec, nil)
				if err != nil {
					return nil, err
				}
			}
		}

		// since we want async later (based on various strategy, we use topic, not direct reply)

		// NOTIFY WORKERS, NEED TO UPDATE ALL OF THEIR IN MEMORY CRON (eg. CREATE / DELETE TIMERS, etc..), ETC
		// if countNew > 0 || countRebalance > 0 {
		// all worker can check the tasks in their end
		m.topic.Broadcast(
			ctx,
			EventWorkAssigned{}, // or all tasks is evaluated by raft; worker can consume their already updated DB
		)
		// }

		return "SUCCESS BRUV", nil
	case Command_Worker_Ping:
		// update node ping

		// TODO: consider to move it outside of the raft?

		rw, err := parseAs[Ping](cmd.Value)
		if err != nil {
			return nil, err
		}
		// todo: maybe use namespacing.. but yeah can be for later

		rw.SourceNodeID = entry.SourceNodeID

		m.topic.Broadcast(ctx, rw)

		return "", nil
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

type RequestWork struct {
	Ping
	QueueIDs []string
	Count    []uint64 // arbitrary unit
}

// Leader assign work to worker
// Worker should use this to notify worker
type AssignTaskExecution struct {
	// todo state

	// HealthNode (as deemed by leader)
	ToNode      uint64
	HealthyUIDs map[uint64]struct{}
	Queue       []string
	Timestamp   time.Time
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

type Ping struct {
	Timestamp    time.Time
	SourceNodeID uint64
	// other status
	RandomNumber int
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
