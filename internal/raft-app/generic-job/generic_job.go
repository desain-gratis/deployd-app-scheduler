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

	Command_Leader_CreateTaskExecution Command = "leader.task-execution.create"
	Command_Worker_UpdateTaskExecution Command = "worker.task-execution.update"
	Command_Worker_RequestWork         Command = "worker.request-work"
	Command_Worker_Ping                Command = "worker.ping"
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

	node                  map[uint64]*node
	healthyDurationWithin time.Duration
}

type node struct {
	// last ping of this node
	LastPing time.Time
	QueueIDs map[string]*QueueState
}

type QueueState struct {
	TaskAssigned uint64
}

type CommandWrapper struct {
	Name  Command         `json:"name"`
	Value json.RawMessage `json:"value"`
}

type ApplyResult func() (any, error)

type EventTaskCreated *entity.Task
type EventTaskUpdated *entity.Task
type EventTaskNeedRegenerate *entity.Task

type EventTaskExecutionCreated *entity.TaskExecution

type EventWorkAssigned struct {
	AssignedNodeID        uint64
	TaskExecs             []*entity.TaskExecution
	ExecToleranceDuration time.Duration
	CountRebalance        int
}

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
		node:                  make(map[uint64]*node),
		healthyDurationWithin: 15 * time.Second,
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
			m.getNodeQueueState(entry.SourceNodeID, t.QueueID).TaskAssigned--
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
		// Right now we implement Request -> Give work strategy (first come first serve), later can extend / decouple with many logic..
		// Before submitting it via. raft, we can filter it from worker so no need to send many RequestWork msg

		rw, err := parseAs[RequestWork](cmd.Value)
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

		filtered := make([]*entity.TaskExecution, 0)

		var countRebalance int

		// check node health
		healthy := make(map[uint64]struct{})

		// by default, the work requester is healthy
		m.getNode(entry.SourceNodeID).LastPing = rw.Timestamp

		for nodeID, node := range m.node {
			if rw.Timestamp.Sub(node.LastPing) <= m.healthyDurationWithin {
				healthy[nodeID] = struct{}{}
			} else {
				// log.Printf("I'm detecting that this node: %v is not healthy", nodeID)
			}
		}

		// We can have various algorithm here to distribute the tasks into the "healthy" node's queue for later..
		// Right now, we just assign it all to the requesting node.

		for _, queue := range rw.QueueIDs {
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

					// continue

					// check if that worker is active!
					// and last ping is recent
					if _, ok := healthy[pendingTaskExec.State.AssignedNodeID]; ok {
						continue
					}

					countRebalance++
				}

				// filter only in the next 1 hour maybe~~~

				pendingTaskExec.State = &entity.TaskExecutionState{
					AssignedNodeID: entry.SourceNodeID,
				}

				// todo: in storage layer directly~
				pte, err := m.taskExecutionUsecase.Post(ctx, pendingTaskExec, nil)
				if err != nil {
					return nil, err
				}

				filtered = append(filtered, pte)

				m.getNodeQueueState(entry.SourceNodeID, queue).TaskAssigned++
			}
		}

		// since we want async later (based on various strategy, we use topic, not direct reply)

		m.topic.Broadcast(
			ctx,
			EventWorkAssigned{
				AssignedNodeID:        entry.SourceNodeID,
				TaskExecs:             filtered,
				ExecToleranceDuration: m.healthyDurationWithin,
				CountRebalance:        countRebalance,
			},
		)

		return "SUCCESS BRUV", nil
	case Command_Worker_Ping:
		// update node ping

		// TODO: consider to move it outside of the raft?

		rw, err := parseAs[Ping](cmd.Value)
		if err != nil {
			return nil, err
		}
		// todo: maybe use namespacing.. but yeah can be for later

		m.getNode(entry.SourceNodeID).LastPing = rw.Timestamp

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

func (m *RaftApp) getNode(nodeID uint64) *node {
	_, ok := m.node[nodeID]
	if !ok {
		m.node[nodeID] = &node{QueueIDs: make(map[string]*QueueState)}
	}

	return m.node[nodeID]
}

func (m *RaftApp) getNodeQueueState(nodeID uint64, queueID string) *QueueState {
	_, ok := m.node[nodeID]
	if !ok {
		m.node[nodeID] = &node{QueueIDs: make(map[string]*QueueState)}
	}

	_, ok = m.node[nodeID].QueueIDs[queueID]
	if !ok {
		m.node[nodeID].QueueIDs[queueID] = &QueueState{}
	}

	return m.node[nodeID].QueueIDs[queueID]
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

type Ping struct {
	Timestamp time.Time
	// other status
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
