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

const (
	TableRecurringTask = "recurring_task"
	TableTaskExecution = "task_execution"
)

var _ raft.ApplicationV2 = (*scheduler)(nil)
var _ raft.ApplicationV2WithSnapshot = (*scheduler)(nil)

type scheduler struct {
	namespace string
	topic     notifier.Topic

	scheduledTaskUsecase *mycontent_base.Handler[*entity.RecurringTask]
	taskExecutionUsecase *mycontent_base.Handler[*entity.TaskExecution]

	executor chan *entity.TaskExecution

	// experiment use chDB
	taskExecutionUsecaseV2 TaskExecutionStorage
}

type CommandWrapper struct {
	Name  Command         `json:"name"`
	Value json.RawMessage `json:"value"`
}

type ApplyResult func() (any, error)

var ErrRetryable = errors.New("retryable")

func New(namespace string, topic notifier.Topic, dbJob *badger.DB, teStorageV2 TaskExecutionStorage, executor chan *entity.TaskExecution) *scheduler {
	scheduledTaskStorage := content_badger.New(dbJob, TableRecurringTask, 0)
	taskExecutionStorage := content_badger.NewAutoIncrement(dbJob, TableTaskExecution, 1) // refer to queue

	scheduledTaskUsecase := mycontent_base.New[*entity.RecurringTask](scheduledTaskStorage)
	taskExecutionUsecase := mycontent_base.New[*entity.TaskExecution](taskExecutionStorage)

	return &scheduler{
		namespace:              namespace,
		topic:                  topic,
		scheduledTaskUsecase:   scheduledTaskUsecase,
		taskExecutionUsecase:   taskExecutionUsecase,
		executor:               executor,
		taskExecutionUsecaseV2: teStorageV2,
	}
}

// todo all make read only.. for the exposed reader
func (m *scheduler) GetRecurringTaskStore() *mycontent_base.Handler[*entity.RecurringTask] {
	return m.scheduledTaskUsecase
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
	case Command_Leader_UpdateRecurringTask:
		// Update the scheduled task
		data, err := parseAs[UpdateRecurringTask](cmd.Value)
		if err != nil {
			return nil, err
		}
		return m.userCreateTask(ctx, CreateRecurringTask(data), "update")

	case Command_CreateTaskExecution:
		data, err := parseAs[CreateTaskExecution](cmd.Value)
		if err != nil {
			return nil, err
		}

		t := (*entity.TaskExecution)(data)

		result, err := m.taskExecutionUsecaseV2.Post(ctx, t, metadata{EventID: entry.Index})
		if err != nil {
			return nil, err
		}

		m.topic.Broadcast(ctx, EventTaskExecutionCreated(data))

		return result, nil
	case Command_Worker_UpdateTaskExecution:
		data, err := parseAs[UpdateTaskExecution](cmd.Value)
		if err != nil {
			return nil, err
		}

		t := (*entity.TaskExecution)(data)

		// experimental chDB
		getData, err := m.taskExecutionUsecaseV2.GetByID(ctx, t.Ns, t.QueueID, t.Id)
		if err != nil {
			return nil, err
		}
		if getData == nil {
			return nil, fmt.Errorf("task execution does not exist")
		}

		result, err := m.taskExecutionUsecaseV2.Post(ctx, t, metadata{EventID: entry.Index})
		if err != nil {
			return nil, err
		}

		m.executor <- result

		return result, nil

	case Command_Leader_GenerateRecurringTaskExecution:
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

	default:
		return nil, fmt.Errorf("%w command: %s", errors.ErrUnsupported, cmd.Name)
	}
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
