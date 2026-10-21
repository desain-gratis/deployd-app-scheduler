package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	mycontent_base "github.com/desain-gratis/common/delivery/mycontent-api/mycontent/base"
	"github.com/desain-gratis/common/lib/notifier"
	runneretcd "github.com/desain-gratis/common/lib/raft/runner-etcd"
	"github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/scheduler"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

const maxScheduledInstancePerTask = 100

type worker struct {
	topic    notifier.Topic
	toLeader chan string

	namespace string

	taskDefinitionStoreRO *mycontent_base.Handler[*entity.RecurringTask]
	taskExecutionStoreRO  *mycontent_base.Handler[*entity.TaskExecution]

	// task IDs
	scheduled     map[queueTaskPair]*TaskExecutionRuntime
	scheduledLock *sync.Mutex

	queue  map[string]chan *TaskExecutionRuntime
	client *scheduler.Client

	timeBucket        map[timeQueue][]*TaskExecutionRuntime
	timeBucketLock    *sync.Mutex
	assignmentUpdated chan *entity.TaskExecution
}
type queueTaskPair struct {
	queueID string
	TaskID  string
}

// or background process I guess
func NewWorker(
	namespace string,
	topic notifier.Topic,
	client *scheduler.Client,
	taskDefinitionStoreRO *mycontent_base.Handler[*entity.RecurringTask],
	taskExecutionStoreRO *mycontent_base.Handler[*entity.TaskExecution],
	queue map[string]chan *TaskExecutionRuntime,
	assignmentUpdated chan *entity.TaskExecution,
) *worker {
	return &worker{
		topic:                 topic,
		toLeader:              make(chan string),
		taskDefinitionStoreRO: taskDefinitionStoreRO,
		taskExecutionStoreRO:  taskExecutionStoreRO,
		namespace:             namespace,
		scheduled:             make(map[queueTaskPair]*TaskExecutionRuntime),
		scheduledLock:         &sync.Mutex{},
		queue:                 queue,
		client:                client,
		timeBucket:            make(map[timeQueue][]*TaskExecutionRuntime),
		assignmentUpdated:     assignmentUpdated,
		timeBucketLock:        &sync.Mutex{},
	}
}

func (w *worker) Run(ctx context.Context) error {
	allTask, err := w.taskExecutionStoreRO.Get(ctx, w.namespace, nil, "")
	if err != nil {
		return err
	}

	for _, taskExec := range allTask {
		w.scheduleWorkIndvidual(ctx, taskExec, "backfill")
	}

	// raft will block before listened to w.assignmentUpdated channel
	// therefore, we must consume.
	// this also act to make taskExecutionStoreRO consistent.
	go func() {
		for {
			select {
			case <-ctx.Done():
			case taskExec := <-w.assignmentUpdated:
				w.scheduleWorkIndvidual(ctx, taskExec, "live")
			}
		}
	}()

	w.updateAvailability(ctx)

	return nil
}

func (w *worker) updateAvailability(ctx context.Context) {
	log.Printf("worker: updating availability")

	queueList := make([]string, 0, len(w.queue))
	for queueName := range w.queue {
		queueList = append(queueList, queueName)
	}

	_, err := w.client.Propose(ctx, scheduler.UpdateAvailability{
		QueueIDs: queueList, // todo: now hardcoded, latter we get all from list of executors queue
		NodeID:   w.client.ID(),
	})
	if err != nil {
		log.Printf("Error requesting work: %v", err)
		return
	}
}

type TaskExecutionRuntime struct {
	*entity.TaskExecution
	ctx    context.Context
	cancel func()
}

func (t *TaskExecutionRuntime) Cancel() {
	if t == nil {
		return
	}
	t.cancel()
}

func (w *worker) cleanUp(taskExec *TaskExecutionRuntime) {
	taskExec.Cancel()

	if taskExec == nil {
		return
	}

	if taskExec.TaskExecution == nil {
		return
	}

	func() {
		defer w.scheduledLock.Unlock()
		w.scheduledLock.Lock()
		delete(w.scheduled, queueTaskPair{taskExec.QueueID, taskExec.Id})
	}()

	func() {
		defer w.timeBucketLock.Unlock()
		w.timeBucketLock.Lock()

		tq := timeQueue{taskScheduledRunAt: taskExec.ScheduledRunAt, queueID: taskExec.QueueID}
		tasks := w.timeBucket[tq]
		for idx, tr := range tasks {
			if tr.QueueID == taskExec.QueueID && tr.Id == taskExec.Id {
				tasks = append(tasks[:idx], tasks[idx+1:]...)
				w.timeBucket[tq] = tasks
				break
			}
		}

		if len(tasks) == 0 {
			delete(w.timeBucket, tq)
		}
	}()

	// maybe cancel the timer as well
}

func (w *worker) scheduleWorkIndvidual(ctx context.Context, taskExec *entity.TaskExecution, _ string) {
	taskCoord := queueTaskPair{taskExec.QueueID, taskExec.Id}
	prev, ok := w.scheduled[taskCoord]

	// if it's not / no longer pending
	if taskExec.Status != entity.ExecutionStatusPending {
		w.cleanUp(prev)
		return
	}

	// if it's not / no longer assigned
	if taskExec.AssignmentStatus == nil {
		w.cleanUp(prev)
		return
	}

	// if it's (not / no longer) our task
	if taskExec.AssignmentStatus.AssignedTo != w.client.ID() {
		w.cleanUp(prev)
		return
	}

	// if the queue no longer found
	q, ok := w.queue[taskExec.QueueID]
	if !ok {
		w.cleanUp(prev)
		log.Printf("warn: queue %v not found in this node", taskExec.QueueID)
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	taskExecRuntime := &TaskExecutionRuntime{
		TaskExecution: taskExec, ctx: ctx, cancel: cancel,
	}

	// reference for future
	func() {
		defer w.scheduledLock.Unlock()
		w.scheduledLock.Lock()
		w.scheduled[queueTaskPair{taskExec.QueueID, taskExec.Id}] = taskExecRuntime
	}()

	tq := timeQueue{taskExec.ScheduledRunAt, taskExec.QueueID}
	var newTimeBucket bool

	// initialize
	func() {
		defer w.timeBucketLock.Unlock()
		w.timeBucketLock.Lock()

		if _, ok := w.timeBucket[tq]; !ok {
			w.timeBucket[tq] = make([]*TaskExecutionRuntime, 0)
			newTimeBucket = true
		}
		w.timeBucket[tq] = append(w.timeBucket[tq], taskExecRuntime)
	}()

	// log.Printf("worker: got task execution assignment updated event (%v)", mode)

	if newTimeBucket {
		executeAt := time.Until(tq.taskScheduledRunAt)

		// TODO: choose what's best
		if executeAt < -10*time.Second {
			log.Println("worker: warn. stale task will not be executed")
			return
		} else if executeAt < 0 {
			// immediately execued
			executeAt = 0
		}

		time.AfterFunc(executeAt, func() {
			var taskExecs []*TaskExecutionRuntime
			func() {
				defer w.timeBucketLock.Unlock()
				w.timeBucketLock.Lock()
				taskExecs = w.timeBucket[tq]
			}()

			for _, taskExec := range taskExecs {
				if taskExec.ctx.Err() != nil {
					// we will no longer execute
					continue
				}

				q <- taskExec
			}
		})
	}
}

type timeQueue struct {
	taskScheduledRunAt time.Time
	queueID            string
}

// Called everytime becoming raft leader
// Context will be cancelled when stepping down from raft leader
func (w *worker) OnBecomingRaftLeader(ctx context.Context, term uint64, leaderID uint64) error {
	ticker := time.NewTicker(10 * time.Second)

	timers := make(map[string]*time.Timer)
	timersLock := &sync.RWMutex{}

	// Guarantees that we can get the latest state, and all incoming message after that
	rc := runneretcd.GetRaftContext(ctx)
	// Load all task definition from store
	node := rc.Node()

	subscription, err := w.client.Subscribe(ctx)
	if err != nil {
		return err
	}

	ls := leader{
		worker:     w,
		client:     w.client,
		timersLock: timersLock,
		timers:     timers,
		id:         leaderID,
		term:       term,

		// keep track of node health
		node:                  make(map[uint64]*entity.Node),
		healthyDurationWithin: 10 * time.Second,

		ticker:       ticker,
		raftNode:     node,
		subscription: subscription,
	}

	go func(term, leaderID uint64) {
		defer ticker.Stop()
		ls.Handler(ctx, term, leaderID)
	}(term, leaderID)

	return nil
}

// utility to retrieve task from task-ish alias
func getTask(taskish any) *entity.RecurringTask {
	switch task := taskish.(type) {
	case scheduler.EventRecurringTaskCreated:
		return (*entity.RecurringTask)(task)
	case scheduler.EventRecurringTaskUpdated:
		return (*entity.RecurringTask)(task)
	}

	return nil
}
