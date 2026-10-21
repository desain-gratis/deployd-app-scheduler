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
	scheduled     map[queueTaskPair]struct{}
	scheduledLock *sync.Mutex

	queue  map[string]chan *entity.TaskExecution
	client *scheduler.Client
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
	queue map[string]chan *entity.TaskExecution,
) *worker {
	return &worker{
		topic:                 topic,
		toLeader:              make(chan string),
		taskDefinitionStoreRO: taskDefinitionStoreRO,
		taskExecutionStoreRO:  taskExecutionStoreRO,
		namespace:             namespace,
		scheduled:             map[queueTaskPair]struct{}{},
		scheduledLock:         &sync.Mutex{},
		queue:                 queue,
		client:                client,
	}
}

func (w *worker) Run(ctx context.Context) error {
	// TODO: have "workerState" per queue; (eg how much they can take more work, or should stop UpdateAvailability etc..)

	// each queue have their "request work" loop;
	// we can add back-off / backpressure logic here later~
	// right now, we're just using simple ticker, and it will always ask for more work

	// Guarantees that we can get the latest state, and all incoming message after that
	subscription, err := w.client.Subscribe(ctx)
	if err != nil {
		return err
	}

	// We create schedule in memory using the latest state
	w.scheduleWork(ctx)
	w.updateAvailability(ctx)

	go func() {
		// Start listening
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-subscription.Listen():
				switch evt := msg.(type) {
				case scheduler.EventTaskExecutionAssignmentUpdated:
					// If there is an adjustment in the assignment, we do work here
					log.Println("worker: got task execution assignment updated event")
					w.scheduleWork(ctx)
				case scheduler.EventTaskExecutionCreated:
					// TODO: refactor2
					if evt.AssignmentState == nil || evt.AssignmentState.AssignedTo != w.client.ID() {
						continue
					}

					log.Println("worker: got immediate task")

					q, ok := w.queue[evt.QueueID]
					if !ok {
						// queue not found
						log.Printf("worker: queue %v not found", evt.QueueID)
						continue
					}

					q <- evt
				}
			}
		}
	}()

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

// Schedule work according to our "local" state
// We should Propose to raft firt, to get the very latest state.
func (w *worker) scheduleWork(ctx context.Context) {
	tgs := make(map[timeQueue][]*entity.TaskExecution)

	for _, queue := range []string{"sendInvoice()"} {
		taskOfThisQueue, err := w.taskExecutionStoreRO.Get(ctx, w.namespace, []string{queue}, "")
		if err != nil {
			log.Printf("Failed to load task execution instance (queue=%v): %v", queue, err)
			continue
		}

		var newCount int

		// Iterate all of our task..
		for _, taskExec := range taskOfThisQueue {
			if taskExec.Status != entity.ExecutionStatusPending {
				continue
			}

			if taskExec.AssignmentState == nil {
				// this task should not be run, since not yet assigned by leader
				continue
			}

			if taskExec.AssignmentState.AssignedTo != w.client.ID() {
				// don't schedule other people task :)
				continue
			}

			// book keep / idempotency
			// todo: lock
			if _, ok := w.scheduled[queueTaskPair{queue, taskExec.Id}]; ok {
				// already scheduled (has cron)
				continue
			}

			func() {
				defer w.scheduledLock.Unlock()
				w.scheduledLock.Lock()
				w.scheduled[queueTaskPair{queue, taskExec.Id}] = struct{}{}
			}()

			newCount++

			tq := timeQueue{taskExec.ScheduledRunAt, taskExec.QueueID, taskExec.AssignmentState.AssignedTo}
			if _, ok := tgs[tq]; !ok {
				tgs[tq] = make([]*entity.TaskExecution, 0)
			}
			tgs[tq] = append(tgs[tq], taskExec)
		}
	}

	// TODO: every worker client should persist the work inside their own local storage / DB
	// to keep track.
	// TODO: Client programming language, maybe interact via network/websocket.
	// for now we keep it simple, we just use the in process golang worker.
	// TODO: worker type, registered to server (for work assignment logic/strategy)

	for tg := range tgs {
		q, ok := w.queue[tg.queueID]
		if !ok {
			// queue not found
			log.Printf("warn: queue %v not found", tg.queueID)
			continue
		}

		tasks := tgs[tg]

		// we can execute all old task..
		// or handles it according to different strategy..
		// or simply marked it as ExecutionStatusMissed

		executeAt := time.Until(tg.taskScheduledRunAt)

		time.AfterFunc(executeAt, func() {
			// we can re-query here, instead of using "tasks" (to reduce memory)
			// after we move to chDB
			for _, taskExec := range tasks {
				q <- taskExec
			}
			for _, taskExec := range tasks {
				func() {
					defer w.scheduledLock.Unlock()
					w.scheduledLock.Lock()
					delete(w.scheduled, queueTaskPair{tg.queueID, taskExec.Id})
				}()
			}
		})

		if executeAt <= 0 {
			go func() {
				// to avoid many goroutine, use UpdateAvailability's backpressure mechanism.
				for _, scheduledTask := range tasks {
					q <- scheduledTask
				}
				for _, taskExec := range tasks {
					func() {
						defer w.scheduledLock.Unlock()
						w.scheduledLock.Lock()
						delete(w.scheduled, queueTaskPair{tg.queueID, taskExec.Id})
					}()
				}
			}()
		}
	}
}

type timeQueue struct {
	taskScheduledRunAt time.Time
	queueID            string
	nodeID             uint64
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
