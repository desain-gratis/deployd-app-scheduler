package genericjob

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/arcward/crong"
	mycontent_base "github.com/desain-gratis/common/delivery/mycontent-api/mycontent/base"
	"github.com/desain-gratis/common/lib/notifier"
	runneretcd "github.com/desain-gratis/common/lib/raft/runner-etcd"
	genericjob "github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/generic-job"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
	"github.com/google/uuid"
)

const maxScheduledInstancePerTask = 100

type worker struct {
	topic    notifier.Topic
	toLeader chan string

	namespace string

	taskDefinitionStoreRO *mycontent_base.Handler[*entity.Task]
	taskExecutionStoreRO  *mycontent_base.Handler[*entity.TaskExecution]

	// task IDs
	scheduled     map[queueTaskPair]struct{}
	scheduledLock *sync.Mutex
}
type queueTaskPair struct {
	queueID string
	TaskID  string
}

// or background process I guess
func NewWorker(
	namespace string,
	topic notifier.Topic,
	taskDefinitionStoreRO *mycontent_base.Handler[*entity.Task],
	taskExecutionStoreRO *mycontent_base.Handler[*entity.TaskExecution],
) *worker {
	return &worker{
		topic:                 topic,
		toLeader:              make(chan string),
		taskDefinitionStoreRO: taskDefinitionStoreRO,
		taskExecutionStoreRO:  taskExecutionStoreRO,
		namespace:             namespace,
		scheduled:             map[queueTaskPair]struct{}{},
		scheduledLock:         &sync.Mutex{},
	}
}

func (w *worker) Run(ctx context.Context, client *genericjob.Client) error {
	queue := make(map[string]chan *entity.TaskExecution)
	queue["sendInvoice()"] = newPooledQueue(ctx, client, 30, sendInvoice)

	// TODO: have "workerState" per queue; (eg how much they can take more work, or should stop UpdateAvailability etc..)

	// each queue have their "request work" loop;
	// we can add back-off / backpressure logic here later~
	// right now, we're just using simple ticker, and it will always ask for more work

	// Guarantees that we can get the latest state, and all incoming message after that
	subscription, err := client.Subscribe(ctx)
	if err != nil {
		return err
	}

	// We create schedule in memory using the latest state
	w.scheduleWork(ctx, client, queue)

	w.updateAvailability(ctx, client)

	go func() {
		// Start listening
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-subscription.Listen():
				switch msg.(type) {
				case genericjob.EventTaskExecutionUpdated:
					// If there is an adjustment in the assignment, we do work here
					log.Println("worker: got task execution updated event")
					w.scheduleWork(ctx, client, queue)
				}
			}
		}
	}()

	return nil
}

func (w *worker) updateAvailability(ctx context.Context, client *genericjob.Client) {
	log.Printf("worker: updating availability")

	// request a tasks to leader if worker/executor ready for a particular queue;
	// advertising how much resource we have, geolocation, etc~
	// a "lock" for the next 1 hour etc..
	// in ourcase, the worker is ready for sendInvoice()
	// can use the "workerState"

	var countWork int
	func() {
		defer w.scheduledLock.Unlock()
		w.scheduledLock.Lock()
		countWork = len(w.scheduled) // TODO: per queue
	}()

	_, err := client.Propose(ctx, genericjob.UpdateAvailability{
		QueueIDs: []string{"sendInvoice()"}, // todo: now hardcoded, latter we get all from list of executors queue
		Count:    []uint64{uint64(countWork)},
		NodeID:   client.ID(),
	})
	if err != nil {
		log.Printf("Error requesting work: %v", err)
		return
	}
}

// Schedule work according to our "local" state
// We should Propose to raft firt, to get the very latest state.
func (w *worker) scheduleWork(ctx context.Context, client *genericjob.Client, queue map[string]chan *entity.TaskExecution) {
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

			if taskExec.State == nil {
				// this task should not be run, since not yet assigned by leader
				continue
			}

			if taskExec.State.AssignedNodeID != client.ID() {
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

			tq := timeQueue{taskExec.ScheduledRunAt, taskExec.QueueID, taskExec.State.AssignedNodeID}
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
		q, ok := queue[tg.queueID]
		if !ok {
			// queue not found
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

		//  && executeAt >= -evt.ExecToleranceDuration
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

func (m *leaderState) getNode(nodeID uint64) *entity.Node {
	_, ok := m.node[nodeID]
	if !ok {
		m.node[nodeID] = &entity.Node{QueueIDs: make(map[string]*entity.QueueState)}
	}

	return m.node[nodeID]
}

func (m *leaderState) getNodeQueueState(nodeID uint64, queueID string) *entity.QueueState {
	_, ok := m.node[nodeID]
	if !ok {
		m.node[nodeID] = &entity.Node{QueueIDs: make(map[string]*entity.QueueState)}
	}

	_, ok = m.node[nodeID].QueueIDs[queueID]
	if !ok {
		m.node[nodeID].QueueIDs[queueID] = &entity.QueueState{}
	}

	return m.node[nodeID].QueueIDs[queueID]
}

type timeQueue struct {
	taskScheduledRunAt time.Time
	queueID            string
	nodeID             uint64
}

// Called everytime becoming raft leader
// Context will be cancelled when stepping down from raft leader
func (w *worker) OnBecomingRaftLeader(ctx context.Context, term uint64, leaderID uint64) error {

	// return nil
	//TODO:

	// Guarantees that we can get the latest state, and all incoming message after that
	rc := runneretcd.GetRaftContext(ctx)
	// Load all task definition from store
	node := rc.Node()
	client := genericjob.NewClient(rc)
	subscription, err := client.Subscribe(ctx)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(10 * time.Second)

	timers := make(map[string]*time.Timer)
	timersLock := &sync.RWMutex{}

	ls := leaderState{
		client:     client,
		timersLock: timersLock,
		timers:     timers,
		id:         leaderID,
		term:       term,

		// keep track of node health
		node:                  make(map[uint64]*entity.Node),
		healthyDurationWithin: 10 * time.Second,
	}

	// request the leader (us) to send ALL task as genericjob.EventTaskNeedRegenerate event in raft
	// to make it more consistent
	// _, err = ls.client.Propose(ctx, genericjob.RequestRegenerateTaskInstances{RefIDs: nil, ID: ""})
	// if err != nil {
	// 	return nil
	// }

	ls.client.ID()

	go func(term, leaderID uint64) {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				healthyUIDs := map[uint64]struct{}{}
				var healthyUID uint64

				// only available in leader
				for nodeID, progress := range node.Status().Progress {
					if progress.RecentActive {
						healthyUIDs[nodeID] = struct{}{}
						healthyUID = nodeID // TODO: for now just select the last
					}
				}

				if len(healthyUIDs) == 0 {
					log.Printf("leader: danger! no healthy node")
					continue
				}

				log.Printf("leader: healthy uids %v", healthyUIDs)

				taskExecs, err := w.taskExecutionStoreRO.Stream(ctx, "deployd", nil, "")
				if err != nil {
					log.Printf("leader: failed to read task execution store %v", err)
					continue
				}

				var notifyWorker bool

				for taskExec := range taskExecs {
					if taskExec.State != nil {
						if _, ok := healthyUIDs[taskExec.State.AssignedNodeID]; ok {
							continue
						}
						// notice we don't do "upscale" if healthy node increase :)
						// maybe manual rebalance later :)
					}

					taskExec.State = &entity.TaskExecutionState{
						AssignedNodeID: healthyUID,
					}

					_, err := client.Propose(ctx, genericjob.UpdateTaskExecution(taskExec))
					if err != nil {
						// determien error, maybe panic or maybe just fail
						log.Printf("failed to (re)assign work: %v", err)
					}
					notifyWorker = true
				}

				if notifyWorker {
					log.Printf("leader: notifying workers that there is an update in task execution table")
					// worker will be ready to take them from it's own DB
					_, err := client.Propose(ctx, genericjob.NotifyTaskUpdated{})
					if err != nil {
						log.Printf("failed to (re)assign work: %v", err)
					}
				} else {
					log.Printf("leader: all good in the scheduler world")
				}
			case msg := <-subscription.Listen():
				t := time.Now()

				switch evt := msg.(type) {
				case genericjob.EventTaskCreated, genericjob.EventTaskNeedRegenerate:
					log.Printf("leader: a new task definition has come. I better schedule! %+v", evt)
					task := getTask(evt)
					if task == nil {
						// TODO: error inside the raft app, can check which causes empty event
						continue
					}

					if task.ExecutionStrategy != entity.ExecutionStrategyScheduled {
						log.Printf("[warn] unsupported execution strategy: %v", task.ExecutionStrategy)
						continue
					}

					err := ls.generateScheduledTaskExecutionInstances(ctx, task, t)
					if err != nil {
						log.Printf("ERR SCHEDULE %v", err)
						continue
					}
				case genericjob.EventTaskUpdated:
					// task updated! we don't do anything.

				case genericjob.EventWorkerRequestedWork:
					// worker request a work

					// update the "worker pool"
					for idx, q := range evt.QueueIDs {
						capacity := evt.Count[idx]
						queue := ls.getNodeQueueState(evt.NodeID, q)
						queue.Capacity = capacity
					}

				default:
					// log.Printf("Unknown job evt")
				}
			case <-ctx.Done():
				return
			}
		}
	}(term, leaderID)

	return nil
}

// utility to retrieve task from task-ish alias
func getTask(taskish any) *entity.Task {
	switch task := taskish.(type) {
	case genericjob.EventTaskCreated:
		return (*entity.Task)(task)
	case genericjob.EventTaskUpdated:
		return (*entity.Task)(task)
	}

	return nil
}

type leaderState struct {
	client     *genericjob.Client
	timersLock *sync.RWMutex
	timers     map[string]*time.Timer
	id         uint64
	term       uint64

	node map[uint64]*entity.Node

	// config
	healthyDurationWithin time.Duration
}

func (ls *leaderState) cancelAllScheduledExecutions(ctx context.Context, task *entity.Task) error {
	// TODO:
	// maybe the cron definition changed, or manually cancelled (I don't want to run this tasks.)
	// only if it's already in memory ofcourse
	return nil
}

// schedule the task that is already in memory.
// currentTime usually same as t0
func (ls *leaderState) generateScheduledTaskExecutionInstances(ctx context.Context, task *entity.Task, serverTime time.Time) error {

	// TODO SUPER: right now we implement the scheduled task first.
	// need to switch case based on strategy later ~

	if task == nil {
		return fmt.Errorf("empty task definition")
	}

	if task.State != nil && task.State.LastGenerateEnd.After(serverTime.Add(-1*time.Minute)) { // arbitrary delta 1 minute
		// no need to regenerate.
		// just create the next trigger when to generate
		err := ls.scheduleNextGenerate(ctx, time.Until(task.State.LastGenerateEnd), task)
		if err != nil {
			return err
		}

		return nil
	}

	// generate between last generate end, and server time's next one hour

	serverTime = serverTime.Truncate(1 * time.Second)

	t0 := serverTime
	t1 := serverTime.AddDate(1, 0, 0) // GENERATE FOR FULL YEAR (PLANNED)

	loc := task.UserTime.Location() // schedule relative to Task's location / time zone
	schedule, err := crong.New(task.ExecutionStrategyInput.Scheduled.Cron, loc)
	if err != nil {
		return err
	}

	count := 0
	execTimes := make([]time.Time, 0)

	lastGeneratedEnd := t0

	// generate for one full year.., but limited to max task only if it's exceed one hour (since we need to schedule soon)

	for next := schedule.Next(t0); next.Before(t1) && (count < maxScheduledInstancePerTask || next.Before(t0.Add(1*time.Hour))); next = schedule.Next(next) {
		execTimes = append(execTimes, next)
		count++
		execID := uuid.NewString()

		status := entity.ExecutionStatusPending
		if !next.After(serverTime) {
			status = entity.ExecutionStatusMissed // missed execution, we can ask user how to handle this using different strategies
		}

		state := &entity.TaskExecution{
			Ns:             task.Ns,
			Id:             execID,
			TaskID:         task.Id,
			QueueID:        task.QueueID,
			ScheduledRunAt: next,
			Status:         status,
			PublishedAt:    serverTime,
			Input:          task.Input,
			UserTime:       task.UserTime,
		}

		_, err := ls.client.Propose(ctx, genericjob.CreateTaskExecution(state))
		if err != nil {
			log.Printf("ERROR create task execution instance: %v\n", err)
			continue
		}

		lastGeneratedEnd = t1
	}

	log.Printf("Scheduled %v task execution instance(s): %v. Updating the task.", count, execTimes)

	// Update the task definition state
	task.State = &entity.TaskState{
		LastGenerateStart: t0,
		LastGenerateEnd:   lastGeneratedEnd,
	}

	result, err := ls.client.Propose(ctx, genericjob.UpdateTask{
		Task:       *task,
		ServerTime: serverTime,
	})
	if err != nil {
		log.Printf("ERROR update task %v", err)
		return err
	}

	err = ls.scheduleNextGenerate(ctx, time.Until(t1), task)
	if err != nil {
		return err
	}

	log.Printf("Task scheduled: %+v", result)

	return nil
}

func (ls *leaderState) scheduleNextGenerate(ctx context.Context, t time.Duration, task *entity.Task) error {
	// Schedule for next generate trigger for this task
	_ = time.AfterFunc(t, func() {
		// to trigger the tick
		_, err := ls.client.Propose(ctx, genericjob.RequestRegenerateTaskInstances{RefIDs: task.RefIDs(), ID: task.ID()})
		if err != nil {
			log.Printf("Error scheduling next generate time")
			return
		}
	})

	return nil
}

func SampleTask(ctx context.Context, execID, taskId string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	log.Printf(". \"%v\" (%v) is now running!", execID, taskId)

	return nil
}
