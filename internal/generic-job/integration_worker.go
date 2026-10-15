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
	notifier_impl "github.com/desain-gratis/common/lib/notifier/impl"
	runneretcd "github.com/desain-gratis/common/lib/raft/runner-etcd"
	genericjob "github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/generic-job"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
	"github.com/google/uuid"
)

const maxScheduledInstancePerTask = 100

type worker struct {
	topic    notifier.Topic
	toLeader chan string

	taskDefinitionStoreRO *mycontent_base.Handler[*entity.Task]
	taskExecutionStoreRO  *mycontent_base.Handler[*entity.TaskExecution]
}

// or background process I guess
func NewWorker(
	topic notifier.Topic,
	taskDefinitionStoreRO *mycontent_base.Handler[*entity.Task],
	taskExecutionStoreRO *mycontent_base.Handler[*entity.TaskExecution],
) *worker {
	return &worker{
		topic:                 topic,
		toLeader:              make(chan string),
		taskDefinitionStoreRO: taskDefinitionStoreRO,
		taskExecutionStoreRO:  taskExecutionStoreRO,
	}
}

func (w *worker) Run(ctx context.Context, client *genericjob.Client) error {
	subscriber, err := w.topic.Subscribe(ctx, notifier_impl.NewStandardSubscriber(nil))
	if err != nil {
		return err
	}

	subscriber.Start()

	// simple workers
	sendInvoiceQueue := make(chan *entity.TaskExecution)
	go func() {

		sendInvoice := func(taskExec *entity.TaskExecution) error {
			log.Printf("The task sendInvoice(): %v is running!", taskExec.Id)
			return nil
		}

		// simple executor/worker for sendInvoice~
		for taskExec := range sendInvoiceQueue {

			// SAMPLE LOGIC FOR DOING TASK
			runAt := time.Now()
			taskExec.ActualRunAt = &runAt
			taskExec.Status = entity.ExecutionStatusInProgress

			// update before
			_, err := client.Propose(ctx, genericjob.UpdateTaskExecution(taskExec))
			if err != nil {
				log.Printf("ERROR failed to update task execution instance: %v\n", err)
				return
			}

			// FINALLY, ACTUAL FUNCTION TO EXECUTE
			err = sendInvoice(taskExec)

			if err != nil {
				taskExec.Status = entity.ExecutionStatusFailed
			} else {
				taskExec.Status = entity.ExecutionStatusDone
			}

			tComplete := time.Now()
			taskExec.CompletedAt = &tComplete

			// update after
			_, err = client.Propose(ctx, genericjob.UpdateTaskExecution(taskExec))
			if err != nil {
				// failed to inform cluster back. Leader's timeout mechanism will kick in.
				log.Printf("ERROR failed to update task execution instance: %v\n", err)
				return
			}

		}
	}()

	queue := make(map[string]chan *entity.TaskExecution)
	queue["sendInvoice()"] = sendInvoiceQueue

	ticker := time.NewTicker(1 * time.Second)
	workerID := uuid.NewString()

	// TODO: have "workerState" per queue; (eg how much they can take more work, or should stop RequestWork etc..)

	go func() {
		for {
			select {
			case <-ticker.C:
				// request a tasks to leader if worker/executor ready for a particular queue;
				// advertising how much resource we have, geolocation, etc~
				// a "lock" for the next 1 hour etc..
				// in ourcase, the worker is ready for sendInvoice()
				// can use the "workerState"
				_, err := client.Propose(ctx, genericjob.RequestWork{
					Namespace: "deployd", // namespace of the queue, should be the same as the queue used in the tasks
					WorkerID:  workerID,
					QueueID:   "sendInvoice()", // todo: the value dynamic depending on worker's capacity
					Count:     1000,
				})
				if err != nil {
					log.Printf("Error requesting work: %v", err)
					continue
				}

			case msg := <-subscriber.Listen():
				// t := time.Now()

				switch evt := msg.(type) {
				case genericjob.EventWorkAssigned:
					if evt.WorkerID != workerID {
						// not our work!
						continue
					}

					// TODO: every worker client should persist the work inside their own local storage / DB
					// to keep track.
					// TODO: Client programming language, maybe interact via network/websocket.
					// for now we keep it simple, we just use the in process golang worker.
					// TODO: worker type, registered to server (for work assignment logic/strategy)

					q, ok := queue[evt.QueueID]
					if !ok {
						// i don't want / not supposed to do this kind of work
						// i'm not advertisindg this?? but how do i get?
						continue
					}

					// group by time
					tgs := make(map[time.Time][]*entity.TaskExecution)

					for _, taskExec := range evt.TaskExecs {
						if _, ok := tgs[taskExec.ScheduledRunAt]; !ok {
							tgs[taskExec.ScheduledRunAt] = make([]*entity.TaskExecution, 0)
						}
						tgs[taskExec.ScheduledRunAt] = append(tgs[taskExec.ScheduledRunAt], taskExec)
					}

					for tg := range tgs {
						tasks := tgs[tg]
						tm := time.AfterFunc(time.Until(tg), func() {
							for _, scheduledTask := range tasks {
								q <- scheduledTask
							}
						})
						// TODO: handle this for cancellation (in the future hehe)
						_ = tm
					}

				case genericjob.EventTaskExecutionCreated:
					//
				}
			}
		}
	}()

	return nil
}

// Called everytime becoming raft leader
// Context will be cancelled when stepping down from raft leader
func (w *worker) OnBecomingRaftLeader(ctx context.Context, term uint64, leaderID uint64) error {

	// return nil
	//TODO:

	subscriber, err := w.topic.Subscribe(ctx, notifier_impl.NewStandardSubscriber(nil))
	if err != nil {
		return err
	}
	subscriber.Start()

	// Load all task definition from store
	rc := runneretcd.GetRaftContext(ctx)
	client := genericjob.NewClient(rc)

	ticker := time.NewTicker(15 * time.Second)

	timers := make(map[string]*time.Timer)
	timersLock := &sync.RWMutex{}

	ls := leaderState{
		client:     client,
		timersLock: timersLock,
		timers:     timers,
		id:         leaderID,
		term:       term,
	}

	// request the leader (us) to send ALL task as genericjob.EventTaskNeedRegenerate event in raft
	// to make it more consistent
	_, err = ls.client.Propose(ctx, genericjob.RequestRegenerateTaskInstances{Namespace: "*", RefIDs: nil, ID: ""})
	if err != nil {
		return nil
	}

	go func(term, leaderID uint64) {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// log.Printf("I'm ZA LEADER. IM POLLING FOR JOBSS SCHEDULE! term=%v leaderID=%v", term, leaderID)
			case msg := <-subscriber.Listen():
				t := time.Now()

				switch evt := msg.(type) {
				case genericjob.EventTaskCreated, genericjob.EventTaskNeedRegenerate:
					log.Printf("A new task definition has come. I better schedule! %+v", evt)
					task := getTask(evt)
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
		_, err := ls.client.Propose(ctx, genericjob.RequestRegenerateTaskInstances{Namespace: task.Namespace(), RefIDs: task.RefIDs(), ID: task.ID()})
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
