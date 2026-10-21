package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/arcward/crong"
	"github.com/desain-gratis/common/lib/notifier"
	"github.com/google/uuid"
	"go.etcd.io/raft/v3"

	"github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/scheduler"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

type leaderState struct {
	*worker
	client     *scheduler.Client
	timersLock *sync.RWMutex
	timers     map[string]*time.Timer
	id         uint64
	term       uint64

	node   map[uint64]*entity.Node
	ticker *time.Ticker

	// config
	healthyDurationWithin time.Duration

	raftNode     raft.Node
	subscription notifier.Subscription
}

// Main loop for leader
func (ls *leaderState) Handler(ctx context.Context, term, leaderID uint64) {
	for {
		select {
		// Check unhealthy, if so rebalance Recurring / pending tasks
		case <-ls.ticker.C:
			healthyUIDs := map[uint64]struct{}{}
			var healthyUID uint64

			// only available in leader
			for nodeID, progress := range ls.raftNode.Status().Progress {
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

			taskExecs, err := ls.worker.taskExecutionStoreRO.Stream(ctx, "deployd", nil, "")
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

				// TODO: use batch inside the raft to make it more efficient
				// but it needed the allocation algorithm to be deterministic
				// (eg. it will be the same accross all nodes)
				_, err := ls.client.Propose(ctx, scheduler.UpdateTaskExecution(taskExec))
				if err != nil {
					// determien error, maybe panic or maybe just fail
					log.Printf("failed to (re)assign work: %v", err)
				}
				notifyWorker = true
			}

			if notifyWorker {
				log.Printf("leader: notifying workers that there is an update in task execution table")
				// worker will be ready to take them from it's own DB
				_, err := ls.client.Propose(ctx, scheduler.NotifyTaskUpdated{})
				if err != nil {
					log.Printf("failed to (re)assign work: %v", err)
				}
			} else {
				log.Printf("leader: all good in the scheduler world")
			}
		case msg := <-ls.subscription.Listen():
			t := time.Now()

			switch evt := msg.(type) {
			case scheduler.EventTaskCreated, scheduler.EventTaskNeedRegenerate:
				log.Printf("leader: a new task definition has come. I better schedule! %+v", evt)
				task := getTask(evt)
				if task == nil {
					// TODO: error inside the raft app, can check which causes empty event
					continue
				}

				switch task.ExecutionStrategy {
				case entity.ExecutionStrategyImmediate:
					err := ls.generateImmediateTaskExecutionInstances(ctx, task, t)
					if err != nil {
						log.Printf("leader: err creating immediate task execution instances %v", err)
						continue
					}

				case entity.ExecutionStrategyScheduled:
					err := ls.generateScheduledTaskExecutionInstances(ctx, task, t)
					if err != nil {
						log.Printf("leader: err creating scheduled task execution instances %v", err)
						continue
					}
				default:
					log.Printf("[warn] unsupported execution strategy: %v", task.ExecutionStrategy)
				}

			case scheduler.EventTaskUpdated:
				// task updated! we don't do anything.

			case scheduler.EventWorkerRequestedWork:
				// worker request a work

			default:
				// log.Printf("Unknown job evt")
			}
		case <-ctx.Done():
			return
		}
	}
}

func (ls *leaderState) cancelAllScheduledExecutions(ctx context.Context, task *entity.Task) error {
	// TODO:
	// maybe the cron definition changed, or manually cancelled (I don't want to run this tasks.)
	// only if it's already in memory ofcourse
	return nil
}

func (ls *leaderState) generateImmediateTaskExecutionInstances(ctx context.Context, task *entity.Task, serverTime time.Time) error {
	healthyUIDs := map[uint64]struct{}{}
	var healthyUID uint64

	// only available in leader
	for nodeID, progress := range ls.raftNode.Status().Progress {
		if progress.RecentActive {
			healthyUIDs[nodeID] = struct{}{}
			healthyUID = nodeID // TODO: for now just select the last
		}
	}

	if len(healthyUIDs) == 0 {
		return fmt.Errorf("leader: node not available")
	}

	state := &entity.TaskExecution{
		Ns:             task.Ns,
		Id:             uuid.NewString(),
		TaskID:         task.Id,
		QueueID:        task.QueueID,
		ScheduledRunAt: time.Now(),
		Status:         entity.ExecutionStatusPending,
		PublishedAt:    serverTime,
		Input:          task.Input,
		UserTime:       task.UserTime,
		State: &entity.TaskExecutionState{
			AssignedNodeID: healthyUID, // assign executor node immediately
		},
	}

	_, err := ls.client.Propose(ctx, scheduler.CreateTaskExecution(state))
	if err != nil {
		log.Printf("ERROR create task execution instance: %v\n", err)
		return err
	}

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

		_, err := ls.client.Propose(ctx, scheduler.CreateTaskExecution(state))
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

	result, err := ls.client.Propose(ctx, scheduler.UpdateTask{
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
		_, err := ls.client.Propose(ctx, scheduler.RequestRegenerateTaskInstances{RefIDs: task.RefIDs(), ID: task.ID()})
		if err != nil {
			log.Printf("Error scheduling next generate time")
			return
		}
	})

	return nil
}
