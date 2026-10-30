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

type leader struct {
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
func (ls *leader) Handler(ctx context.Context, term, leaderID uint64) {
	_ = ls.periodicCheck(ctx, time.Now())
	for {
		select {
		case t := <-ls.ticker.C:
			// check tasks that has empty assignment / unhealthy node
			err := ls.periodicCheck(ctx, t)
			if err != nil {
				log.Printf("leader: error during periodic check: %v", err)
			}

		case msg := <-ls.subscription.Listen():
			t := time.Now()

			switch evt := msg.(type) {
			case scheduler.EventRecurringTaskCreated, scheduler.EventRecurringTaskNeedRegenerate:
				log.Printf("leader: a new task definition has come. I better schedule! %+v", evt)
				task := getTask(evt)
				if task == nil {
					// TODO: error inside the raft app, can check which causes empty event
					continue
				}

				err := ls.generateInstances(ctx, task, t)
				if err != nil {
					log.Printf("leader: err creating scheduled task execution instances %v", err)
					continue
				}
			case scheduler.EventTaskExecutionCreated:
				taskExec := (*entity.TaskExecution)(evt)

				healthyUIDs := map[uint64]struct{}{}
				var anyHealthyUID uint64

				// only available in leader
				for nodeID, progress := range ls.raftNode.Status().Progress {
					if progress.RecentActive {
						healthyUIDs[nodeID] = struct{}{}
						anyHealthyUID = nodeID // TODO: for now just select the last
					}
				}

				// whenever new task coming, we attempt to assign it to worker
				ls.assignPendingTask(ctx, anyHealthyUID, taskExec, t)
			default:
				// log.Printf("Unknown job evt")
			}
		case <-ctx.Done():
			return
		}
	}
}

// check for unhealthy and if found, re-assign task execution, notify worker
func (ls *leader) periodicCheck(ctx context.Context, t0 time.Time) error {
	healthyUIDs := map[uint64]struct{}{}
	unhealthyUIDs := make([]uint64, 0)
	var healthyUID uint64

	// only available in leader
	for nodeID, progress := range ls.raftNode.Status().Progress {
		if progress.RecentActive {
			healthyUIDs[nodeID] = struct{}{}
			healthyUID = nodeID // TODO: for now just select the last
		} else {
			unhealthyUIDs = append(unhealthyUIDs, nodeID)
		}
	}

	if len(healthyUIDs) == 0 {
		return fmt.Errorf("leader: no healthy node")
	}

	_ = ls.assignPendingTasks(ctx, healthyUID, t0)
	_ = ls.reassignTasksFromUnhealthyNode(ctx, healthyUID, unhealthyUIDs, t0) // can be in different periodic tick

	return nil
}

// Assign pending tasks to a worker.
// Move pending task to another worker if detected unhealthy.
func (ls *leader) assignPendingTasks(ctx context.Context, anyHealthyUID uint64, t0 time.Time) error {
	// This is totally separate with worker code; we can use different value
	// todo: replace the hardcoded value
	before := t0.Add(3 * time.Hour)
	notAssigned := entity.AssignmentStatusPending
	filter := scheduler.TaskExecutionFilter{
		Before:           &before,
		ExecutionStatus:  []entity.ExecutionStatus{entity.ExecutionStatusPending},
		AssignmentStatus: &notAssigned,
	}

	taskCh, errCh := ls.teStorageV2.GetAllWithFilter(ctx, filter, ls.namespace)
	for taskCh != nil || errCh != nil {
		select {
		case task, ok := <-taskCh:
			if !ok {
				taskCh = nil
				continue
			}
			ls.assignPendingTask(ctx, anyHealthyUID, task, t0)
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}

			if err != nil {
				log.Println("GetAll error:", err)
			}
		}
	}

	return nil
}

func (ls *leader) reassignTasksFromUnhealthyNode(ctx context.Context, anyHealthyUID uint64, unhealthyUIDs []uint64, t0 time.Time) error {
	if len(unhealthyUIDs) == 0 {
		// no error
		return nil
	}

	// This is totally separate with worker code. we can use different value
	// todo: replace the hardcoded value
	before := t0.Add(3 * time.Hour)
	assigned := entity.AssignmentStatusAssigned
	filter := scheduler.TaskExecutionFilter{
		Before:           &before,
		ExecutionStatus:  []entity.ExecutionStatus{entity.ExecutionStatusPending}, // Maybe check also the one in progress
		AssignmentStatus: &assigned,
		AssignedNode:     unhealthyUIDs,
	}

	taskCh, errCh := ls.teStorageV2.GetAllWithFilter(ctx, filter, ls.namespace)
	for taskCh != nil || errCh != nil {
		select {
		case task, ok := <-taskCh:
			if !ok {
				taskCh = nil
				continue
			}

			ls.assignPendingTask(ctx, anyHealthyUID, task, t0)
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}

			if err != nil {
				log.Println("GetAll error:", err)
			}
		}
	}

	return nil
}

// assignPendingTask
// it can be called immediately during live request, or polled
func (ls *leader) assignPendingTask(ctx context.Context, anyHealthyUID uint64, taskExec *entity.TaskExecution, t time.Time) {
	// safeguard
	// todo: maybe responsibility of the caller
	if taskExec.Status != entity.ExecutionStatusPending {
		return
	}

	// if it's not immediate, we can assign to worker later
	if taskExec.ExecutionMode != entity.TaskExecutionModeImmediate && taskExec.ScheduledRunAt.After(t.Add(3*time.Hour)) {
		return
	}

	// in case of re-assignment, it's possible taskExec already have previous assignment
	if taskExec.AssignmentState == nil {
		taskExec.AssignmentState = &entity.AssignmentState{}
	}

	taskExec.AssignmentState.AssignedTo = anyHealthyUID
	taskExec.AssignmentState.AssignedBy = ls.client.ID()
	taskExec.AssignmentState.AssignmentTime = t
	taskExec.AssignmentState.AssignmentCount++
	taskExec.AssignmentStatus = entity.AssignmentStatusAssigned

	_, err := ls.client.Propose(ctx, scheduler.UpdateTaskExecution(taskExec))
	if err != nil {
		// determien error, maybe panic or maybe just fail
		log.Printf("failed to (re)assign work: %v", err)
	}
}

// schedule the task that is already in memory.
// currentTime usually same as t0
func (ls *leader) generateInstances(ctx context.Context, task *entity.RecurringTask, serverTime time.Time) error {

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
	schedule, err := crong.New(task.Cron, loc)
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
			Ns:               task.Ns,
			Id:               execID,
			QueueID:          task.QueueID,
			ScheduledRunAt:   next,
			Status:           status,
			PublishedAt:      serverTime.In(task.UserTime.Location()),
			Input:            task.Input,
			UserTime:         task.UserTime,
			ExecutionMode:    entity.TaskExecutionModeRecurring,
			AssignmentStatus: entity.AssignmentStatusPending,
			ExecutionState:   nil,
			RecurringTaskID:  &task.Id,
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
	task.State = &entity.RecurringTaskState{
		LastGenerateStart: t0,
		LastGenerateEnd:   lastGeneratedEnd,
	}

	result, err := ls.client.Propose(ctx, scheduler.UpdateRecurringTask{
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

func (ls *leader) scheduleNextGenerate(ctx context.Context, t time.Duration, task *entity.RecurringTask) error {
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
