package genericjob

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/arcward/crong"
	genericjob "github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/generic-job"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
	"github.com/google/uuid"
	"go.etcd.io/raft/v3"
)

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

func (ls *leaderState) generateImmediateTaskExecutionInstances(ctx context.Context, task *entity.Task, serverTime time.Time, raftNode raft.Node) error {
	healthyUIDs := map[uint64]struct{}{}
	var healthyUID uint64

	// only available in leader
	for nodeID, progress := range raftNode.Status().Progress {
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

	_, err := ls.client.Propose(ctx, genericjob.CreateTaskExecution(state))
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
