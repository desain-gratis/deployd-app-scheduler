package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/desain-gratis/common/lib/notifier"
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

	// todo: refactor
	regenerate := make(chan *entity.RecurringTask)

	// todo pt in function
	allRecurringTasks, err := ls.taskDefinitionStoreRO.Get(ctx, ls.namespace, nil, "")
	if err != nil {
		return
	}
	for _, rt := range allRecurringTasks {
		if rt.State == nil {
			// warning!
			continue
		}

		_ = time.AfterFunc(time.Until(rt.State.LastGenerateEnd), func() {
			if ctx.Err() != nil {
				return
			}

			regenerate <- rt
		})
	}

	for {
		select {
		case t := <-ls.ticker.C:
			// check tasks that has empty assignment / unhealthy node
			err := ls.periodicCheck(ctx, t)
			if err != nil {
				log.Printf("leader: error during periodic check: %v", err)
			}

		case task := <-regenerate:
			updatedTaskAny, err := ls.client.Propose(ctx, scheduler.RequestRegenerateTaskInstances{
				Task:       task,
				ServerTime: time.Now(),
			})
			if err != nil {
				log.Printf("leader: err creating scheduled task execution instances %v", err)
				continue
			}

			updatedTask, ok := updatedTaskAny.(*entity.RecurringTask)
			if !ok {
				log.Printf("leader: not a recurring task")
			}

			_ = time.AfterFunc(time.Until(updatedTask.State.LastGenerateEnd), func() {
				if ctx.Err() != nil {
					return
				}

				regenerate <- updatedTask
			})

		case msg := <-ls.subscription.Listen():
			t := time.Now()

			switch evt := msg.(type) {
			case scheduler.EventRecurringTaskCreated, scheduler.EventRecurringTaskNeedRegenerate:
				log.Printf("leader: a new task definition has come. I better schedule! %+v", evt)
				task := getTask(evt)
				if task == nil {
					// TODO: error inside the raft app, can check which causes empty event
					log.Printf("leader: lah task nya nil")
					continue
				}

				go func() {
					regenerate <- task
				}()

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

	result, err := ls.client.Propose(ctx, scheduler.UpdateTaskExecution(taskExec))
	if err != nil {
		// determien error, maybe panic or maybe just fail
		log.Printf("failed to (re)assign work: %v", err)
	}

	_ = result
}
