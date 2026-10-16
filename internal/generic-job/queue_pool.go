package genericjob

import (
	"context"
	"log"
	"time"

	genericjob "github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/generic-job"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

func newPooledQueue(ctx context.Context, client *genericjob.Client, count int, fn func(task *entity.TaskExecution) error) chan *entity.TaskExecution {
	// simple workers
	queue := make(chan *entity.TaskExecution)

	go func() {
		defer close(queue)
		<-ctx.Done()
	}()

	for range count {
		go func() {

			// simple executor/worker for sendInvoice~
			for taskExec := range queue {

				runAt := time.Now().In(taskExec.UserTime.Location())
				taskExec.ActualRunAt = &runAt
				taskExec.Status = entity.ExecutionStatusInProgress

				// update before
				_, err := client.Propose(ctx, genericjob.UpdateTaskExecution(taskExec))
				if err != nil {
					log.Printf("ERROR failed to update task execution instance: %v\n", err)
					return
				}

				// FINALLY, ACTUAL FUNCTION TO EXECUTE
				err = fn(taskExec)

				if err != nil {
					taskExec.Status = entity.ExecutionStatusFailed
				} else {
					taskExec.Status = entity.ExecutionStatusDone
				}

				tComplete := time.Now().In(taskExec.UserTime.Location())
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
	}

	return queue
}
