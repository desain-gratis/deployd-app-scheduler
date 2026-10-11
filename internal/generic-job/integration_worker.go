package genericjob

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/arcward/crong"
	"github.com/desain-gratis/common/lib/notifier"
	notifier_impl "github.com/desain-gratis/common/lib/notifier/impl"
	runneretcd "github.com/desain-gratis/common/lib/raft/runner-etcd"
	genericjob "github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/generic-job"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
	"github.com/google/uuid"
)

type worker struct {
	topic    notifier.Topic
	toLeader chan string
}

// or background process I guess
func NewWorker(topic notifier.Topic) *worker {
	return &worker{
		topic:    topic,
		toLeader: make(chan string),
	}
}

func (w *worker) Run(ctx context.Context) error {
	subs, err := w.topic.Subscribe(ctx, notifier_impl.NewStandardSubscriber(nil))
	if err != nil {
		return err
	}

	subs.Start()

	go func() {
		for msg := range subs.Listen() {
			_ = msg
			// worker specific task..
		}
	}()

	return nil
}

// Called everytime becoming raft leader
// Context will be cancelled when stepping down from raft leader
func (w *worker) OnBecomingRaftLeader(ctx context.Context, term uint64, leaderID uint64) error {

	return nil
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

	go func(term, leaderID uint64) {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf("I'm ZA LEADER. IM POLLING FOR JOBSS SCHEDULE! term=%v leaderID=%v", term, leaderID)

				// queue all task, and handles is as just like we got a new task definition (<-subscriber.Listen())

			case msg := <-subscriber.Listen():
				t := time.Now()
				switch evt := msg.(type) {
				case *entity.TaskDefinition:
					// 1. Check if task exist previously?

					// 1a. If task exist previously, we should iterate all scheduled execution & cancel the timer & delete the entries.
					//     This might be configurable (user can cancel which one)
					//     Update it in the state machine for other to know.

					// 2. Schedule the new executions in the next n minutes/hours (should be longer than the ticker period)

					log.Printf("A new task definition has come. I better schedule! %+v", evt)
					userTime := evt.UserTime
					loc := userTime.Location()
					schedule, err := crong.New(evt.Cron, loc)
					if err != nil {
						continue
					}

					// schedule for the next n minutes (n=30 for now)

					for next := t; next.Before(t.Add(30 * time.Minute)); next = schedule.Next(next) {
						execID := uuid.NewString()
						result, err := client.Propose(ctx, genericjob.InitiateTaskExecution{ExecutionID: execID, TaskDefinition: *evt})
						if err != nil {
							log.Printf("ERROR SCHEDULE WEH: %v\n", err)
							continue
						}
						// todo: can handle cancellation
						tm := time.AfterFunc(time.Until(next), func() {
							log.Printf(" FROM ZA TASK >>> execution ID \"%v\" (%v) is now running!", execID, evt.Id)
						})
						func() {
							defer timersLock.Unlock()
							timersLock.Lock()
							timers[execID] = tm
						}()
						log.Printf("The ticker prepared in memory: %v\n", result)
					}

				default:
					log.Printf("Unknown job evt")
				}
			case <-ctx.Done():
				return
			}
		}
	}(term, leaderID)

	return nil
}
