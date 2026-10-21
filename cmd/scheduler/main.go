package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	mycontentapi "github.com/desain-gratis/common/delivery/mycontent-api"
	notifier_impl "github.com/desain-gratis/common/lib/notifier/impl"
	runneretcd "github.com/desain-gratis/common/lib/raft/runner-etcd"
	"github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/scheduler"
	scheduler_integration "github.com/desain-gratis/deployd-app-scheduler/internal/scheduler"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

const (
	publicBaseURL = "https://deployd-app-scheduler.desain.gratis"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).With().Logger()
}

const (
	httpPublicAddress = ":9100"
)

func main() {
	ctx, cancel := context.WithCancelCause(context.Background())

	router := httprouter.New()

	enableSchedulerModule(ctx, router)

	wg := new(sync.WaitGroup)

	go startHttpListener(ctx, wg, router, httpPublicAddress)

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	log.Info().Msgf("waiting for sigint")
	<-sigint
	cancel(errors.New("server closed"))
	wg.Wait()
	log.Info().Msgf("bye bye")
}

func enableSchedulerModule(ctx context.Context, router *httprouter.Router) {
	db, err := badger.Open(badger.DefaultOptions("./tmp/scheduler.db"))
	if err != nil {
		log.Fatal().Msgf("UHUY %v", err)
	}

	topic := notifier_impl.NewStandardTopic()

	namespace := "deployd"
	assignmentUpdated := make(chan *entity.TaskExecution) // to sync schedule
	schedulerApp := scheduler.New(namespace, topic, db, assignmentUpdated)

	schedulerRaft, _, err := runneretcd.RunWithConfig(ctx, os.Getenv("DEPLOYD_RAFT"), "scheduler", schedulerApp)
	if err != nil {
		log.Fatal().Msgf("err running raft: %v", err)
	}

	recurringTaskUsecase := schedulerApp.GetRecurringTaskStore()
	taskExecutionUsecase := schedulerApp.GetTaskExecutionStore()

	schedulerClient := scheduler.NewClient(schedulerRaft)

	// List of functions to be executed inside scheduler
	queue := map[string]chan *scheduler_integration.TaskExecutionRuntime{
		"sendInvoice()": scheduler_integration.NewPooledQueue(ctx, schedulerClient, 30, sendInvoice),
		"echoReverse()": scheduler_integration.NewPooledQueue(ctx, schedulerClient, 30, echoReverse),
	}

	worker := scheduler_integration.NewWorker(namespace, topic, schedulerClient, recurringTaskUsecase, taskExecutionUsecase, queue, assignmentUpdated)
	httpAPI := scheduler_integration.NewHttpAPI(topic, schedulerClient)

	err = schedulerRaft.Run(ctx, worker.OnBecomingRaftLeader)
	if err != nil {
		log.Fatal().Msgf("err running raft: %v", err)
	}

	err = worker.Run(ctx)
	if err != nil {
		log.Fatal().Msgf("err running integration: %v", err)
	}

	recurringTaskHandler := mycontentapi.New(recurringTaskUsecase, publicBaseURL+"/task", nil)
	taskExecutionHandler := mycontentapi.New(taskExecutionUsecase, publicBaseURL+"/task/instance", []string{"queue_id"})

	router.GET("/hello", httpAPI.Hello)
	router.POST("/schedule", httpAPI.CreateTask)

	router.GET("/task", recurringTaskHandler.Get)
	router.GET("/task/instance", taskExecutionHandler.Get)
}
