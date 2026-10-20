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
	genericjob_integration "github.com/desain-gratis/deployd-app-scheduler/internal/generic-job"
	genericjob "github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/generic-job"
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
	jobApp := genericjob.New(namespace, topic, db)

	schedulerRaft, _, err := runneretcd.RunWithConfig(ctx, os.Getenv("DEPLOYD_RAFT"), "scheduler", jobApp)
	if err != nil {
		log.Fatal().Msgf("err running raft: %v", err)
	}

	taskUsecase := jobApp.GetTaskStore()
	taskExecutionUsecase := jobApp.GetTaskExecutionStore()

	schedulerClient := genericjob.NewClient(schedulerRaft)

	// List of functions to be executed inside scheduler
	queue := map[string]chan *entity.TaskExecution{
		"sendInvoice()": genericjob_integration.NewPooledQueue(ctx, schedulerClient, 30, sendInvoice),
		"echoReverse()": genericjob_integration.NewPooledQueue(ctx, schedulerClient, 30, echoReverse),
	}

	worker := genericjob_integration.NewWorker(namespace, topic, schedulerClient, taskUsecase, taskExecutionUsecase, queue)
	httpAPI := genericjob_integration.NewHttpAPI(topic, schedulerClient)

	err = schedulerRaft.Run(ctx, worker.OnBecomingRaftLeader)
	if err != nil {
		log.Fatal().Msgf("err running raft: %v", err)
	}

	err = worker.Run(ctx)
	if err != nil {
		log.Fatal().Msgf("err running integration: %v", err)
	}

	taskHandler := mycontentapi.New(taskUsecase, publicBaseURL+"/task", nil)
	taskExecutionHandler := mycontentapi.New(taskExecutionUsecase, publicBaseURL+"/task/instance", []string{"queue_id"})

	router.GET("/hello", httpAPI.Hello)
	router.POST("/schedule", httpAPI.CreateTask)

	router.GET("/task", taskHandler.Get)
	router.GET("/task/instance", taskExecutionHandler.Get)
}
