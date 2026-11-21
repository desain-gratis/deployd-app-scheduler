package main

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/signal"
	"sync"

	_ "github.com/chdb-io/chdb-go/v2/chdb/driver"
	mycontentapi "github.com/desain-gratis/common/delivery/mycontent-api"
	schedulerapi "github.com/desain-gratis/common/delivery/scheduler-api"
	runneretcd "github.com/desain-gratis/common/lib/raft/runner-etcd"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
	defaultNamespace := "deployd"

	chDB, err := sql.Open("chdb", "session=./tmp/scheduler.chdb")
	if err != nil {
		log.Fatal().Msgf("UHUY %v", err)
	}

	scheduler, err := schedulerapi.NewReplicatedScheduler(ctx, defaultNamespace, chDB)
	if err != nil {
		log.Fatal().Msgf("FAILED init scheduler: %v", scheduler)
	}

	// raft wrapper
	raftwrapper, _, err := runneretcd.InitWithConfig(ctx, os.Getenv("DEPLOYD_RAFT"), "scheduler", scheduler.StateMachine)
	if err != nil {
		log.Fatal().Msgf("err running raft: %v", err)
	}

	// List of functions to be executed inside scheduler
	// queue := map[string]chan *scheduler_integration.TaskExecutionRuntime{
	// 	"sendInvoice()": scheduler_integration.NewPooledQueue(ctx, schedulerClient, 30, sendInvoice),
	// 	"echoReverse()": scheduler_integration.NewPooledQueue(ctx, schedulerClient, 30, echoReverse),
	// }

	err = scheduler.StartWorker(ctx, raftwrapper)
	if err != nil {
		log.Fatal().Msgf("failed to start scheduler worker")
	}

	_ = scheduler.Queue(raftwrapper).
		AddFunction(ctx, "sendInvoice()", 30, sendInvoice).
		AddFunction(ctx, "echoReverse()", 30, echoReverse)

	// Start the raft engine
	err = raftwrapper.Start(ctx, scheduler.OnBecomingRaftLeader)
	if err != nil {
		log.Fatal().Msgf("err running replicated scheduler: %v", err)
	}

	recurringTaskHandler := mycontentapi.New(scheduler.RecurringTaskUsecase, publicBaseURL+"/task", nil)
	taskExecutionHandler := mycontentapi.New(scheduler.TaskExecutionUsecase, publicBaseURL+"/task/instance", []string{"queue_id"})

	httpHandler := scheduler.HttpHandler(raftwrapper)
	router.GET("/hello", httpHandler.Hello)

	router.POST("/task/recurring", scheduler.HttpHandler(raftwrapper).CreateRecurringTask)
	router.GET("/task/recurring", recurringTaskHandler.Get)
	router.POST("/task/recurring/cancel", httpHandler.CancelRecurringTask)

	// task meaning task execution
	router.POST("/task", httpHandler.ExecuteTask)
	router.POST("/task/cancel", httpHandler.CancelTaskExecution)
	router.GET("/task/daily", httpHandler.GetDailyCount)
	router.GET("/task/hourly", httpHandler.GetHourlyCount)

	router.GET("/task", taskExecutionHandler.Get) // todo: integrate with httpAPI to make it consistent
}
