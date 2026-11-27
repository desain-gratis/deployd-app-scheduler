package main

import (
	"context"
	"database/sql"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/chdb-io/chdb-go/v2/chdb/driver"
	mycontentapi "github.com/desain-gratis/common/delivery/mycontent-api"
	schedulerapi "github.com/desain-gratis/common/delivery/scheduler-api"
	runneretcd "github.com/desain-gratis/common/lib/raft/runner-etcd"
	entity "github.com/desain-gratis/common/types/entity/scheduler"
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
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	router := httprouter.New()

	wg := new(sync.WaitGroup)

	enableSchedulerModule(ctx, wg, router)

	go startHttpListener(ctx, wg, router, httpPublicAddress)

	log.Info().Msgf("waiting for sigint")

	<-ctx.Done()

	wg.Wait()

	log.Info().Msgf("bye bye")
}

func enableSchedulerModule(ctx context.Context, wg *sync.WaitGroup, router *httprouter.Router) {
	defaultNamespace := "deployd"

	chDB, err := sql.Open("chdb", "session=/data/deployd_scheduler/scheduler.chdb")
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

	_ = scheduler.Queue(raftwrapper, "/var/log/deployd_scheduler/queue").
		AddFunction(ctx, "sendInvoice()", 30, sendInvoice).
		AddFunction(ctx, "echoReverse()", 30, echoReverse).
		AddFunction(ctx, "longRunning()", 30, longRunning).
		WithLogProducer(logInject)

	err = scheduler.StartWorker(ctx, raftwrapper)
	if err != nil {
		log.Fatal().Msgf("failed to start scheduler worker")
	}

	// Start the raft engine
	// use context.Background() for graceful shutdown (todo: might need to move it inside)
	err = raftwrapper.Start(scheduler.OnBecomingRaftLeader)
	if err != nil {
		log.Fatal().Msgf("err running replicated scheduler: %v", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-ctx.Done()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		log.Info().Msgf("MAIN: shutting down the raft scheduler..")
		if err := raftwrapper.Shutdown(ctx); err != nil {
			log.Err(err).Msgf("MAIN: raft scheduler")
			return
		}

		log.Info().Msgf("MAIN: raft wrapper has been shut down")
	}()

	recurringTaskHandler := mycontentapi.New(scheduler.RecurringTaskUsecase, publicBaseURL+"/task", nil)
	taskExecutionHandler := mycontentapi.New(scheduler.TaskExecutionUsecase, publicBaseURL+"/task/instance", []string{"queue_id"})

	host := os.Getenv("DEPLOYD_HOST")
	taskExecutionHandler.WithPostProcess(func(t *entity.TaskExecution) {
		// todo: make proper later
		t.LogURL = "http://" + host + httpPublicAddress + "/task/log?queue_id=" + t.QueueID + "&task_id=" + t.Id
	})

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

	router.GET("/task/log/tail", httpHandler.TailLog(ctx))
	router.GET("/task/log", httpHandler.TailLog(ctx)) // todo: implement
}
