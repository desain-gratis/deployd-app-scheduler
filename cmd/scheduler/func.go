package main

import (
	"context"
	"fmt"
	"time"

	schedulerapi "github.com/desain-gratis/common/delivery/scheduler-api"
)

func sendInvoice(ctx context.Context, input any) (any, error) {
	taskExec := schedulerapi.GetTaskExecutionView(ctx).TaskDefinition()
	log := schedulerapi.GetLogger(ctx)

	log.Info(fmt.Sprintf("The task sendInvoice(): %v triggered! Scheduled run at %v with input: %v !", taskExec.Id, taskExec.ScheduledRunAt.Truncate(1*time.Second), taskExec.Input))

	return "BERHAZIL", nil
}

func longRunning(ctx context.Context, input any) (any, error) {
	taskExec := schedulerapi.GetTaskExecutionView(ctx).TaskDefinition()
	log := schedulerapi.GetLogger(ctx)

	durationSec := 5 * 60

	for i := range durationSec {
		err := ctx.Err()
		if err != nil {
			log.Error(fmt.Sprintf("I've been cancelled ma boyx: %v", err.Error()))
			return "SHORT SHORT MAN", nil
		}

		log.Info(fmt.Sprintf("This is a long running task id=%v schedule=%v count=%v", taskExec.Id, taskExec.ScheduledRunAt.Truncate(1*time.Second), i+1))
		time.Sleep(1 * time.Second)
	}

	return "LONG LONG MAN", nil
}

func echoReverse(ctx context.Context, input any) (any, error) {
	log := schedulerapi.GetLogger(ctx)

	b, ok := input.(string)
	if !ok {
		return "Not a byte", nil
	}

	runes := []rune(string(b))
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}

	output := string(runes)

	log.Info(fmt.Sprintf("The task echoReverse(): triggered! result: %v", output))

	return output, nil
}
