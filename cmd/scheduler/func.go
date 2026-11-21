package main

import (
	"context"
	"log"
	"time"

	schedulerapi "github.com/desain-gratis/common/delivery/scheduler-api"
)

func sendInvoice(ctx context.Context, input any) (any, error) {
	taskExec := schedulerapi.GetTaskExecutionView(ctx).TaskDefinition()

	log.Printf("The task sendInvoice(): %v triggered! Scheduled run at %v with input: %v !", taskExec.Id, taskExec.ScheduledRunAt.Truncate(1*time.Second), taskExec.Input)

	return "BERHAZIL", nil
}

func echoReverse(ctx context.Context, input any) (any, error) {
	b, ok := input.(string)
	if !ok {
		return "Not a byte", nil
	}

	runes := []rune(string(b))
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}

	output := string(runes)

	log.Printf("The task echoReverse(): triggered! result: %v", output)

	return output, nil
}
