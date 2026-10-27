package main

import (
	"log"
	"time"

	scheduler_integration "github.com/desain-gratis/deployd-app-scheduler/internal/scheduler"
)

func sendInvoice(taskExec *scheduler_integration.TaskExecutionRuntime) error {
	log.Printf("The task sendInvoice(): %v triggered! Scheduled run at %v with input: %v !", taskExec.Id, taskExec.ScheduledRunAt.Truncate(1*time.Second), taskExec.Input)

	taskExec.ExecutionState.Output = "BERHAZIL"

	return nil
}

func echoReverse(taskExec *scheduler_integration.TaskExecutionRuntime) error {
	b, ok := taskExec.Input.(string)
	if !ok {
		taskExec.ExecutionState.Output = "Not a byte"

		return nil
	}

	runes := []rune(string(b))
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}

	taskExec.ExecutionState.Output = string(runes)

	log.Printf("The task echoReverse(): triggered! result: %v", string(runes))

	return nil
}
