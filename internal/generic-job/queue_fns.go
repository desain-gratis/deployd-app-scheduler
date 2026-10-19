package genericjob

import (
	"log"
	"time"

	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

func sendInvoice(taskExec *entity.TaskExecution) error {
	log.Printf("The task sendInvoice(): %v triggered! Scheduled run at %v with input: %v !", taskExec.Id, taskExec.ScheduledRunAt.Truncate(1*time.Second), taskExec.Input)

	taskExec.Output = "BERHAZIL"

	return nil
}

func echoReverse(taskExec *entity.TaskExecution) error {
	b, ok := taskExec.Input.(string)
	if !ok {
		taskExec.Output = "Not a byte"
		return nil
	}

	runes := []rune(string(b))
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}

	taskExec.Output = string(runes)

	log.Printf("The task echoReverse(): %v triggered! result: %v", string(runes))

	return nil
}
