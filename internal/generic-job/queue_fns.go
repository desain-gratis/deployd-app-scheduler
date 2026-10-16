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
