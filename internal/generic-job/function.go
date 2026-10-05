package genericjob

import (
	"context"
	"encoding/json"

	dgraft "github.com/desain-gratis/common/lib/raft"
	runneretcd "github.com/desain-gratis/common/lib/raft/runner-etcd"
	genericjob "github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/generic-job"
	"github.com/rs/zerolog/log"
)

func Execute(ctx context.Context) error {
	// log

	rctx, ok := dgraft.GetRaftContext(ctx).(*runneretcd.RaftContext)
	if !ok {
		log.Fatal().Msgf("not an etcd raft runner")
	}

	// create task execution ID
	task := map[string]any{
		"task_id":      "do-something",
		"execution_id": "asdhkfadsa98f69812312j", // auto generated
		// other metadata...
		"trigger": "cron **",
		"status":  "queued",
	}

	payload, err := json.Marshal(task)
	if err != nil {
		return err
	}

	// register task execution
	// also, notifies the cluster to monitor the task result if retries is needed.
	_, err = rctx.Propose(ctx, genericjob.CommandWrapper{Name: genericjob.Command_User_ExecuteTask, Value: payload})
	if err != nil {
		log.Err(err).Msgf("ohno")
		return err
	}

	status := "failed"

	defer func() error {
		task["status"] = status
		_, err := rctx.Propose(ctx, genericjob.CommandWrapper{Name: genericjob.Command_User_UpdateTaskExecution, Value: payload}) // todo different payload
		if err != nil {
			log.Err(err).Msgf("ohno")
			return err
		}
		return nil
	}()

	// then we do the task logic
	status = "success"

	return nil
}
