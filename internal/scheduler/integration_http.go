package scheduler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/desain-gratis/common/lib/notifier"
	"github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/scheduler"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
)

type httpAPI struct {
	rc *scheduler.Client
}

// or background process I guess
func NewHttpAPI(topic notifier.Topic, rc *scheduler.Client) *httpAPI {
	return &httpAPI{rc: rc}
}

func (h *httpAPI) Hello(w http.ResponseWriter, r *http.Request, p httprouter.Params) {

}

func (h *httpAPI) ExecuteTask(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "WADUUH: %v", err)
		return
	}

	var input entity.TaskExecution
	err = json.Unmarshal(payload, &input)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "WADUUH: %v", err)
		return
	}

	t0 := time.Now()

	taskExec := &entity.TaskExecution{
		Ns:             input.Ns,
		Id:             uuid.NewString(),
		ParentId:       input.ParentId,
		QueueID:        input.QueueID,
		ScheduledRunAt: t0,
		Status:         entity.ExecutionStatusPending,
		Input:          input.Input,
		UserTime:       input.UserTime,
		ExecutionMode:  entity.TaskExecutionModeImmediate,
		PublishedAt:    t0,

		// overwrriten
		AssignmentStatus: nil,
		ActualRunAt:      nil,
		CompletedAt:      nil,
		Output:           nil,
		RecurringTaskID:  nil,
		URLx:             "",
	}

	// server do something
	data := scheduler.CreateTaskExecution(taskExec)

	result, err := h.rc.Propose(r.Context(), data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "XXX UWADUUH: %v %v", err, data)
		return
	}

	resp, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "XXX UWADUUH: %v %v", err, data)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprint(w, string(resp))
}

func (h *httpAPI) CreateRecurringTask(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "WADUUH: %v", err)
		return
	}

	var taskDefinition entity.RecurringTask
	err = json.Unmarshal(payload, &taskDefinition)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "WADUUH: %v", err)
		return
	}

	taskDefinition.Id = uuid.NewString()

	// server do something
	data := scheduler.CreateRecurringTask{
		Task:       taskDefinition,
		ServerTime: time.Now(),
	}

	result, err := h.rc.Propose(r.Context(), data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "XXX UWADUUH: %v %v", err, data)
		return
	}

	resp, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "XXX UWADUUH: %v %v", err, data)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprint(w, string(resp))
}
