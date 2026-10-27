package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/desain-gratis/common/delivery/mycontent-api/mycontent"
	"github.com/desain-gratis/common/lib/notifier"
	"github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/scheduler"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
)

type httpAPI struct {
	rc *scheduler.Client

	tes scheduler.TaskExecutionStorage
}

// or background process I guess
func NewHttpAPI(topic notifier.Topic, rc *scheduler.Client, tes scheduler.TaskExecutionStorage) *httpAPI {
	return &httpAPI{rc: rc, tes: tes}
}

func (h *httpAPI) Hello(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	taskCh, errCh := h.tes.GetAll(r.Context(), "deployd")
	for taskCh != nil || errCh != nil {
		select {
		case task, ok := <-taskCh:
			if !ok {
				taskCh = nil
				continue
			}

			// Process task.
			result, err := json.Marshal(task)
			if err != nil {
				continue
			}
			fmt.Fprintln(w, string(result))

		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}

			if err != nil {
				log.Println("GetAll error:", err)
			}
		}
	}
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
		AssignmentState: nil,
		ExecutionState:  nil,
		RecurringTaskID: nil,
		URLx:            "",
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

func (h *httpAPI) CancelTaskExecution(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
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

	taskExec, err := h.tes.GetByID(r.Context(), input.Ns, input.QueueID, input.Id)
	if err != nil {
		if errors.Is(err, mycontent.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "WADUUH: %v", err)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "WADUUH: %v", err)
		return
	}

	// client validation (raft will validate as well, this just to make it quicker)
	switch taskExec.Status {
	case entity.ExecutionStatusInProgress, entity.ExecutionStatusPending:
	default:
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid state: %v", err)
		return
	}

	taskExec.Status = entity.ExecutionStatusCancelled

	// server do something
	data := scheduler.UpdateTaskExecution(taskExec)

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

	w.WriteHeader(http.StatusOK)
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
