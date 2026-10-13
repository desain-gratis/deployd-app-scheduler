package genericjob

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/desain-gratis/common/lib/notifier"
	genericjob "github.com/desain-gratis/deployd-app-scheduler/internal/raft-app/generic-job"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
	"github.com/julienschmidt/httprouter"
)

type httpAPI struct {
	rc *genericjob.Client
}

// or background process I guess
func NewHttpAPI(topic notifier.Topic, rc *genericjob.Client) *httpAPI {
	return &httpAPI{rc: rc}
}

func (h *httpAPI) Hello(w http.ResponseWriter, r *http.Request, p httprouter.Params) {

}

func (h *httpAPI) CreateTask(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "WADUUH: %v", err)
		return
	}

	var taskDefinition entity.TaskDefinition
	err = json.Unmarshal(payload, &taskDefinition)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "WADUUH: %v", err)
		return
	}
	// taskUID := uuid.New()
	// server do something
	data := genericjob.CreateTask{
		TaskDefinition: taskDefinition,
		// NextExecutionUUID: taskUID.String(),
		ServerTime: time.Now(),
	}

	result, err := h.rc.Propose(r.Context(), data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "XXX UWADUUH: %v %v", err, data)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "SUCCESS: %v", result)
}
