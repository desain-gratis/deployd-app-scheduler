package genericjob

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type httpHandler struct{}

func (h *httpHandler) ConfigureWebsite(w http.ResponseWriter, r *http.Request, p httprouter.Params) {

}
