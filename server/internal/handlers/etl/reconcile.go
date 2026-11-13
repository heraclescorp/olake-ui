package etl

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/utils"
	"github.com/gin-gonic/gin"
)

func (h *Handler) GetJobExecutionRecovery(c *gin.Context) {
	h.jobExecutionRecovery(c, false)
}

func (h *Handler) ReconcileJobExecution(c *gin.Context) {
	h.jobExecutionRecovery(c, true)
}

func (h *Handler) jobExecutionRecovery(c *gin.Context, reset bool) {
	if utils.GetCurrentUserID(c) == nil {
		utils.ErrorResponse(c, http.StatusUnauthorized, "authentication required", nil)
		return
	}
	projectID, err := utils.GetProjectID(c)
	if err != nil {
		utils.ErrorResponse(c, http.StatusBadRequest, err.Error(), err)
		return
	}
	jobID, err := utils.GetIDParam(c)
	if err != nil {
		utils.ErrorResponse(c, http.StatusBadRequest, err.Error(), err)
		return
	}
	result, err := h.etl.ReconcileJobExecution(c.Request.Context(), projectID, jobID, reset)
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, constants.ErrJobNotFound):
			status = http.StatusNotFound
		case errors.Is(err, constants.ErrJobBusy):
			status = http.StatusConflict
		}
		utils.ErrorResponse(c, status, fmt.Sprintf("cannot reconcile job execution: %s", err), err)
		return
	}
	if reset {
		utils.AcceptedResponse(c, result.Message, result)
		return
	}
	utils.SuccessResponse(c, result.Message, result)
}
