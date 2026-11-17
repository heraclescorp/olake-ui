package etl

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	services "github.com/datazip-inc/olake-ui/server/internal/services/etl"
	"github.com/datazip-inc/olake-ui/server/internal/utils"
	"github.com/gin-gonic/gin"
)

func (h *Handler) ForceLSN(c *gin.Context) {
	project, err := utils.GetProjectID(c)
	if err != nil {
		utils.ErrorResponse(c, 400, "invalid project", err)
		return
	}
	id, err := utils.GetIDParam(c)
	if err != nil {
		utils.ErrorResponse(c, 400, "invalid job", err)
		return
	}
	actor := utils.GetCurrentUserID(c)
	if actor == nil {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	var req services.ForceLSNRequest
	decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 8192))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&req); err != nil {
		utils.ErrorResponse(c, 400, "invalid checkpoint request", err)
		return
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		utils.ErrorResponse(c, 400, "expected a single JSON object", err)
		return
	}
	apply := c.Request.Method == http.MethodPut
	preview, backup, err := h.etl.ForceLSN(c.Request.Context(), project, id, *actor, req, apply)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, constants.ErrJobBusy) || errors.Is(err, constants.ErrCatalogRevision) {
			status = http.StatusConflict
		}
		if errors.Is(err, constants.ErrJobNotFound) {
			status = 404
		}
		utils.ErrorResponse(c, status, err.Error(), err)
		return
	}
	if apply {
		utils.SuccessResponse(c, "Checkpoint overwritten; job remains paused", gin.H{"backup_id": backup, "lsn": preview.LSN})
		return
	}
	utils.SuccessResponse(c, "Checkpoint preview", preview)
}
