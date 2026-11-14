package etl

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	"github.com/datazip-inc/olake-ui/server/internal/utils"
	"github.com/gin-gonic/gin"
)

func decodeResyncTableRequest(body io.Reader) (dto.ResyncTableRequest, error) {
	var req dto.ResyncTableRequest
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return req, err
	}
	if strings.TrimSpace(req.Namespace) == "" || strings.TrimSpace(req.StreamName) == "" {
		return req, fmt.Errorf("namespace and stream_name are required")
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return req, fmt.Errorf("expected a single JSON object")
	}
	return req, nil
}

func (h *Handler) ResyncTable(c *gin.Context) {
	projectID, err := utils.GetProjectID(c)
	if err != nil {
		utils.ErrorResponse(c, http.StatusBadRequest, fmt.Sprintf("failed to validate request: %s", err), err)
		return
	}
	id, err := utils.GetIDParam(c)
	if err != nil {
		utils.ErrorResponse(c, http.StatusBadRequest, fmt.Sprintf("failed to validate request: %s", err), err)
		return
	}
	req, err := decodeResyncTableRequest(c.Request.Body)
	if err != nil {
		utils.ErrorResponse(c, http.StatusBadRequest, fmt.Sprintf("failed to validate request: %s", err), err)
		return
	}
	if err := h.etl.ResyncTable(c.Request.Context(), projectID, id, req); err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, constants.ErrJobNotFound):
			status = http.StatusNotFound
		case errors.Is(err, constants.ErrInvalidResync):
			status = http.StatusBadRequest
		case errors.Is(err, constants.ErrJobBusy):
			status = http.StatusConflict
		}
		utils.ErrorResponse(c, status, fmt.Sprintf("failed to resync table: %s", err), err)
		return
	}
	utils.SuccessResponse(c, fmt.Sprintf("table rebuild started for job_id[%d]; existing data remains readable until atomic publication, then readers see it after their usual refresh", id), nil)
}
