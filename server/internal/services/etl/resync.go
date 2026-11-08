package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	"github.com/datazip-inc/olake-ui/server/internal/types"
)

func (s Service) ResyncTable(ctx context.Context, projectID string, jobID int, req dto.ResyncTableRequest) error {
	job, err := s.db.GetJobByID(jobID, true)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}
	catalog, err := resolveTableResync(job, projectID, req)
	if err != nil {
		return err
	}
	return s.ClearDestination(ctx, projectID, job.ID, catalog, 0, false, true)
}

func resolveTableResync(job *models.Job, projectID string, req dto.ResyncTableRequest) (types.StreamsCatalog, error) {
	if job.ProjectID != projectID {
		return types.StreamsCatalog{}, constants.ErrJobNotFound
	}
	if !job.Active {
		return types.StreamsCatalog{}, fmt.Errorf("%w: please unpause to resync a table", constants.ErrJobBusy)
	}
	return tableResyncCatalog(job.StreamsCatalog(), req)
}

func tableResyncCatalog(catalog types.StreamsCatalog, req dto.ResyncTableRequest) (types.StreamsCatalog, error) {
	invalid := func(message string) (types.StreamsCatalog, error) {
		return types.StreamsCatalog{}, fmt.Errorf("%w: %s", constants.ErrInvalidResync, message)
	}
	if strings.TrimSpace(req.Namespace) == "" || strings.TrimSpace(req.StreamName) == "" {
		return invalid("namespace and stream_name are required")
	}
	availableJSON, selectedJSON := catalog.Streams, catalog.Streams
	if catalog.IsSplit() {
		availableJSON, selectedJSON = catalog.Available, catalog.Selected
	}
	var available, selected map[string]json.RawMessage
	if err := json.Unmarshal([]byte(availableJSON), &available); err != nil || available == nil {
		return invalid("invalid saved available streams")
	}
	if err := json.Unmarshal([]byte(selectedJSON), &selected); err != nil || selected == nil {
		return invalid("invalid saved selected streams")
	}
	var streams []json.RawMessage
	if err := json.Unmarshal(available["streams"], &streams); err != nil {
		return invalid("invalid saved streams")
	}
	var matchedStream json.RawMessage
	var streamMode string
	for _, raw := range streams {
		var entry struct {
			Stream struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
				SyncMode  string `json:"sync_mode"`
			} `json:"stream"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return invalid("invalid saved stream metadata")
		}
		if entry.Stream.Namespace == req.Namespace && entry.Stream.Name == req.StreamName {
			if matchedStream != nil {
				return invalid("ambiguous saved table")
			}
			matchedStream, streamMode = raw, entry.Stream.SyncMode
		}
	}
	if matchedStream == nil {
		return invalid("unknown table")
	}
	var namespaces map[string][]json.RawMessage
	if err := json.Unmarshal(selected["selected_streams"], &namespaces); err != nil {
		return invalid("invalid saved table selection")
	}
	var matchedSelection json.RawMessage
	for _, raw := range namespaces[req.Namespace] {
		var selection struct {
			StreamName string `json:"stream_name"`
			SyncMode   string `json:"sync_mode"`
			Disabled   bool   `json:"disabled"`
		}
		if err := json.Unmarshal(raw, &selection); err != nil {
			return invalid("invalid saved table selection")
		}
		if selection.StreamName != req.StreamName {
			continue
		}
		if matchedSelection != nil {
			return invalid("ambiguous saved table selection")
		}
		if selection.Disabled {
			return invalid("table is not selected")
		}
		matchedSelection = raw
		if selection.SyncMode != "" {
			streamMode = selection.SyncMode
		}
	}
	if matchedSelection == nil {
		return invalid("table is not selected")
	}
	switch streamMode {
	case "cdc", "full_refresh", "incremental":
	default:
		return invalid("table must use a backfill-capable sync mode, not CDC only")
	}
	streamSubset, err := json.Marshal([]json.RawMessage{matchedStream})
	if err != nil {
		return invalid("invalid saved stream metadata")
	}
	selectionSubset, err := json.Marshal(map[string][]json.RawMessage{req.Namespace: {matchedSelection}})
	if err != nil {
		return invalid("invalid saved table selection")
	}
	available["streams"] = streamSubset
	selected["selected_streams"] = selectionSubset
	if !catalog.IsSplit() {
		available["selected_streams"] = selectionSubset
		data, err := json.Marshal(available)
		if err != nil {
			return invalid("invalid saved catalog")
		}
		return types.StreamsCatalog{Streams: string(data)}, nil
	}
	availableData, err := json.Marshal(available)
	if err != nil {
		return invalid("invalid saved available streams")
	}
	selectedData, err := json.Marshal(selected)
	if err != nil {
		return invalid("invalid saved selected streams")
	}
	return types.StreamsCatalog{Available: string(availableData), Selected: string(selectedData)}, nil
}
