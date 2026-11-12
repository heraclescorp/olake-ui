package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	"github.com/datazip-inc/olake-ui/server/internal/services/temporal"
	"github.com/datazip-inc/olake-ui/server/internal/types"
)

func (s Service) ResyncTable(ctx context.Context, projectID string, jobID int, req dto.ResyncTableRequest) error {
	ctx, release, err := s.db.BeginCatalogEdit(ctx, jobID)
	if err != nil {
		return err
	}
	defer release()

	job, err := s.db.GetJobByID(jobID, true)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}
	_, err = resolveTableResync(job, projectID, req)
	if err != nil {
		return err
	}
	return s.startTableResync(ctx, job, req)
}

func (s Service) startTableResync(ctx context.Context, job *models.Job, req dto.ResyncTableRequest) error {
	if err := validateAtomicResyncSupport(job); err != nil {
		return err
	}
	projectID, jobID := job.ProjectID, job.ID
	if err := s.temporal.RequireUnpausedSchedule(ctx, projectID, jobID); err != nil {
		return err
	}
	if err := ensureNoResync(ctx, s.temporal, projectID, jobID); err != nil {
		return err
	}
	for _, command := range []temporal.Command{temporal.Sync, temporal.ClearDestination} {
		running, _, err := isWorkflowRunning(ctx, s.temporal, projectID, jobID, command)
		if err != nil {
			return fmt.Errorf("failed to check %s status: %w", command, err)
		}
		if running {
			return fmt.Errorf("%w: %s is running", constants.ErrJobBusy, command)
		}
	}
	if err := s.temporal.PauseSchedule(ctx, projectID, jobID); err != nil {
		return fmt.Errorf("failed to pause schedule: %w", err)
	}
	if err := waitForSyncToStop(ctx, s.temporal, projectID, jobID, 0); err != nil {
		if resumeErr := s.temporal.ResumeSchedule(ctx, projectID, jobID); resumeErr != nil {
			return fmt.Errorf("%w; failed to resume schedule: %v", err, resumeErr)
		}
		return err
	}
	// A start error can mean Temporal accepted the request but its response was lost.
	// Leave the ordinary schedule paused until the operation is reconciled.
	return s.temporal.ResyncTable(ctx, job, req)
}

func ensureNoResync(ctx context.Context, t *temporal.Temporal, projectID string, jobID int) error {
	running, _, err := isWorkflowRunning(ctx, t, projectID, jobID, temporal.Resync)
	if err != nil {
		return fmt.Errorf("failed to check table resync status: %w", err)
	}
	if running {
		return fmt.Errorf("%w: table resync is running", constants.ErrJobBusy)
	}
	return nil
}

func jobRunType(command temporal.Command) string {
	switch command {
	case temporal.ClearDestination:
		return "clear"
	case temporal.Resync:
		return "resync"
	default:
		return "sync"
	}
}

func ensureJobExecutionIdle(job *models.Job) error {
	if job.ExecutionWorkflowID != nil || job.ExecutionRunID != nil {
		return fmt.Errorf("%w: job execution is in progress or awaiting reconciliation", constants.ErrJobBusy)
	}
	return nil
}

func validateAtomicResyncSupport(job *models.Job) error {
	if job.Source == nil || job.Destination == nil {
		return fmt.Errorf("%w: saved source and destination are required", constants.ErrInvalidResync)
	}
	if job.Source.Type != "postgres" || job.Destination.DestType != "iceberg" {
		return fmt.Errorf("%w: uninterrupted table resync supports PostgreSQL sources with Iceberg destinations only; Clear Destination remains a separate destructive operation", constants.ErrInvalidResync)
	}
	return nil
}

func resolveTableResync(job *models.Job, projectID string, req dto.ResyncTableRequest) (types.StreamsCatalog, error) {
	if job.ProjectID != projectID {
		return types.StreamsCatalog{}, constants.ErrJobNotFound
	}
	if err := validateAtomicResyncSupport(job); err != nil {
		return types.StreamsCatalog{}, err
	}
	if err := ensureJobExecutionIdle(job); err != nil {
		return types.StreamsCatalog{}, err
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
	return catalog, nil
}
