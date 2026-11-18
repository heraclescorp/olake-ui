package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/database"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/datazip-inc/olake-ui/server/internal/services/temporal"
	"github.com/datazip-inc/olake-ui/server/internal/utils/logger"
)

type ForceLSNRequest struct {
	LSN             string `json:"lsn"`
	Revision        string `json:"revision"`
	ConfirmBaseline bool   `json:"confirm_baseline"`
	Confirmation    string `json:"confirmation"`
}
type CheckpointPreview struct {
	LSN        string          `json:"lsn"`
	CurrentLSN string          `json:"current_lsn"`
	Streams    []string        `json:"streams"`
	Revision   string          `json:"revision"`
	State      json.RawMessage `json:"state"`
}

var lsnSyntax = regexp.MustCompile(`^[0-9a-fA-F]{1,8}/[0-9a-fA-F]{1,8}$`)

func buildCheckpointPreview(job *models.Job, lsn string) (CheckpointPreview, error) {
	var result CheckpointPreview
	lsn = strings.TrimSpace(lsn)
	if !lsnSyntax.MatchString(lsn) {
		return result, fmt.Errorf("LSN must be two 1–8 digit hexadecimal values separated by / (for example 761/F6ADE70)")
	}
	parts := strings.Split(lsn, "/")
	hi, _ := strconv.ParseUint(parts[0], 16, 32)
	lo, _ := strconv.ParseUint(parts[1], 16, 32)
	if hi == 0 && lo == 0 {
		return result, fmt.Errorf("0/0 is not a recovery checkpoint")
	}
	result.LSN = fmt.Sprintf("%X/%X", hi, lo)
	if job.Source == nil || !strings.EqualFold(job.Source.Type, "postgres") {
		return result, fmt.Errorf("force LSN supports PostgreSQL jobs only")
	}
	// Respect sparse selections by resolving modes from the saved available catalog.
	catalog := job.StreamsCatalog()
	available, selected := catalog.Streams, catalog.Streams
	if catalog.IsSplit() {
		available, selected = catalog.Available, catalog.Selected
	}
	var a struct {
		Streams []struct {
			Stream struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
				SyncMode  string `json:"sync_mode"`
			} `json:"stream"`
		} `json:"streams"`
	}
	if err := json.Unmarshal([]byte(available), &a); err != nil {
		return result, fmt.Errorf("invalid saved catalog")
	}
	modes := map[string]string{}
	for _, s := range a.Streams {
		id := s.Stream.Namespace + "." + s.Stream.Name
		if _, ok := modes[id]; ok {
			return result, fmt.Errorf("ambiguous saved stream %s", id)
		}
		modes[id] = s.Stream.SyncMode
	}
	var selections struct {
		Selected map[string][]struct {
			Name     string `json:"stream_name"`
			Disabled bool   `json:"disabled"`
			Mode     string `json:"sync_mode"`
		} `json:"selected_streams"`
	}
	if err := json.Unmarshal([]byte(selected), &selections); err != nil || selections.Selected == nil {
		return result, fmt.Errorf("saved selected streams are required")
	}
	targets := map[string]bool{}
	for ns, rows := range selections.Selected {
		for _, row := range rows {
			if row.Disabled {
				continue
			}
			id := ns + "." + row.Name
			mode, ok := modes[id]
			if !ok {
				return result, fmt.Errorf("selected stream %s is missing from catalog", id)
			}
			if row.Mode != "" {
				mode = row.Mode
			}
			if mode == "" {
				return result, fmt.Errorf("selected stream %s has no saved sync mode", id)
			}
			if mode == "cdc" || mode == "strict_cdc" {
				if targets[id] {
					return result, fmt.Errorf("duplicate selection %s", id)
				}
				targets[id] = true
			}
		}
	}
	if len(targets) == 0 {
		return result, fmt.Errorf("job has no selected CDC streams")
	}
	for id := range targets {
		result.Streams = append(result.Streams, id)
	}
	sort.Strings(result.Streams)
	state := map[string]json.RawMessage{}
	if strings.TrimSpace(job.State) != "" {
		if err := json.Unmarshal([]byte(job.State), &state); err != nil || state == nil {
			return result, fmt.Errorf("saved state is invalid; refusing to discard it")
		}
	}
	global := map[string]json.RawMessage{}
	if raw := state["global"]; len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &global); err != nil || global == nil {
			return result, fmt.Errorf("saved global state is invalid")
		}
	}
	position := map[string]json.RawMessage{}
	if raw := global["state"]; len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &position); err != nil || position == nil {
			return result, fmt.Errorf("saved global checkpoint is invalid")
		}
	}
	_ = json.Unmarshal(position["lsn"], &result.CurrentLSN)
	position["lsn"], _ = json.Marshal(result.LSN)
	global["state"], _ = json.Marshal(position)
	// Preserve prior completion markers, including unselected streams, while adding
	// current CDC selections. Never infer completion for non-CDC selections.
	var completed []string
	if raw := global["streams"]; len(raw) > 0 && string(raw) != "null" {
		if json.Unmarshal(raw, &completed) != nil {
			return result, fmt.Errorf("invalid saved completion markers")
		}
	}
	all := map[string]bool{}
	for _, id := range completed {
		all[id] = true
	}
	for _, id := range result.Streams {
		all[id] = true
	}
	completed = nil
	for id := range all {
		completed = append(completed, id)
	}
	sort.Strings(completed)
	global["streams"], _ = json.Marshal(completed)
	state["global"], _ = json.Marshal(global)
	state["type"] = json.RawMessage(`"GLOBAL"`)
	// Do not silently migrate nonempty older state versions.
	if raw, ok := state["version"]; ok {
		var version int
		if json.Unmarshal(raw, &version) != nil || version != 8 {
			return result, fmt.Errorf("only state version 8 is supported")
		}
	} else if len(state) > 2 {
		return result, fmt.Errorf("existing state has no version; refusing implicit migration")
	}
	state["version"] = json.RawMessage(`8`)
	result.State, _ = json.Marshal(state)
	revision, err := database.CheckpointRevision(job)
	result.Revision = revision
	return result, err
}

// ForceLSN preview and commit both fence execution and never contact source/destination.
func (s Service) ForceLSN(ctx context.Context, project string, id, actor int, req ForceLSNRequest, apply bool) (CheckpointPreview, uint64, error) {
	var empty CheckpointPreview
	ctx, release, err := s.db.BeginCatalogEdit(ctx, id)
	if err != nil {
		return empty, 0, err
	}
	defer release()
	job, err := s.db.GetJobByID(id, true)
	if err != nil {
		return empty, 0, err
	}
	if job.ProjectID != project {
		return empty, 0, constants.ErrJobNotFound
	}
	if job.Active {
		return empty, 0, fmt.Errorf("%w: pause the job first", constants.ErrJobBusy)
	}
	if err = ensureJobExecutionIdle(job); err != nil {
		return empty, 0, err
	}
	for _, cmd := range []temporal.Command{temporal.Sync, temporal.ClearDestination, temporal.Resync} {
		running, _, e := isWorkflowRunning(ctx, s.temporal, project, id, cmd)
		if e != nil {
			return empty, 0, e
		}
		if running {
			return empty, 0, constants.ErrJobBusy
		}
	}
	preview, err := buildCheckpointPreview(job, req.LSN)
	if err != nil {
		return empty, 0, err
	}
	if !apply {
		return preview, 0, nil
	}
	if !req.ConfirmBaseline || req.Confirmation != "FORCE "+preview.LSN {
		return empty, 0, fmt.Errorf("baseline acknowledgement and exact FORCE LSN confirmation are required")
	}
	if req.Revision == "" || req.Revision != preview.Revision {
		return empty, 0, constants.ErrCatalogRevision
	}
	backup, err := s.db.ForceCheckpoint(ctx, project, id, actor, req.Revision, string(preview.State), preview.LSN)
	if err == nil {
		logger.Infof("forced_cdc_checkpoint job_id=%d actor_id=%d old_lsn=%q new_lsn=%q streams=%d backup_id=%d", id, actor, preview.CurrentLSN, preview.LSN, len(preview.Streams), backup)
	}
	return preview, backup, err
}
