package etl

import (
	"context"
	"errors"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/utils"
)

func TestForceLSNServiceFencesAndConfirmation(t *testing.T) {
	service, job, actor := resyncFenceService(t)
	job.Active = false
	sample := checkpointTestJob(true)
	fields := map[string]any{"active": false, "state": `{}`, "streams_config": nil, "available_streams_config": sample.AvailableStreamsConfig, "selected_streams_config": sample.SelectedStreamsConfig}
	if err := service.db.UpdateJob(context.Background(), job.ID, fields); err != nil {
		t.Fatal(err)
	}
	req := ForceLSNRequest{LSN: "761/F6ADE70"}
	preview, _, err := service.ForceLSN(context.Background(), job.ProjectID, job.ID, actor.ID, req, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = service.ForceLSN(context.Background(), "wrong-project", job.ID, actor.ID, req, false); !errors.Is(err, constants.ErrJobNotFound) {
		t.Fatal("project scope missing", err)
	}
	req.Revision = preview.Revision
	if _, _, err = service.ForceLSN(context.Background(), job.ProjectID, job.ID, actor.ID, req, true); err == nil {
		t.Fatal("confirmation not enforced")
	}
	req.ConfirmBaseline = true
	req.Confirmation = "FORCE 761/F6ADE70"
	_, backup, err := service.ForceLSN(context.Background(), job.ProjectID, job.ID, actor.ID, req, true)
	if err != nil {
		t.Fatal(err)
	}
	if backup == 0 {
		t.Fatal("missing backup")
	}
	saved, err := service.db.GetJobByID(job.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Active {
		t.Fatal("job resumed")
	}
	if _, _, err = service.ForceLSN(context.Background(), job.ProjectID, job.ID, actor.ID, req, true); !errors.Is(err, constants.ErrCatalogRevision) {
		t.Fatal("stale preview accepted", err)
	}
	if err = service.db.UpdateJob(context.Background(), job.ID, map[string]any{"execution_workflow_id": utils.StringPtr("running"), "execution_run_id": utils.StringPtr("run")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = service.ForceLSN(context.Background(), job.ProjectID, job.ID, actor.ID, req, false); !errors.Is(err, constants.ErrJobBusy) {
		t.Fatal("owner fence missing", err)
	}
}
