package database

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
)

func TestForceCheckpointAtomicBackupAndFences(t *testing.T) {
	db := enrollmentTestDatabase(t)
	job := &models.Job{Name: "checkpoint", ProjectID: "p", Active: false, State: `{"version":8,"old":true}`}
	if err := db.conn.Create(job).Error; err != nil {
		t.Fatal(err)
	}
	revision, _ := CheckpointRevision(job)
	ctx, release, err := db.BeginCatalogEdit(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	id, err := db.ForceCheckpoint(ctx, "p", job.ID, 7, revision, `{"version":8,"new":true}`, "0/123")
	if err != nil {
		t.Fatal(err)
	}
	var backup models.CheckpointBackup
	if err = db.conn.First(&backup, id).Error; err != nil {
		t.Fatal(err)
	}
	var saved, original any
	_ = json.Unmarshal([]byte(backup.PreviousState), &saved)
	_ = json.Unmarshal([]byte(job.State), &original)
	if !reflect.DeepEqual(saved, original) || backup.ActorID != 7 {
		t.Fatal("backup missing original")
	}
	if _, err = db.ForceCheckpoint(ctx, "p", job.ID, 7, revision, `{}`, "0/234"); !errors.Is(err, constants.ErrCatalogRevision) {
		t.Fatal("stale preview accepted", err)
	}
	for _, fields := range []map[string]any{{"active": true}, {"active": false, "execution_workflow_id": "running", "execution_run_id": "run"}} {
		if err = db.conn.Model(job).Updates(fields).Error; err != nil {
			t.Fatal(err)
		}
		if _, err = db.ForceCheckpoint(ctx, "p", job.ID, 7, revision, `{}`, "0/234"); !errors.Is(err, constants.ErrJobBusy) {
			t.Fatal("busy job accepted", err)
		}
	}
	// A retained execution record blocks mutation even after owner pointers clear.
	if err = db.conn.Model(job).Updates(map[string]any{"execution_workflow_id": nil, "execution_run_id": nil, "active": false}).Error; err != nil {
		t.Fatal(err)
	}
	var current models.Job
	db.conn.First(&current, job.ID)
	currentRevision, _ := CheckpointRevision(&current)
	record := models.JobExecutionRecord{JobID: job.ID, WorkflowID: "retained", Phase: "closing"}
	if err = db.conn.Create(&record).Error; err != nil {
		t.Fatal(err)
	}
	if _, err = db.ForceCheckpoint(ctx, "p", job.ID, 7, currentRevision, `{}`, "0/234"); !errors.Is(err, constants.ErrJobBusy) {
		t.Fatal("retained record accepted", err)
	}
	var count int64
	db.conn.Model(&models.CheckpointBackup{}).Count(&count)
	if count != 1 {
		t.Fatal("failed mutation created backup", count)
	}
}

func TestForceCheckpointBackupFailureRollsBack(t *testing.T) {
	db := enrollmentTestDatabase(t)
	job := &models.Job{Name: "rollback", ProjectID: "p", State: `{}`}
	if err := db.conn.Create(job).Error; err != nil {
		t.Fatal(err)
	}
	revision, _ := CheckpointRevision(job)
	if err := db.conn.Migrator().DropTable(&models.CheckpointBackup{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ForceCheckpoint(context.Background(), "p", job.ID, 7, revision, `{"version":8}`, "0/123"); err == nil {
		t.Fatal("write succeeded without backup")
	}
	var saved models.Job
	db.conn.First(&saved, job.ID)
	if saved.State != "{}" {
		t.Fatal("state changed without backup", saved.State)
	}
}
