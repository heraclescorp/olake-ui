package database

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CheckpointRevision binds a preview to both the checkpoint and saved catalog.
func CheckpointRevision(job *models.Job) (string, error) {
	catalog, err := job.CatalogRevision()
	if err != nil {
		return "", err
	}
	var value any
	raw := job.State
	if strings.TrimSpace(raw) == "" {
		raw = "{}"
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(catalog+"\n"+string(canonical)))), nil
}

func (db *Database) ForceCheckpoint(ctx context.Context, project string, id, actor int, revision, state, lsn string) (uint64, error) {
	var backup models.CheckpointBackup
	err := db.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job models.Job
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND project_id = ?", id, project).First(&job).Error; err != nil {
			return err
		}
		if job.Active || job.ExecutionWorkflowID != nil || job.ExecutionRunID != nil {
			return constants.ErrJobBusy
		}
		current, err := CheckpointRevision(&job)
		if err != nil {
			return err
		}
		if current != revision {
			return constants.ErrCatalogRevision
		}
		var pending int64
		if err := tx.Model(&models.JobExecutionRecord{}).Where("job_id = ? AND phase <> ?", id, "closed").Count(&pending).Error; err != nil {
			return err
		}
		if pending > 0 {
			return constants.ErrJobBusy
		}
		backup = models.CheckpointBackup{JobID: id, ProjectID: project, ActorID: actor, PreviousState: job.State, NewLSN: lsn}
		if err := tx.Create(&backup).Error; err != nil {
			return err
		}
		return db.idleJobMutationResult(ctx, id, idleJobMutation(tx, id).Where("active = false").Updates(map[string]any{"state": state, "updated_by_id": actor}))
	})
	return backup.ID, err
}
