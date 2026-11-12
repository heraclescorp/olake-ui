package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type catalogEditContextKey struct {
	database *Database
	jobID    int
}

func (db *Database) BeginCatalogEdit(ctx context.Context, jobID int) (context.Context, func(), error) {
	key := catalogEditContextKey{database: db, jobID: jobID}
	if ctx.Value(key) != nil {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		return ctx, func() {}, nil
	}
	fenceContext, cancel := context.WithTimeout(ctx, 2*time.Minute)
	dialector, ok := db.conn.Dialector.(*postgres.Dialector)
	if !ok || dialector.DSN == "" {
		cancel()
		return nil, nil, fmt.Errorf("catalog editing requires a PostgreSQL DSN")
	}
	fenceDB, err := sql.Open("pgx", dialector.DSN)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	fenceDB.SetMaxOpenConns(1)
	fenceDB.SetMaxIdleConns(0)
	tx, err := fenceDB.BeginTx(fenceContext, nil)
	if err != nil {
		cancel()
		_ = fenceDB.Close()
		return nil, nil, err
	}
	release := func() {
		cancel()
		_ = tx.Rollback()
		_ = fenceDB.Close()
	}
	var acquired bool
	err = tx.QueryRowContext(fenceContext,
		"SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
		fmt.Sprintf("olake:catalog-edit:%d", jobID)).Scan(&acquired)
	if err != nil {
		release()
		return nil, nil, err
	}
	if !acquired {
		release()
		return nil, nil, fmt.Errorf("%w: settings are already being edited", constants.ErrJobBusy)
	}
	var handoffPending bool
	err = tx.QueryRowContext(fenceContext,
		"SELECT EXISTS (SELECT 1 FROM "+pq.QuoteIdentifier((&models.JobExecutionRecord{}).TableName())+" WHERE job_id = $1 AND phase = 'handoff')", jobID).Scan(&handoffPending)
	if err != nil {
		release()
		return nil, nil, err
	}
	if handoffPending {
		release()
		return nil, nil, fmt.Errorf("%w: published checkpoint handoff is pending", constants.ErrJobBusy)
	}
	return context.WithValue(fenceContext, key, true), release, nil
}

func (db *Database) CheckCatalogRevision(ctx context.Context, jobID int, projectID, revision string, required bool) error {
	return db.withCatalogRevision(ctx, jobID, projectID, revision, required, nil)
}

func (db *Database) UpdateJobWithCatalogRevision(ctx context.Context, jobID int, projectID, revision string, required bool, params map[string]any) error {
	return db.withCatalogRevision(ctx, jobID, projectID, revision, required, params)
}

func (db *Database) withCatalogRevision(ctx context.Context, jobID int, projectID, revision string, required bool, params map[string]any) error {
	return db.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job models.Job
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND project_id = ?", jobID, projectID).First(&job).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return constants.ErrJobNotFound
			}
			return err
		}
		if job.ExecutionWorkflowID != nil || job.ExecutionRunID != nil {
			return constants.ErrJobBusy
		}
		var handoffPending int64
		if err := tx.Model(&models.JobExecutionRecord{}).Where("job_id = ? AND phase = ?", jobID, "handoff").Count(&handoffPending).Error; err != nil {
			return err
		}
		if handoffPending != 0 {
			return constants.ErrJobBusy
		}
		enabled, err := job.AutoTableEnrollmentEnabled()
		if err != nil {
			return err
		}
		if required || enabled {
			current, err := job.CatalogRevision()
			if err != nil {
				return err
			}
			if revision == "" || current != revision {
				return constants.ErrCatalogRevision
			}
		}
		if params == nil {
			return nil
		}
		return db.idleJobMutationResult(ctx, jobID, idleJobMutation(tx, jobID).Updates(params))
	})
}

// The advisory lock can be released by request cancellation before a handler
// returns. Keep the final write conditional on current database authority too.
func idleJobMutation(tx *gorm.DB, jobID int) *gorm.DB {
	pending := tx.Session(&gorm.Session{NewDB: true}).Model(&models.JobExecutionRecord{}).
		Select("1").Where("job_id = ? AND phase = ?", jobID, "handoff")
	return tx.Model(&models.Job{}).
		Where("id = ? AND execution_workflow_id IS NULL AND execution_run_id IS NULL", jobID).
		Where("NOT EXISTS (?)", pending)
}

func (db *Database) idleJobMutationResult(ctx context.Context, jobID int, result *gorm.DB) error {
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 1 {
		return nil
	}
	var count int64
	if err := result.Session(&gorm.Session{NewDB: true, Context: ctx}).Model(&models.Job{}).Where("id = ?", jobID).Count(&count).Error; err != nil {
		return err
	}
	if count == 0 {
		return constants.ErrJobNotFound
	}
	return constants.ErrJobBusy
}
