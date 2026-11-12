package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
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
		return tx.Model(&job).Updates(params).Error
	})
}
