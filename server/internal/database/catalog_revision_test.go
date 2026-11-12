package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// These tests exercise the production row-lock/write path against an isolated
// schema in a real PostgreSQL database, never an echoing SQL mock.
func enrollmentTestDatabase(t *testing.T) *Database {
	t.Helper()
	dsn := os.Getenv("OLAKE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set OLAKE_TEST_POSTGRES_DSN to an isolated PostgreSQL database")
	}
	previousTables := constants.TableNameMap
	constants.TableNameMap = map[constants.TableType]string{
		constants.JobTable:             "catalog_revision_jobs",
		constants.UserTable:            "catalog_revision_users",
		constants.SourceTable:          "catalog_revision_sources",
		constants.DestinationTable:     "catalog_revision_destinations",
		constants.CatalogTable:         "catalog_revision_catalogs",
		constants.SessionTable:         "catalog_revision_sessions",
		constants.ProjectSettingsTable: "catalog_revision_project_settings",
	}
	t.Cleanup(func() { constants.TableNameMap = previousTables })
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	adminSQL, err := admin.DB()
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("enrollment_test_%d", time.Now().UnixNano())
	if err := admin.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error; err != nil {
			t.Error(err)
		}
		if err := adminSQL.Close(); err != nil {
			t.Error(err)
		}
	})
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		dsn += " search_path=" + schema
	}
	conn, err := gorm.Open(postgres.Open(dsn), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := conn.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := conn.AutoMigrate(&models.Job{}); err != nil {
		t.Fatal(err)
	}
	return &Database{conn: conn}
}

func savedEnrollmentJob(t *testing.T, db *Database, split, enabled bool) *models.Job {
	t.Helper()
	available := `{"streams":[{"stream":{"namespace":"public","name":"orders"}}]}`
	selected := `{"selected_streams":{"public":[{"stream_name":"orders","destination_table":"custom_orders","selected_columns":{"sync_new_columns":false}}]}}`
	settings := fmt.Sprintf(`{"auto_table_enrollment":{"enabled":%t}}`, enabled)
	job := &models.Job{Name: "orders", ProjectID: "project-a", SourceID: 101, DestID: 202, Active: true, Frequency: "@hourly", State: `{"global":{"lsn":"0/ABC"},"streams":{"public.orders":{"cursor":42}}}`, AdvancedSettings: &settings}
	if split {
		job.AvailableStreamsConfig, job.SelectedStreamsConfig = &available, &selected
	} else {
		legacy := `{"streams":[{"stream":{"namespace":"public","name":"orders"}}],"selected_streams":{"public":[{"stream_name":"orders","destination_table":"custom_orders","selected_columns":{"sync_new_columns":false}}]}}`
		job.StreamsConfig = &legacy
	}
	if err := db.CreateJob(job); err != nil {
		t.Fatal(err)
	}
	return job
}

func TestCatalogRevisionRejectsStaleAndMissingBeforeCancellation(t *testing.T) {
	db := enrollmentTestDatabase(t)
	for _, split := range []bool{false, true} {
		job := savedEnrollmentJob(t, db, split, true)
		for _, revision := range []string{"", "stale"} {
			if err := db.CheckCatalogRevision(context.Background(), job.ID, job.ProjectID, revision, true); !errors.Is(err, constants.ErrCatalogRevision) {
				t.Fatalf("accepted stale request: %v", err)
			}
		}
	}
}

func TestCatalogRevisionRechecksAfterWorkerEnrollmentWithoutResettingState(t *testing.T) {
	db := enrollmentTestDatabase(t)
	ctx := context.Background()
	for _, split := range []bool{false, true} {
		job := savedEnrollmentJob(t, db, split, true)
		revision, err := job.CatalogRevision()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CheckCatalogRevision(ctx, job.ID, job.ProjectID, revision, true); err != nil {
			t.Fatal(err)
		}
		// A real worker commit can land after the first check while the service
		// cancels workflows. The final locked write must not erase its catalog.
		field := "streams_config"
		proposal := `{"streams":[],"selected_streams":{"public":[{"stream_name":"orders"},{"stream_name":"new_table"}]}}`
		if split {
			field = "selected_streams_config"
			proposal = `{"selected_streams":{"public":[{"stream_name":"orders"},{"stream_name":"new_table"}]}}`
		}
		if err := db.UpdateJob(job.ID, map[string]any{field: proposal}); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateJobWithCatalogRevision(ctx, job.ID, job.ProjectID, revision, true, map[string]any{field: job.StreamsConfig, "name": "stale edit"}); !errors.Is(err, constants.ErrCatalogRevision) {
			t.Fatalf("erased worker enrollment: %v", err)
		}
		var fresh models.Job
		if err := db.conn.First(&fresh, job.ID).Error; err != nil {
			t.Fatal(err)
		}
		freshRevision, err := fresh.CatalogRevision()
		if err != nil {
			t.Fatal(err)
		}
		if freshRevision == revision || fresh.Name != job.Name {
			t.Fatal("stale edit committed")
		}
		// A policy-only save uses the fresh token and touches neither driver
		// reference, available/selected overrides, nor whole-job/global LSN.
		updatedSettings := `{"auto_table_enrollment":{"enabled":false}}`
		if err := db.UpdateJobWithCatalogRevision(ctx, job.ID, job.ProjectID, freshRevision, true, map[string]any{"advanced_settings": updatedSettings}); err != nil {
			t.Fatal(err)
		}
		var updated models.Job
		if err := db.conn.First(&updated, job.ID).Error; err != nil {
			t.Fatal(err)
		}
		if updated.SourceID != job.SourceID || updated.DestID != job.DestID || updated.State != fresh.State || updated.Frequency != job.Frequency {
			t.Fatal("policy save reset source/destination refs, frequency or CDC state")
		}
		if value(fresh.StreamsConfig) != value(updated.StreamsConfig) || value(fresh.AvailableStreamsConfig) != value(updated.AvailableStreamsConfig) || value(fresh.SelectedStreamsConfig) != value(updated.SelectedStreamsConfig) {
			t.Fatal("policy save reset saved catalogs/overrides")
		}
	}
}

func TestCatalogRevisionOptOutAndEnableTransitions(t *testing.T) {
	db := enrollmentTestDatabase(t)
	ctx := context.Background()
	job := savedEnrollmentJob(t, db, false, false)
	if err := db.UpdateJobWithCatalogRevision(ctx, job.ID, job.ProjectID, "", false, map[string]any{"name": "legacy edit"}); err != nil {
		t.Fatal(err)
	}
	if err := db.CheckCatalogRevision(ctx, job.ID, job.ProjectID, "", true); !errors.Is(err, constants.ErrCatalogRevision) {
		t.Fatalf("enabled without revision: %v", err)
	}
	// Even a caller that initially observed an off job must not overwrite a
	// concurrently enabled job without a revision at the final locked write.
	if err := db.UpdateJob(job.ID, map[string]any{"advanced_settings": `{"auto_table_enrollment":{"enabled":true}}`}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateJobWithCatalogRevision(ctx, job.ID, job.ProjectID, "", false, map[string]any{"name": "unsafe edit"}); !errors.Is(err, constants.ErrCatalogRevision) {
		t.Fatalf("concurrent enable bypassed guard: %v", err)
	}
	if err := db.CheckCatalogRevision(ctx, job.ID, "other-project", "", false); !errors.Is(err, constants.ErrJobNotFound) {
		t.Fatalf("cross-project row accepted: %v", err)
	}
}

func value(raw *string) string {
	if raw == nil {
		return ""
	}
	return *raw
}

func TestCatalogRevisionFinalWriteWaitsForConcurrentWorkerCommit(t *testing.T) {
	db := enrollmentTestDatabase(t)
	job := savedEnrollmentJob(t, db, true, true)
	revision, err := job.CatalogRevision()
	if err != nil {
		t.Fatal(err)
	}
	worker := db.conn.Begin()
	if worker.Error != nil {
		t.Fatal(worker.Error)
	}
	defer worker.Rollback()
	if err := worker.Model(&models.Job{}).Where("id = ?", job.ID).Update("selected_streams_config", `{"selected_streams":{"public":[{"stream_name":"orders"},{"stream_name":"new_table"}]}}`).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- db.UpdateJobWithCatalogRevision(ctx, job.ID, job.ProjectID, revision, true, map[string]any{"name": "stale while worker commits"})
	}()
	select {
	case err := <-result:
		t.Fatalf("final write bypassed the worker's row lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := worker.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, constants.ErrCatalogRevision) {
			t.Fatalf("final write did not reread committed catalog: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("final write never completed after worker released row lock")
	}
}

func TestCatalogRevisionEditFenceDoesNotBlockCancellationStateWrites(t *testing.T) {
	db := enrollmentTestDatabase(t)
	job := savedEnrollmentJob(t, db, true, true)
	pool, err := db.conn.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fencedContext, release, err := db.BeginCatalogEdit(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	revision, err := job.CatalogRevision()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CheckCatalogRevision(fencedContext, job.ID, job.ProjectID, revision, true); err != nil {
		t.Fatalf("fence exhausted the application pool: %v", err)
	}
	if _, otherRelease, err := db.BeginCatalogEdit(ctx, job.ID); !errors.Is(err, constants.ErrJobBusy) {
		if otherRelease != nil {
			otherRelease()
		}
		t.Fatalf("concurrent edit was not rejected before side effects: %v", err)
	}
	updatedState := `{"global":{"lsn":"0/DEF"},"streams":{"public.orders":{"cursor":43}}}`
	if err := db.conn.WithContext(ctx).Model(&models.Job{}).Where("id = ?", job.ID).Update("state", updatedState).Error; err != nil {
		t.Fatalf("fence blocked cancellation state persistence: %v", err)
	}
	if err := db.UpdateJobWithCatalogRevision(fencedContext, job.ID, job.ProjectID, revision, true, map[string]any{"name": "accepted edit"}); err != nil {
		t.Fatal(err)
	}
	var fresh models.Job
	if err := db.conn.WithContext(ctx).First(&fresh, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	var state struct {
		Global struct {
			LSN string `json:"lsn"`
		} `json:"global"`
	}
	if err := json.Unmarshal([]byte(fresh.State), &state); err != nil {
		t.Fatal(err)
	}
	if fresh.Name != "accepted edit" || state.Global.LSN != "0/DEF" {
		t.Fatal("accepted edit lost cancellation's persisted state")
	}
	release()
	_, nextRelease, err := db.BeginCatalogEdit(ctx, job.ID)
	if err != nil {
		t.Fatalf("released edit fence leaked its lock: %v", err)
	}
	nextRelease()
}

func TestCatalogEditFenceReentrancyIsBoundToDatabaseAndJob(t *testing.T) {
	db := enrollmentTestDatabase(t)
	job := savedEnrollmentJob(t, db, true, false)
	ctx, release, err := db.BeginCatalogEdit(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	nested, borrowedRelease, err := db.BeginCatalogEdit(ctx, job.ID)
	if err != nil || nested != ctx {
		t.Fatalf("same-job nested edit did not borrow its fence: %v", err)
	}
	borrowedRelease()
	if _, otherRelease, err := db.BeginCatalogEdit(context.Background(), job.ID); !errors.Is(err, constants.ErrJobBusy) {
		if otherRelease != nil {
			otherRelease()
		}
		t.Fatalf("borrowed release unlocked the outer edit: %v", err)
	}
	otherDB := &Database{conn: db.conn}
	if _, otherRelease, err := otherDB.BeginCatalogEdit(ctx, job.ID); !errors.Is(err, constants.ErrJobBusy) {
		if otherRelease != nil {
			otherRelease()
		}
		t.Fatalf("another Database instance borrowed the fence: %v", err)
	}
	otherContext, otherRelease, err := db.BeginCatalogEdit(ctx, job.ID+1)
	if err != nil {
		t.Fatal(err)
	}
	defer otherRelease()
	if otherContext == ctx {
		t.Fatal("another job borrowed the fence")
	}
	if _, competingRelease, err := db.BeginCatalogEdit(context.Background(), job.ID+1); !errors.Is(err, constants.ErrJobBusy) {
		if competingRelease != nil {
			competingRelease()
		}
		t.Fatalf("other job did not acquire its own real fence: %v", err)
	}
	otherRelease()
	release()
	if _, staleRelease, err := db.BeginCatalogEdit(ctx, job.ID); !errors.Is(err, context.Canceled) {
		if staleRelease != nil {
			staleRelease()
		}
		t.Fatalf("released context borrowed stale ownership: %v", err)
	}
	_, nextRelease, err := db.BeginCatalogEdit(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("outer release leaked the PostgreSQL fence: %v", err)
	}
	nextRelease()
}

func TestCatalogEditFenceSerializesMutationBeforeWorkerOwnership(t *testing.T) {
	db := enrollmentTestDatabase(t)
	job := savedEnrollmentJob(t, db, true, false)
	ctx, release, err := db.BeginCatalogEdit(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	workerContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pidReady := make(chan int, 1)
	finished := make(chan error, 1)
	go func() {
		finished <- db.conn.WithContext(workerContext).Transaction(func(tx *gorm.DB) error {
			var pid int
			if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
				return err
			}
			pidReady <- pid
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", fmt.Sprintf("olake:catalog-edit:%d", job.ID)).Error; err != nil {
				return err
			}
			result := tx.Model(&models.Job{}).Where("id = ? AND execution_workflow_id IS NULL", job.ID).Update("execution_workflow_id", "resync-worker")
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("worker ownership was not acquired")
			}
			return nil
		})
	}()
	var pid int
	select {
	case pid = <-pidReady:
	case err := <-finished:
		t.Fatalf("worker could not begin: %v", err)
	case <-workerContext.Done():
		t.Fatal(workerContext.Err())
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := db.conn.Raw("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = ? AND locktype = 'advisory' AND NOT granted)", pid).Scan(&waiting).Error; err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-finished:
			t.Fatalf("worker bypassed API fence: %v", err)
		case <-workerContext.Done():
			t.Fatal("worker never waited for API fence")
		case <-ticker.C:
		}
	}
	nested, nestedRelease, err := db.BeginCatalogEdit(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer nestedRelease()
	newState := `{"global":{"lsn":"0/DEF"}}`
	if err := db.conn.WithContext(nested).Model(&models.Job{}).Where("id = ? AND execution_workflow_id IS NULL", job.ID).Update("state", newState).Error; err != nil {
		t.Fatal(err)
	}
	nestedRelease()
	select {
	case err := <-finished:
		t.Fatalf("nested release unlocked outer fence: %v", err)
	default:
	}
	release()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-workerContext.Done():
		t.Fatal("worker remained blocked after API release")
	}
	var fresh models.Job
	if err := db.conn.First(&fresh, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if fresh.ExecutionWorkflowID == nil || *fresh.ExecutionWorkflowID != "resync-worker" {
		t.Fatal("worker claim missing")
	}
	var actual, expected interface{}
	if err := json.Unmarshal([]byte(fresh.State), &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(newState), &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatal("worker claim raced past the protected state mutation")
	}
	_, afterClaimRelease, err := db.BeginCatalogEdit(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("worker leaked its transaction fence: %v", err)
	}
	afterClaimRelease()
}
