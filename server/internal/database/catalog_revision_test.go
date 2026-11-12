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
	if err := conn.AutoMigrate(&models.Job{}, &models.JobExecutionRecord{}); err != nil {
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
		if err := db.UpdateJob(ctx, job.ID, map[string]any{field: proposal}); err != nil {
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
	if err := db.UpdateJob(ctx, job.ID, map[string]any{"advanced_settings": `{"auto_table_enrollment":{"enabled":true}}`}); err != nil {
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

func TestCancelledCatalogEditCannotWriteAfterWorkerClaims(t *testing.T) {
	db := enrollmentTestDatabase(t)
	for _, mutation := range []struct {
		name  string
		apply func(context.Context, *models.Job) error
	}{
		{name: "state", apply: func(ctx context.Context, job *models.Job) error {
			return db.UpdateJob(ctx, job.ID, map[string]any{"state": `{"stale":true}`})
		}},
		{name: "activation", apply: func(ctx context.Context, job *models.Job) error {
			return db.UpdateJob(ctx, job.ID, map[string]any{"active": false})
		}},
		{name: "config", apply: func(ctx context.Context, job *models.Job) error {
			return db.UpdateJobWithCatalogRevision(ctx, job.ID, job.ProjectID, "", false, map[string]any{"name": "stale config", "selected_streams_config": `{"selected_streams":{}}`})
		}},
		{name: "delete", apply: func(ctx context.Context, job *models.Job) error {
			return db.DeleteJob(ctx, job.ID)
		}},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			job := savedEnrollmentJob(t, db, true, false)
			requestContext, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			fencedContext, release, err := db.BeginCatalogEdit(requestContext, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			var idleSnapshot models.Job
			if err := db.conn.First(&idleSnapshot, job.ID).Error; err != nil {
				t.Fatal(err)
			}
			if idleSnapshot.ExecutionWorkflowID != nil || idleSnapshot.ExecutionRunID != nil {
				t.Fatal("request did not observe an idle job")
			}
			workerContext, cancelWorker := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelWorker()
			pidReady := make(chan int, 1)
			claimed := make(chan error, 1)
			go func() {
				claimed <- db.conn.WithContext(workerContext).Transaction(func(tx *gorm.DB) error {
					var pid int
					if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
						return err
					}
					pidReady <- pid
					if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", fmt.Sprintf("olake:catalog-edit:%d", job.ID)).Error; err != nil {
						return err
					}
					result := tx.Model(&models.Job{}).Where("id = ? AND execution_workflow_id IS NULL AND execution_run_id IS NULL", job.ID).
						Updates(map[string]any{"execution_workflow_id": "replacement-operation", "execution_run_id": "replacement-run"})
					if result.Error != nil {
						return result.Error
					}
					if result.RowsAffected != 1 {
						return fmt.Errorf("worker did not claim idle job")
					}
					return nil
				})
			}()
			var pid int
			select {
			case pid = <-pidReady:
			case err := <-claimed:
				t.Fatalf("worker did not reach claim barrier: %v", err)
			case <-workerContext.Done():
				t.Fatal(workerContext.Err())
			}
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				var waiting bool
				if err := db.conn.WithContext(workerContext).Raw("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = ? AND locktype = 'advisory' AND NOT granted)", pid).Scan(&waiting).Error; err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case err := <-claimed:
					t.Fatalf("worker bypassed active request fence: %v", err)
				case <-workerContext.Done():
					t.Fatal("worker never reached PostgreSQL advisory wait")
				case <-ticker.C:
				}
			}
			// Do not call release: database/sql must roll the request transaction
			// back on cancellation while the stale handler is still alive.
			cancelRequest()
			select {
			case err := <-claimed:
				if err != nil {
					t.Fatal(err)
				}
			case <-workerContext.Done():
				t.Fatal("cancelled request kept its advisory lock")
			}
			if err := mutation.apply(fencedContext, &idleSnapshot); err == nil {
				t.Fatal("cancelled stale request mutated worker-owned job")
			}
			// The owner predicate, not only a context precheck, must reject a
			// late write even if cleanup uses a cancellation-independent context.
			if err := mutation.apply(context.WithoutCancel(fencedContext), &idleSnapshot); !errors.Is(err, constants.ErrJobBusy) {
				t.Fatalf("late stale write bypassed current worker ownership: %v", err)
			}
			var fresh models.Job
			if err := db.conn.First(&fresh, job.ID).Error; err != nil {
				t.Fatal(err)
			}
			if fresh.ExecutionWorkflowID == nil || *fresh.ExecutionWorkflowID != "replacement-operation" || fresh.ExecutionRunID == nil || *fresh.ExecutionRunID != "replacement-run" {
				t.Fatal("stale API write changed replacement epoch")
			}
			if fresh.Name != idleSnapshot.Name || fresh.Active != idleSnapshot.Active || fresh.State != idleSnapshot.State || *fresh.SelectedStreamsConfig != *idleSnapshot.SelectedStreamsConfig {
				t.Fatal("late request changed authoritative worker inputs")
			}
		})
	}
}

func TestPendingHandoffRemainsAuthorityWithoutJobOwner(t *testing.T) {
	db := enrollmentTestDatabase(t)
	job := savedEnrollmentJob(t, db, false, false)
	record := &models.JobExecutionRecord{JobID: job.ID, WorkflowID: "published-operation", OwnerRunID: "published-run", Phase: "handoff", StateSHA256: "published-checkpoint", HandoffToken: "transfer-token", HandoffActive: true, HandoffRequestID: "stable-patch-id"}
	if err := db.conn.Create(record).Error; err != nil {
		t.Fatal(err)
	}
	if _, release, err := db.BeginCatalogEdit(context.Background(), job.ID); !errors.Is(err, constants.ErrJobBusy) {
		if release != nil {
			release()
		}
		t.Fatalf("pending durable handoff was not busy: %v", err)
	}
	if err := db.UpdateJob(context.Background(), job.ID, map[string]any{"state": `{}`, "active": false}); !errors.Is(err, constants.ErrJobBusy) {
		t.Fatalf("state/pause bypassed pending handoff: %v", err)
	}
	if err := db.UpdateJobWithCatalogRevision(context.Background(), job.ID, job.ProjectID, "", false, map[string]any{"name": "stale"}); !errors.Is(err, constants.ErrJobBusy) {
		t.Fatalf("config bypassed pending handoff: %v", err)
	}
	if err := db.DeleteJob(context.Background(), job.ID); !errors.Is(err, constants.ErrJobBusy) {
		t.Fatalf("delete bypassed pending handoff: %v", err)
	}
	if updated, err := db.SetStreamsV2Catalog(context.Background(), ConvertedCatalog{JobID: job.ID, From: *job.StreamsConfig, Available: `{"streams":[]}`, Selected: `{"selected_streams":{}}`}); err != nil || updated {
		t.Fatalf("linked source migration rewrote pending handoff catalog: updated=%t err=%v", updated, err)
	}
	if err := db.conn.Model(record).Update("phase", "handed_off").Error; err != nil {
		t.Fatal(err)
	}
	ctx, release, err := db.BeginCatalogEdit(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("handed-off record kept API authority: %v", err)
	}
	defer release()
	if err := db.UpdateJob(ctx, job.ID, map[string]any{"active": false}); err != nil {
		t.Fatalf("completed handoff blocked user pause: %v", err)
	}
}

func TestExecutionAuthorityPostgresMigrationContract(t *testing.T) {
	db := enrollmentTestDatabase(t)
	table := (&models.JobExecutionRecord{}).TableName()
	var columns []struct {
		ColumnName    string
		DataType      string
		IsNullable    string
		ColumnDefault *string
	}
	if err := db.conn.Raw("SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ?", table).Scan(&columns).Error; err != nil {
		t.Fatal(err)
	}
	wantTypes := map[string]string{
		"job_id": "integer", "workflow_id": "text", "owner_run_id": "text",
		"launch_id": "text", "launcher_attempt_id": "text", "phase": "text",
		"runtime_id": "text", "resource_id": "text", "resource_uid": "text",
		"outcome": "text", "state_sha256": "text", "catalog_sha256": "text",
		"handoff_token": "text", "handoff_request_id": "text", "handoff_active": "boolean",
		"handoff_run_id": "text",
		"created_at":     "timestamp with time zone", "updated_at": "timestamp with time zone",
	}
	if len(columns) != len(wantTypes) {
		t.Fatalf("execution record column count: got %d want %d", len(columns), len(wantTypes))
	}
	for _, column := range columns {
		if column.DataType != wantTypes[column.ColumnName] || column.IsNullable != "NO" {
			t.Fatalf("execution column %s has type %s nullable=%s", column.ColumnName, column.DataType, column.IsNullable)
		}
		if column.ColumnName == "job_id" || column.ColumnName == "workflow_id" {
			if column.ColumnDefault != nil {
				t.Fatalf("execution identity %s must not be generated", column.ColumnName)
			}
			continue
		}
		wantDefault := "''::text"
		switch column.ColumnName {
		case "handoff_active":
			wantDefault = "false"
		case "created_at", "updated_at":
			wantDefault = "now()"
		}
		if column.ColumnDefault == nil || *column.ColumnDefault != wantDefault {
			t.Fatalf("execution column %s has wrong default: %v", column.ColumnName, column.ColumnDefault)
		}
	}
	var ownerColumns int64
	if err := db.conn.Raw("SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ? AND column_name IN ('execution_workflow_id','execution_run_id') AND data_type = 'text' AND is_nullable = 'YES'", constants.TableNameMap[constants.JobTable]).Scan(&ownerColumns).Error; err != nil || ownerColumns != 2 {
		t.Fatalf("nullable internal epoch columns: %d, %v", ownerColumns, err)
	}
	var indexed bool
	if err := db.conn.Raw("SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND tablename = ? AND indexdef LIKE '%(job_id, phase)')", table).Scan(&indexed).Error; err != nil || !indexed {
		t.Fatalf("pending handoff index missing: %t, %v", indexed, err)
	}
	job := savedEnrollmentJob(t, db, true, false)
	record := &models.JobExecutionRecord{JobID: job.ID, WorkflowID: "closed-operation", OwnerRunID: "old-run", Phase: "closed"}
	if err := db.conn.Create(record).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateJob(context.Background(), job.ID, map[string]any{"name": "new idle intent"}); err != nil {
		t.Fatal(err)
	}
	var retained int64
	if err := db.conn.Model(&models.JobExecutionRecord{}).Where("job_id = ? AND workflow_id = ?", job.ID, record.WorkflowID).Count(&retained).Error; err != nil || retained != 1 {
		t.Fatalf("released operation lost admission tombstone: %d, %v", retained, err)
	}
	if err := db.DeleteJob(context.Background(), job.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.conn.Model(&models.JobExecutionRecord{}).Where("job_id = ?", job.ID).Count(&retained).Error; err != nil || retained != 1 {
		t.Fatalf("job deletion removed permanent admission tombstone: %d, %v", retained, err)
	}
}
