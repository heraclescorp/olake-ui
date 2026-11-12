package etl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	etltemporal "github.com/datazip-inc/olake-ui/server/internal/services/temporal"
	"github.com/datazip-inc/olake-ui/server/internal/types"
	"github.com/lib/pq"
	"go.temporal.io/sdk/client"
)

func resyncFenceService(t *testing.T) (Service, *models.Job, *models.User) {
	t.Helper()
	db := enrollmentServiceDatabase(t)
	actor := &models.User{Username: "resync-editor", Email: "resync-editor@example.invalid", Password: "unused-test-hash"}
	if err := db.CreateUser(actor); err != nil {
		t.Fatal(err)
	}
	source := &models.Source{Name: "source", Type: "postgres", Version: "v1.1.1", Config: `{}`, ProjectID: "project-a", CreatedByID: actor.ID, UpdatedByID: actor.ID}
	if err := db.CreateSource(source); err != nil {
		t.Fatal(err)
	}
	destination := &models.Destination{Name: "destination", DestType: "iceberg", Version: "v1.1.1", Config: `{}`, ProjectID: "project-a", CreatedByID: actor.ID, UpdatedByID: actor.ID}
	if err := db.CreateDestination(destination); err != nil {
		t.Fatal(err)
	}
	job := resyncJob(t, true)
	job.ID, job.SourceID, job.DestID = 0, source.ID, destination.ID
	job.Source, job.Destination = nil, nil
	job.Name, job.Frequency = "fenced job", "@hourly"
	job.CreatedByID, job.UpdatedByID = actor.ID, actor.ID
	if err := db.CreateJob(job); err != nil {
		t.Fatal(err)
	}
	return Service{db: db, temporal: &etltemporal.Temporal{Client: idleEnrollmentWorkflowClient{}}}, job, actor
}

func TestResyncOwnerBlocksFencedJobMutationsAndReleasesLocks(t *testing.T) {
	service, job, actor := resyncFenceService(t)
	owner := "sync-project-a-resync-in-progress"
	if err := service.db.UpdateJob(job.ID, map[string]interface{}{"execution_workflow_id": owner}); err != nil {
		t.Fatal(err)
	}
	before, err := service.db.GetJobByID(job.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	beforeJSON, _ := json.Marshal(before)
	for _, mutation := range []struct {
		name string
		call func() error
	}{
		{name: "update", call: func() error {
			return service.UpdateJob(context.Background(), &dto.UpdateJobRequest{}, job.ProjectID, job.ID, &actor.ID)
		}},
		{name: "delete", call: func() error { _, err := service.DeleteJob(context.Background(), job.ID); return err }},
		{name: "sync", call: func() error { _, err := service.SyncJob(context.Background(), job.ProjectID, job.ID); return err }},
		{name: "activation", call: func() error {
			return service.ActivateJob(context.Background(), job.ID, dto.JobStatusRequest{Activate: false}, &actor.ID)
		}},
		{name: "clear", call: func() error {
			return service.ClearDestination(context.Background(), job.ProjectID, job.ID, types.StreamsCatalog{}, 0, true)
		}},
		{name: "resync", call: func() error {
			return service.ResyncTable(context.Background(), job.ProjectID, job.ID, dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"})
		}},
		{name: "state", call: func() error { return service.UpdateStateFile(context.Background(), job.ID, `{}`) }},
		{name: "clear recovery", call: func() error { return service.RecoverFromClearDestination(context.Background(), job.ProjectID, job.ID) }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			if err := mutation.call(); !errors.Is(err, constants.ErrJobBusy) {
				t.Fatalf("mutation bypassed execution owner: %v", err)
			}
			after, err := service.db.GetJobByID(job.ID, false)
			if err != nil {
				t.Fatal(err)
			}
			afterJSON, _ := json.Marshal(after)
			if string(afterJSON) != string(beforeJSON) {
				t.Fatal("rejected mutation changed authoritative job")
			}
			_, release, err := service.db.BeginCatalogEdit(context.Background(), job.ID)
			if err != nil {
				t.Fatalf("rejected mutation leaked fence: %v", err)
			}
			release()
		})
	}
}

type nestedClearClient struct {
	idleEnrollmentWorkflowClient
	handle *nestedClearHandle
}

func (c *nestedClearClient) ScheduleClient() client.ScheduleClient {
	return nestedClearScheduleClient{handle: c.handle}
}

type nestedClearScheduleClient struct {
	client.ScheduleClient
	handle *nestedClearHandle
}

func (c nestedClearScheduleClient) GetHandle(context.Context, string) client.ScheduleHandle {
	return c.handle
}

type nestedClearHandle struct {
	client.ScheduleHandle
	schedule client.Schedule
	trigger  func()
}

func (h *nestedClearHandle) Pause(context.Context, client.SchedulePauseOptions) error {
	h.schedule.State.Paused = true
	return nil
}

func (h *nestedClearHandle) Describe(context.Context) (*client.ScheduleDescription, error) {
	return &client.ScheduleDescription{Schedule: h.schedule}, nil
}

func (h *nestedClearHandle) Update(_ context.Context, options client.ScheduleUpdateOptions) error {
	update, err := options.DoUpdate(client.ScheduleUpdateInput{Description: client.ScheduleDescription{Schedule: h.schedule}})
	if err != nil {
		return err
	}
	h.schedule = *update.Schedule
	return nil
}

func (h *nestedClearHandle) Trigger(context.Context, client.ScheduleTriggerOptions) error {
	h.trigger()
	return nil
}

func TestNestedClearDispatchReturnsBeforeWorkerClaimsCatalogFence(t *testing.T) {
	service, job, _ := resyncFenceService(t)
	previousDir := constants.DefaultConfigDir
	constants.DefaultConfigDir = t.TempDir()
	t.Cleanup(func() { constants.DefaultConfigDir = previousDir })
	ctx, release, err := service.db.BeginCatalogEdit(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	workerContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	workerDB, err := sql.Open("pgx", os.Getenv("OLAKE_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer workerDB.Close()
	finished := make(chan error, 1)
	pidReady := make(chan int, 1)
	handle := &nestedClearHandle{schedule: client.Schedule{State: &client.ScheduleState{}}}
	handle.trigger = func() {
		go func() {
			tx, err := workerDB.BeginTx(workerContext, nil)
			if err != nil {
				finished <- err
				return
			}
			defer tx.Rollback()
			var pid int
			if err := tx.QueryRowContext(workerContext, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
				finished <- err
				return
			}
			pidReady <- pid
			_, err = tx.ExecContext(workerContext, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", fmt.Sprintf("olake:catalog-edit:%d", job.ID))
			if err == nil {
				_, err = tx.ExecContext(workerContext, "UPDATE "+pq.QuoteIdentifier(constants.TableNameMap[constants.JobTable])+" SET execution_workflow_id='clear-worker' WHERE id=$1 AND execution_workflow_id IS NULL", job.ID)
			}
			if err == nil {
				err = tx.Commit()
			}
			finished <- err
		}()
	}
	service.temporal = &etltemporal.Temporal{Client: &nestedClearClient{handle: handle}}
	// Like UpdateJob, the caller holds the fence while clear dispatches. Clear
	// waits for dispatch only; waiting for worker completion here would deadlock.
	if err := service.ClearDestination(ctx, job.ProjectID, job.ID, types.StreamsCatalog{}, 0, true); err != nil {
		t.Fatal(err)
	}
	action := handle.schedule.Action.(*client.ScheduleWorkflowAction)
	if action.Args[0].(etltemporal.ExecutionRequest).Command != etltemporal.ClearDestination {
		t.Fatal("clear action changed")
	}
	var pid int
	select {
	case pid = <-pidReady:
	case err := <-finished:
		t.Fatalf("clear worker failed to start: %v", err)
	case <-workerContext.Done():
		t.Fatal("clear dispatch waited for worker completion")
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := workerDB.QueryRowContext(workerContext, "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND NOT granted)", pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-finished:
			t.Fatalf("clear worker bypassed outer edit: %v", err)
		case <-workerContext.Done():
			t.Fatal("clear worker never waited for outer fence")
		case <-ticker.C:
		}
	}
	fresh, err := service.db.GetJobByID(job.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, []byte(fresh.State), []byte(`{}`))
	if fresh.ExecutionWorkflowID != nil {
		t.Fatal("worker claimed before outer mutation finished")
	}
	release()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-workerContext.Done():
		t.Fatal("clear worker stayed blocked after API return")
	}
	fresh, err = service.db.GetJobByID(job.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.ExecutionWorkflowID == nil || *fresh.ExecutionWorkflowID != "clear-worker" {
		t.Fatal("clear worker claim missing after API release")
	}
	if err := service.UpdateStateFile(context.Background(), job.ID, `{"unsafe":true}`); !errors.Is(err, constants.ErrJobBusy) {
		t.Fatalf("state edit raced past clear ownership: %v", err)
	}
}
