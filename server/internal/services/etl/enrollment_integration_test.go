package etl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake-ui/server/internal/appconfig"
	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/database"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	etltemporal "github.com/datazip-inc/olake-ui/server/internal/services/temporal"
	"github.com/lib/pq"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

type idleEnrollmentWorkflowClient struct {
	client.Client
}

func (idleEnrollmentWorkflowClient) ListWorkflow(context.Context, *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	return &workflowservice.ListWorkflowExecutionsResponse{}, nil
}

func enrollmentServiceDatabase(t *testing.T) *database.Database {
	t.Helper()
	dsn := os.Getenv("OLAKE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set OLAKE_TEST_POSTGRES_DSN to an isolated local PostgreSQL URL")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" && parsed.Hostname() != "::1") {
		t.Fatal("service integration test requires a local PostgreSQL URL")
	}
	t.Setenv("POSTGRES_DB", dsn)
	t.Setenv("SESSION_ON", "false")
	if cfg := appconfig.Load(); cfg.PostgresDSN != dsn || cfg.SessionOn {
		t.Skip("set POSTGRES_DB=OLAKE_TEST_POSTGRES_DSN and SESSION_ON=false before starting the test process")
	}
	originalTables := constants.TableNameMap
	prefix := fmt.Sprintf("enrollment_service_%d_", time.Now().UnixNano())
	constants.TableNameMap = map[constants.TableType]string{
		constants.JobTable: prefix + "jobs", constants.UserTable: prefix + "users",
		constants.SourceTable: prefix + "sources", constants.DestinationTable: prefix + "destinations",
		constants.CatalogTable: prefix + "catalogs", constants.SessionTable: prefix + "sessions",
		constants.ProjectSettingsTable: prefix + "project_settings",
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	tables := make([]string, 0, len(constants.TableNameMap))
	for _, name := range constants.TableNameMap {
		tables = append(tables, pq.QuoteIdentifier(name))
	}
	tables = append(tables, pq.QuoteIdentifier((&models.JobExecutionRecord{}).TableName()))
	t.Cleanup(func() {
		if _, err := admin.Exec("DROP TABLE IF EXISTS " + strings.Join(tables, ",") + " CASCADE"); err != nil {
			t.Error(err)
		}
		_ = admin.Close()
		constants.TableNameMap = originalTables
	})
	db, err := database.Init()
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestEnrollmentCatalogSaveAndResyncFence(t *testing.T) {
	db := enrollmentServiceDatabase(t)
	actor := &models.User{Username: "catalog-editor", Email: "catalog-editor@example.invalid", Password: "unused-test-hash"}
	if err := db.CreateUser(actor); err != nil {
		t.Fatal(err)
	}
	source := &models.Source{Name: "source", Type: "postgres", Version: "v1.1.1", Config: `{}`, ProjectID: "project-a", CreatedByID: actor.ID, UpdatedByID: actor.ID}
	if err := db.CreateSource(source); err != nil {
		t.Fatal(err)
	}
	destination := &models.Destination{Name: "destination", DestType: "iceberg", Version: "v1.1.1", Config: resyncDestinationConfig, ProjectID: "project-a", CreatedByID: actor.ID, UpdatedByID: actor.ID}
	if err := db.CreateDestination(destination); err != nil {
		t.Fatal(err)
	}
	service := Service{db: db, temporal: &etltemporal.Temporal{Client: idleEnrollmentWorkflowClient{}}}
	for _, split := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "split"}[split], func(t *testing.T) {
			job := enrollmentJob(t, split)
			job.ID, job.SourceID, job.DestID = 0, source.ID, destination.ID
			job.Source, job.Destination = nil, nil
			job.Name, job.Frequency = "catalog save", "@hourly"
			job.CreatedByID, job.UpdatedByID = actor.ID, actor.ID
			settings := validEnrollmentSettings
			job.AdvancedSettings = &settings
			if err := db.CreateJob(job); err != nil {
				t.Fatal(err)
			}
			revision, err := job.CatalogRevision()
			if err != nil {
				t.Fatal(err)
			}
			catalog := job.StreamsCatalog()
			availableJSON := catalog.Streams
			if split {
				availableJSON = catalog.Available
			}
			var available map[string]json.RawMessage
			if err := json.Unmarshal([]byte(availableJSON), &available); err != nil {
				t.Fatal(err)
			}
			var definitions []json.RawMessage
			if err := json.Unmarshal(available["streams"], &definitions); err != nil {
				t.Fatal(err)
			}
			definitions = append(definitions, json.RawMessage(`{"stream":{"namespace":"new_schema","name":"events","destination_database":"warehouse:new_schema","sync_mode":"cdc"}}`))
			available["streams"], err = json.Marshal(definitions)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(available)
			if err != nil {
				t.Fatal(err)
			}
			policy := enrollmentSettings(t, validEnrollmentSettings)
			policy.AutoTableEnrollment.Schemas = append(policy.AutoTableEnrollment.Schemas, dto.AutoTableEnrollmentSchema{Namespace: "new_schema", DestinationDatabase: "warehouse:new_schema"})
			req := &dto.UpdateJobRequest{
				JobMetadata:   dto.JobMetadata{Name: job.Name, Frequency: job.Frequency, Activate: true, AdvancedSettings: policy},
				Source:        &dto.DriverConfig{ID: &source.ID, Type: source.Type, Version: source.Version},
				Destination:   &dto.DriverConfig{ID: &destination.ID, Type: destination.DestType, Version: destination.Version},
				StreamsConfig: string(encoded), DifferenceStreams: "{}", CatalogRevision: revision,
			}
			if split {
				req.StreamsConfig, req.AvailableStreamsConfig, req.SelectedStreamsConfig = "", string(encoded), catalog.Selected
			}
			if err := service.UpdateJob(context.Background(), req, job.ProjectID, job.ID, &actor.ID); err != nil {
				t.Fatalf("explicit discovered catalog save failed: %v", err)
			}
			fresh, err := db.GetJobByID(job.ID, false)
			if err != nil {
				t.Fatal(err)
			}
			assertJSONEqual(t, []byte(fresh.State), []byte(job.State))
			if fresh.SourceID != source.ID || fresh.DestID != destination.ID {
				t.Fatal("catalog save changed driver references")
			}
			var savedSettings dto.AdvancedSettings
			if err := json.Unmarshal([]byte(*fresh.AdvancedSettings), &savedSettings); err != nil {
				t.Fatal(err)
			}
			if len(savedSettings.AutoTableEnrollment.Schemas) != 2 || savedSettings.AutoTableEnrollment.Schemas[1].Namespace != "new_schema" {
				t.Fatal("explicitly discovered namespace mapping was not persisted")
			}
			if err := service.UpdateJob(context.Background(), req, job.ProjectID, job.ID, &actor.ID); !errors.Is(err, constants.ErrCatalogRevision) {
				t.Fatalf("stale catalog save accepted: %v", err)
			}
			_, release, err := db.BeginCatalogEdit(context.Background(), job.ID)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			err = service.ResyncTable(context.Background(), job.ProjectID, job.ID, dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"})
			if !errors.Is(err, constants.ErrJobBusy) {
				t.Fatalf("resync bypassed catalog edit fence: %v", err)
			}
			after, err := db.GetJobByID(job.ID, false)
			if err != nil {
				t.Fatal(err)
			}
			assertJSONEqual(t, []byte(after.State), []byte(fresh.State))
			if after.StreamsCatalog() != fresh.StreamsCatalog() {
				t.Fatal("blocked resync changed the saved catalog")
			}
			release()
			if err := service.ResyncTable(context.Background(), job.ProjectID, job.ID, dto.ResyncTableRequest{Namespace: "missing", StreamName: "orders"}); !errors.Is(err, constants.ErrInvalidResync) {
				t.Fatalf("released fence did not reach current table validation: %v", err)
			}
		})
	}
}
