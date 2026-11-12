package etl

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	"github.com/datazip-inc/olake-ui/server/internal/types"
)

const resyncAvailable = `{"catalog_metadata":{"revision":7},"streams":[{"stream":{"name":"orders","namespace":"public","sync_mode":"cdc","destination_database":"archive","destination_table":"orders_backup","json_schema":{"properties":{"amount":{"type":"number"}}}},"metadata":{"keep":true}},{"stream":{"name":"orders","namespace":"other","sync_mode":"cdc"}},{"stream":{"name":"users","namespace":"public","sync_mode":"full_refresh"}}]}`
const resyncSelected = `{"selection_metadata":{"keep":true},"selected_streams":{"public":[{"stream_name":"orders","sync_mode":"cdc","destination_database":"warehouse","destination_table":"paid_orders","selected_columns":{"columns":["amount"],"sync_new_columns":false},"partition_regex":"dt={yyyy}","custom":{"keep":true}},{"stream_name":"users"}],"other":[{"stream_name":"orders","sync_mode":"cdc"}]}}`
const resyncDestinationConfig = `{"type":"ICEBERG","writer":{"catalog_type":"rest"}}`

func resyncJob(t *testing.T, split bool) *models.Job {
	t.Helper()
	job := &models.Job{ID: 17, ProjectID: "project-a", Active: true, State: `{"global":{"lsn":"0/ABC"},"streams":{"public.orders":{"done":true},"other.orders":{"cursor":42}}}`, Source: &models.Source{Type: "postgres", Version: "v0.8.2"}, Destination: &models.Destination{DestType: "iceberg", Config: resyncDestinationConfig}}
	if split {
		available, selected := resyncAvailable, resyncSelected
		job.AvailableStreamsConfig, job.SelectedStreamsConfig = &available, &selected
	} else {
		var available, selected map[string]json.RawMessage
		if err := json.Unmarshal([]byte(resyncAvailable), &available); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(resyncSelected), &selected); err != nil {
			t.Fatal(err)
		}
		for key, value := range selected {
			available[key] = value
		}
		data, err := json.Marshal(available)
		if err != nil {
			t.Fatal(err)
		}
		legacy := string(data)
		job.StreamsConfig = &legacy
	}
	return job
}

func TestTableResyncPreservesFullSavedCatalogAndJobState(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "split"}[split], func(t *testing.T) {
			job := resyncJob(t, split)
			before, err := json.Marshal(job)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := resolveTableResync(job, "project-a", dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"})
			if err != nil {
				t.Fatal(err)
			}
			if catalog.IsSplit() != split {
				t.Fatal("changed the saved catalog format")
			}
			if catalog != job.StreamsCatalog() {
				t.Fatal("resync must retain the entire saved catalog byte-for-byte")
			}
			availableJSON, selectedJSON := catalog.Streams, catalog.Streams
			if split {
				availableJSON, selectedJSON = catalog.Available, catalog.Selected
			}
			var available struct {
				Metadata json.RawMessage   `json:"catalog_metadata"`
				Streams  []json.RawMessage `json:"streams"`
			}
			var selected struct {
				Metadata json.RawMessage              `json:"selection_metadata"`
				Selected map[string][]json.RawMessage `json:"selected_streams"`
			}
			if err := json.Unmarshal([]byte(availableJSON), &available); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(selectedJSON), &selected); err != nil {
				t.Fatal(err)
			}
			if len(available.Streams) != 3 || len(selected.Selected) != 2 || len(selected.Selected["public"]) != 2 {
				t.Fatalf("resync lost unrelated streams: %s / %s", availableJSON, selectedJSON)
			}
			var savedAvailable struct {
				Streams []json.RawMessage `json:"streams"`
			}
			var savedSelected struct {
				Selected map[string][]json.RawMessage `json:"selected_streams"`
			}
			if err := json.Unmarshal([]byte(resyncAvailable), &savedAvailable); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(resyncSelected), &savedSelected); err != nil {
				t.Fatal(err)
			}
			assertJSONEqual(t, available.Streams[0], savedAvailable.Streams[0])
			assertJSONEqual(t, selected.Selected["public"][0], savedSelected.Selected["public"][0])
			assertJSONEqual(t, available.Metadata, []byte(`{"revision":7}`))
			assertJSONEqual(t, selected.Metadata, []byte(`{"keep":true}`))
			after, err := json.Marshal(job)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatalf("resync mutated saved job, version, catalog or global/other stream state: %s", after)
			}
		})
	}
}

func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()
	var g, w interface{}
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("JSON mismatch: got %s, want %s", got, want)
	}
}

func TestTableResyncRejectsUnsafeRequestsBeforeLaunch(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			req     dto.ResyncTableRequest
			project string
			change  func(*models.Job)
			want    error
		}{
			{name: "missing namespace", req: dto.ResyncTableRequest{StreamName: "orders"}, want: constants.ErrInvalidResync},
			{name: "blank namespace", req: dto.ResyncTableRequest{Namespace: "  ", StreamName: "orders"}, want: constants.ErrInvalidResync},
			{name: "missing table", req: dto.ResyncTableRequest{Namespace: "public"}, want: constants.ErrInvalidResync},
			{name: "blank table", req: dto.ResyncTableRequest{Namespace: "public", StreamName: "\t"}, want: constants.ErrInvalidResync},
			{name: "unknown namespace", req: dto.ResyncTableRequest{Namespace: "missing", StreamName: "orders"}, want: constants.ErrInvalidResync},
			{name: "unknown table", req: dto.ResyncTableRequest{Namespace: "public", StreamName: "missing"}, want: constants.ErrInvalidResync},
			{name: "wrong project", project: "project-b", want: constants.ErrJobNotFound},
			{name: "paused", change: func(j *models.Job) { j.Active = false }, want: constants.ErrJobBusy},
			{name: "execution awaiting reconciliation", change: func(j *models.Job) { owner := "sync-project-a-17-resync-pending"; j.ExecutionWorkflowID = &owner }, want: constants.ErrJobBusy},
			{name: "unsupported source", change: func(j *models.Job) { j.Source.Type = "mysql" }, want: constants.ErrInvalidResync},
			{name: "unsupported destination", change: func(j *models.Job) { j.Destination.DestType = "parquet" }, want: constants.ErrInvalidResync},
			{name: "missing source", change: func(j *models.Job) { j.Source = nil }, want: constants.ErrInvalidResync},
			{name: "missing destination", change: func(j *models.Job) { j.Destination = nil }, want: constants.ErrInvalidResync},
			{name: "unselected", change: func(j *models.Job) { replaceSavedSelection(j, `"stream_name":"orders"`, `"stream_name":"not_orders"`) }, want: constants.ErrInvalidResync},
			{name: "disabled", change: func(j *models.Job) {
				replaceSavedSelection(j, `"stream_name":"orders"`, `"stream_name":"orders","disabled":true`)
			}, want: constants.ErrInvalidResync},
			{name: "CDC only selection overrides catalog", change: func(j *models.Job) { replaceSavedSelection(j, `"sync_mode":"cdc"`, `"sync_mode":"strict_cdc"`) }, want: constants.ErrInvalidResync},
			{name: "unknown mode", change: func(j *models.Job) { replaceSavedSelection(j, `"sync_mode":"cdc"`, `"sync_mode":"unknown"`) }, want: constants.ErrInvalidResync},
		} {
			t.Run(map[bool]string{false: "legacy/", true: "split/"}[split]+tc.name, func(t *testing.T) {
				job := resyncJob(t, split)
				if tc.change != nil {
					tc.change(job)
				}
				project := tc.project
				if project == "" {
					project = "project-a"
				}
				req := tc.req
				if tc.change != nil || tc.project != "" {
					req = dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"}
				}
				catalog, err := resolveTableResync(job, project, req)
				if !errors.Is(err, tc.want) {
					t.Fatalf("got %v, want %v", err, tc.want)
				}
				if catalog != (types.StreamsCatalog{}) {
					t.Fatalf("invalid request returned a destructive scope: %+v", catalog)
				}
			})
		}
	}
}

func replaceSavedSelection(job *models.Job, old, replacement string) {
	config := job.StreamsConfig
	if job.SelectedStreamsConfig != nil {
		config = job.SelectedStreamsConfig
	}
	*config = strings.ReplaceAll(*config, old, replacement)
}

func TestTableResyncAcceptsBackfillModes(t *testing.T) {
	for _, mode := range []string{"cdc", "full_refresh", "incremental"} {
		t.Run(mode, func(t *testing.T) {
			job := resyncJob(t, true)
			replaceSavedSelection(job, `"sync_mode":"cdc"`, `"sync_mode":"`+mode+`"`)
			catalog, err := resolveTableResync(job, "project-a", dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"})
			if err != nil {
				t.Fatal(err)
			}
			var selected struct {
				Streams map[string][]struct {
					Mode string `json:"sync_mode"`
				} `json:"selected_streams"`
			}
			if err := json.Unmarshal([]byte(catalog.Selected), &selected); err != nil {
				t.Fatal(err)
			}
			if len(selected.Streams["public"]) != 2 || selected.Streams["public"][0].Mode != mode {
				t.Fatalf("backfill mode changed: %s", catalog.Selected)
			}
		})
	}
}

func TestResyncAcceptsCanonicalRESTCatalogFamilyIncludingS3Tables(t *testing.T) {
	for _, catalog := range []string{"rest", "lakekeeper", "nessie", "s3tables", "unity", "polaris", "biglake", "horizon"} {
		t.Run(catalog, func(t *testing.T) {
			job := resyncJob(t, true)
			job.Source.Type, job.Destination.DestType = " Postgres ", " Iceberg "
			config, err := json.Marshal(map[string]any{"type": " iceberg ", "writer": map[string]string{"catalog_type": " " + strings.ToUpper(catalog) + " "}})
			if err != nil {
				t.Fatal(err)
			}
			job.Destination.Config = string(config)
			if err := validateAtomicResyncSupport(job); err != nil {
				t.Fatalf("canonical REST-family catalog was excluded: %v", err)
			}
		})
	}
}

func TestResyncRejectsSavedCatalogOutsideRESTFamilyBeforeScheduling(t *testing.T) {
	for _, unsupported := range []struct {
		name string
		config string
	}{
		{name: "missing saved type", config: `{}`},
		{name: "default Glue", config: `{"type":"ICEBERG"}`},
		{name: "empty writer catalog", config: `{"type":"ICEBERG","writer":{"catalog_type":""}}`},
		{name: "root catalog is not writer catalog", config: `{"type":"ICEBERG","catalog_type":"rest"}`},
		{name: "explicit Glue", config: `{"type":"ICEBERG","writer":{"catalog_type":"glue"}}`},
		{name: "Hive", config: `{"type":"ICEBERG","writer":{"catalog_type":"hive"}}`},
		{name: "unknown catalog", config: `{"type":"ICEBERG","writer":{"catalog_type":"unknown"}}`},
		{name: "Parquet canonical type", config: `{"type":"PARQUET","writer":{"catalog_type":"rest"}}`},
		{name: "malformed saved config", config: `{`},
		{name: "invalid writer type", config: `{"type":"ICEBERG","writer":[]}`},
	} {
		t.Run(unsupported.name, func(t *testing.T) {
			job := resyncJob(t, true)
			job.Destination.Config = unsupported.config
			before, err := json.Marshal(job)
			if err != nil {
				t.Fatal(err)
			}
			// No Temporal client: even inspecting or pausing the schedule is a
			// failure. Only canonical saved configuration can enter the path.
			err = (Service{}).startTableResync(context.Background(), job, dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"})
			if !errors.Is(err, constants.ErrInvalidResync) {
				t.Fatalf("unsupported saved catalog reached scheduling: %v", err)
			}
			after, err := json.Marshal(job)
			if err != nil || string(after) != string(before) {
				t.Fatal("catalog rejection changed authoritative job configuration")
			}
		})
	}
}
