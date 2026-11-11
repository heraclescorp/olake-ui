package etl

import (
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

func resyncJob(t *testing.T, split bool) *models.Job {
	t.Helper()
	job := &models.Job{ID: 17, ProjectID: "project-a", Active: true, State: `{"global":{"lsn":"0/ABC"},"streams":{"public.orders":{"done":true},"other.orders":{"cursor":42}}}`, Source: &models.Source{Version: "v0.8.2"}}
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

func TestTableResyncUsesExactlySavedTableWithoutResettingJob(t *testing.T) {
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
			if len(available.Streams) != 1 || len(selected.Selected) != 1 || len(selected.Selected["public"]) != 1 {
				t.Fatalf("scope leaked to another namespace/table: %s / %s", availableJSON, selectedJSON)
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

func TestTableResyncRejectsUnsafeRequestsBeforeClear(t *testing.T) {
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
			if len(selected.Streams["public"]) != 1 || selected.Streams["public"][0].Mode != mode {
				t.Fatalf("backfill mode changed: %s", catalog.Selected)
			}
		})
	}
}
