package temporal

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/datazip-inc/olake-ui/server/internal/storage"
	"github.com/datazip-inc/olake-ui/server/internal/types"
)

func TestClearDestinationWorkerRequestStagesScopedCatalog(t *testing.T) {
	if storage.Mode() != constants.StorageModeNFS {
		t.Fatal("test requires local config storage")
	}
	previous := constants.DefaultConfigDir
	constants.DefaultConfigDir = t.TempDir()
	t.Cleanup(func() { constants.DefaultConfigDir = previous })
	full := `{"streams":[{"stream":{"namespace":"public","name":"orders"}},{"stream":{"namespace":"other","name":"orders"}}],"selected_streams":{"public":[{"stream_name":"orders"}],"other":[{"stream_name":"orders"}]}}`
	available := `{"streams":[{"stream":{"namespace":"public","name":"orders","destination_table":"saved_target","json_schema":{"type":"object"}}}]}`
	selected := `{"selected_streams":{"public":[{"stream_name":"orders","sync_mode":"cdc","destination_table":"saved_target","selected_columns":{"columns":["amount"]}}]}}`
	legacy := `{"streams":[{"stream":{"namespace":"public","name":"orders","destination_table":"saved_target","json_schema":{"type":"object"}}}],"selected_streams":{"public":[{"stream_name":"orders","sync_mode":"cdc","destination_table":"saved_target","selected_columns":{"columns":["amount"]}}]}}`
	for _, split := range []bool{false, true} {
		for _, startSync := range []bool{false, true} {
			name := map[bool]string{false: "legacy", true: "split"}[split] + map[bool]string{false: "-ordinary", true: "-resync"}[startSync]
			t.Run(name, func(t *testing.T) {
				job := &models.Job{ID: 3, ProjectID: "project", Source: &models.Source{Type: "postgres", Version: "v0.8.2"}, StreamsConfig: &full, State: `{"global":{"lsn":"0/ABC"},"streams":{"other.orders":{"done":true}}}`}
				catalog := types.StreamsCatalog{Streams: legacy}
				if split {
					job.StreamsConfig = nil
					job.AvailableStreamsConfig, job.SelectedStreamsConfig = &available, &selected
					catalog = types.StreamsCatalog{Available: available, Selected: selected}
				}
				statePath := filepath.Join(constants.DefaultConfigDir, "state.json")
				if err := os.WriteFile(statePath, []byte(job.State), 0600); err != nil {
					t.Fatal(err)
				}
				before, err := json.Marshal(job)
				if err != nil {
					t.Fatal(err)
				}
				req, err := buildExecutionReqForClearDestination(context.Background(), job, name, catalog, startSync)
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := json.Marshal(req)
				if err != nil {
					t.Fatal(err)
				}
				var wire struct {
					StartSyncAfterClear *bool  `json:"start_sync_after_clear"`
					Version             string `json:"version"`
					TempPath            string `json:"temp_path"`
				}
				if err := json.Unmarshal(encoded, &wire); err != nil {
					t.Fatal(err)
				}
				if wire.StartSyncAfterClear == nil || *wire.StartSyncAfterClear != startSync {
					t.Fatalf("worker flag missing or wrong: %s", encoded)
				}
				if wire.Version != job.Source.Version || wire.TempPath == "" || req.Command != ClearDestination {
					t.Fatalf("invalid worker request: %s", encoded)
				}
				var wantArgs []string
				if split {
					wantArgs = []string{"clear-destination", "--state", "/mnt/config/state.json", "--destination", "/mnt/config/destination.json", "--available-streams", "/mnt/config/available_streams.json", "--selected-streams", "/mnt/config/selected_streams.json"}
					for filename, want := range map[string]string{constants.AvailableStreamsFile: available, constants.SelectedStreamsFile: selected} {
						got, err := storage.ReadFile(context.Background(), filepath.Join(filepath.Dir(wire.TempPath), filename))
						if err != nil {
							t.Fatal(err)
						}
						if string(got) != want {
							t.Fatalf("worker staged full/altered catalog instead of scoped file: %s", got)
						}
					}
				} else {
					wantArgs = []string{"clear-destination", "--state", "/mnt/config/state.json", "--destination", "/mnt/config/destination.json", "--streams", "/mnt/config/streams.json"}
					got, err := storage.ReadFile(context.Background(), wire.TempPath)
					if err != nil {
						t.Fatal(err)
					}
					if string(got) != legacy {
						t.Fatalf("worker staged full/altered catalog: %s", got)
					}
				}
				if !reflect.DeepEqual(req.Args, wantArgs) {
					t.Fatalf("invalid worker flags: %v", req.Args)
				}
				after, err := json.Marshal(job)
				if err != nil {
					t.Fatal(err)
				}
				state, err := os.ReadFile(statePath)
				if err != nil {
					t.Fatal(err)
				}
				if string(before) != string(after) || string(state) != job.State {
					t.Fatal("clear request preparation wiped saved config or state")
				}
				sync := buildExecutionReqForSync(job, "normal-sync")
				if sync.StartSyncAfterClear || sync.TempPath != "" || sync.Command != Sync || sync.Configs != nil {
					t.Fatalf("normal sync inherited clear-only scope/flag: %+v", sync)
				}
			})
		}
	}
}
