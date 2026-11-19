package reconciler

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseStreamsCM(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr string
	}{
		{name: "selection only", config: userSelectedStreams},
		{name: "sort policies and extension metadata", config: `{"default_sort_order":[{"column":"created_at","direction":"asc","null_order":"nulls-last"}],"extension":{"keep":9007199254740993},"selected_streams":{"public":[{"stream_name":"inherit"},{"stream_name":"unsorted","sort_order":[]},{"stream_name":"custom","sort_order":[{"column":"amount","direction":"desc","null_order":"nulls-first"},{"column":"id","direction":"asc","null_order":"nulls-last"}]}]}}`},
		{name: "COW policy and nested extensions", config: `{"selected_streams":{"public":[{"stream_name":"users","merge_strategy":"copy-on-write","update_type":"dv","cow_options":{"flush_interval_seconds":300,"max_batch_rows":100000,"max_batch_bytes":67108864,"rewrite_concurrency":2,"materialize_existing_deletes":true,"extension":{"large":9007199254740993}}},{"stream_name":"append","append_mode":true,"merge_strategy":"copy-on-write"}]}}`},
		{name: "empty streams array is not legacy", config: `{"streams":[],"selected_streams":{"public":[]}}`},
		{name: "null streams is not legacy", config: `{"streams":null,"selected_streams":{"public":[]}}`},
		{name: "invalid JSON", config: `{"selected_streams":`, wantErr: "invalid streams config"},
		{name: "legacy streams.json", config: `{"streams":[{"stream":{"name":"users"}}],"selected_streams":{"public":[]}}`, wantErr: "legacy streams config (streams[]) is not supported"},
		{name: "legacy streams without selection", config: `{"streams":[{"stream":{"name":"users"}}]}`, wantErr: "legacy streams config"},
		{name: "no selected_streams", config: `{}`, wantErr: "streams config has no selected_streams"},
		{name: "null selected_streams", config: `{"selected_streams":null}`, wantErr: "streams config has no selected_streams"},
		{name: "empty selected_streams", config: `{"selected_streams":{}}`, wantErr: "streams config has no selected_streams"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog, err := parseStreamsCM(tt.config)
			if tt.wantErr != "" {
				assert.ErrorIs(t, err, ErrNonRetryable)
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)
			// the ConfigMap is stored as written; discover fills the available streams later
			assert.Equal(t, tt.config, catalog.Selected)
			assert.Empty(t, catalog.Available)
			assert.Empty(t, catalog.Streams)
		})
	}
}

func TestGitOpsSortPoliciesSurviveDiscoverCreateAndUpdate(t *testing.T) {
	const preset = `[{"column":"created_at","direction":"asc","null_order":"nulls-last"}]`
	selection := `{"default_sort_order":` + preset + `,"extension":{"keep":9007199254740993},"selected_streams":{"public":[{"stream_name":"users","sort_order":[{"column":"id","direction":"desc","null_order":"nulls-first"}]},{"stream_name":"unavailable","sort_order":[]},{"stream_name":"inherit"}]}}`
	selection = strings.Replace(selection, `"stream_name":"users"`, `"stream_name":"users","merge_strategy":"copy-on-write","update_type":"dv","cow_options":{"flush_interval_seconds":300,"max_batch_rows":100000,"max_batch_bytes":67108864,"rewrite_concurrency":2,"materialize_existing_deletes":true,"extension":{"large":9007199254740993}}`, 1)
	e, _, _ := newJobTestEnv(t, nil)
	e.setData(streamsConfigMap, streamsManifestData(jobConfigMap, selection))
	e.reconcileJob(jobConfigMap)
	if !assert.Equal(t, PhaseReady, e.phaseOf(jobConfigMap)) || !assert.Len(t, createJobRequests(e.etl), 1) {
		return
	}
	assert.Equal(t, selection, createJobRequests(e.etl)[0].SelectedStreamsConfig)
	assert.Equal(t, selection, *e.etl.jobs["orders"].SelectedStreamsConfig)
	e.resetRecordings()
	selection = strings.Replace(selection, `"default_sort_order":`+preset, `"default_sort_order":[]`, 1)
	e.setData(streamsConfigMap, streamsManifestData(jobConfigMap, selection))
	e.reconcileJob(jobConfigMap)
	if !assert.Equal(t, PhaseReady, e.phaseOf(jobConfigMap)) || !assert.Len(t, updateJobRequests(e.etl), 1) {
		return
	}
	assert.Equal(t, selection, updateJobRequests(e.etl)[0].SelectedStreamsConfig)
	assert.Equal(t, selection, *e.etl.jobs["orders"].SelectedStreamsConfig)
}
