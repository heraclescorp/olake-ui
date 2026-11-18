package etl

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/datazip-inc/olake-ui/server/internal/utils"
)

func checkpointTestJob(split bool) *models.Job {
	available := `{"streams":[{"stream":{"namespace":"public","name":"a","sync_mode":"cdc"}},{"stream":{"namespace":"public","name":"b","sync_mode":"cdc"}},{"stream":{"namespace":"public","name":"inc","sync_mode":"incremental"}},{"stream":{"namespace":"public","name":"disabled","sync_mode":"cdc"}},{"stream":{"namespace":"public","name":"unselected","sync_mode":"cdc"}}]}`
	selected := `{"selected_streams":{"public":[{"stream_name":"a"},{"stream_name":"b","sync_mode":"strict_cdc"},{"stream_name":"inc"},{"stream_name":"disabled","disabled":true}]}}`
	job := &models.Job{Source: &models.Source{Type: "postgres"}, State: `{"type":"STREAM","version":8,"global":{"state":{"lsn":"0/100","extra":1234567890123456789},"streams":["other.old"]},"streams":[{"stream":"inc","state":{"cursor":1234567890123456789}}]}`}
	if split {
		job.AvailableStreamsConfig = utils.StringPtr(available)
		job.SelectedStreamsConfig = utils.StringPtr(selected)
	} else {
		var a, s map[string]json.RawMessage
		_ = json.Unmarshal([]byte(available), &a)
		_ = json.Unmarshal([]byte(selected), &s)
		a["selected_streams"] = s["selected_streams"]
		b, _ := json.Marshal(a)
		job.StreamsConfig = utils.StringPtr(string(b))
	}
	return job
}
func TestForceCheckpointBuild(t *testing.T) {
	for _, split := range []bool{true, false} {
		job := checkpointTestJob(split)
		before := job.State
		p, err := buildCheckpointPreview(job, "0761/0f6ade70")
		if err != nil {
			t.Fatal(err)
		}
		if p.LSN != "761/F6ADE70" || p.CurrentLSN != "0/100" || !reflect.DeepEqual(p.Streams, []string{"public.a", "public.b"}) {
			t.Fatalf("bad preview: %+v", p)
		}
		var old, next map[string]json.RawMessage
		_ = json.Unmarshal([]byte(before), &old)
		_ = json.Unmarshal(p.State, &next)
		if string(old["streams"]) != string(next["streams"]) {
			t.Fatal("unrelated stream checkpoint changed")
		}
		var global struct {
			State   json.RawMessage
			Streams []string
		}
		_ = json.Unmarshal(next["global"], &global)
		if !reflect.DeepEqual(global.Streams, []string{"other.old", "public.a", "public.b"}) {
			t.Fatal(global.Streams)
		}
		if !strings.Contains(string(global.State), "1234567890123456789") {
			t.Fatal("cursor precision lost")
		}
		if job.State != before {
			t.Fatal("preview mutated job")
		}
		if len(p.Revision) != 64 {
			t.Fatal("missing revision")
		}
	}
}
func TestForceCheckpointRejectsUnsafeInput(t *testing.T) {
	for _, lsn := range []string{"", "0/0", "GG/123", "100000000/1", "1/2/3", "1/-1"} {
		if _, err := buildCheckpointPreview(checkpointTestJob(true), lsn); err == nil {
			t.Fatal(lsn)
		}
	}
	for _, state := range []string{"null", "[]", "{", `{"version":7}`, `{"version":8,"global":{"state":"invalid"}}`, `{"version":8,"global":{"streams":{}}}`} {
		job := checkpointTestJob(true)
		job.State = state
		if _, err := buildCheckpointPreview(job, "0/123"); err == nil {
			t.Fatal(state)
		}
	}
	job := checkpointTestJob(true)
	job.Source.Type = "mysql"
	if _, err := buildCheckpointPreview(job, "0/123"); err == nil {
		t.Fatal("accepted mysql")
	}
	for _, state := range []string{`{}`, `{"type":"STREAM","version":8}`} {
		job := checkpointTestJob(true)
		job.State = state
		if _, err := buildCheckpointPreview(job, "761/F6ADE70"); err != nil {
			t.Fatal(err)
		}
	}
}
