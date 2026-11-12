package models

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestCatalogRevisionCanonicalWireContract(t *testing.T) {
	streams := ` { "z": 9007199254740993, "a": {"b":2,"a":"<tag>"} } `
	settings := `{"auto_table_enrollment":{"enabled":true}}`
	blank, null := " \t", "null"
	job := Job{StreamsConfig: &streams, AvailableStreamsConfig: &blank, SelectedStreamsConfig: &null, AdvancedSettings: &settings}
	got, err := job.CatalogRevision()
	if err != nil {
		t.Fatal(err)
	}
	// Exact encoding/json output, including its default HTML escaping. Large
	// integers must not be rounded through float64 before hashing.
	canonical := `{"advanced_settings":{"auto_table_enrollment":{"enabled":true}},"available_streams_config":null,"selected_streams_config":null,"streams_config":{"a":{"a":"\u003ctag\u003e","b":2},"z":9007199254740993}}`
	digest := sha256.Sum256([]byte(canonical))
	if want := hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	streams = `{"a":{"a":"<tag>","b":2},"z":9007199254740993}`
	job.AvailableStreamsConfig, job.SelectedStreamsConfig = nil, nil
	job.State = `{"global":{"lsn":"0/ABC"}}`
	job.SourceID, job.DestID = 91, 92
	same, err := job.CatalogRevision()
	if err != nil || same != got {
		t.Fatalf("whitespace/key order/null/state changed revision: %s, %v", same, err)
	}
	streams = `{"a":{"a":"<tag>","b":2},"z":9007199254740992}`
	changed, err := job.CatalogRevision()
	if err != nil || changed == got {
		t.Fatalf("large number edit lost: %s, %v", changed, err)
	}
}

func TestCatalogRevisionRejectsMalformedAndTrailingJSON(t *testing.T) {
	for _, raw := range []string{`{`, `{} {}`, `[] trailing`} {
		job := Job{SelectedStreamsConfig: &raw}
		if _, err := job.CatalogRevision(); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}

func TestCatalogRevisionIncludesEverySavedJSONField(t *testing.T) {
	job := Job{}
	original, err := job.CatalogRevision()
	if err != nil {
		t.Fatal(err)
	}
	value := `{}`
	for _, field := range []**string{&job.StreamsConfig, &job.AvailableStreamsConfig, &job.SelectedStreamsConfig, &job.AdvancedSettings} {
		*field = &value
		changed, err := job.CatalogRevision()
		if err != nil || changed == original {
			t.Fatalf("saved field omitted from revision: %s, %v", changed, err)
		}
		*field = nil
	}
}

func TestExecutionAuthorityIsNotPublicJSON(t *testing.T) {
	workflowID, runID := "internal-operation", "internal-run"
	raw, err := json.Marshal(Job{ExecutionWorkflowID: &workflowID, ExecutionRunID: &runID})
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"execution_workflow_id", "execution_run_id", "execution_records"} {
		if _, exposed := payload[field]; exposed {
			t.Fatalf("internal execution authority exposed as %s", field)
		}
	}
	raw, err = json.Marshal(JobExecutionRecord{JobID: 17, WorkflowID: workflowID, OwnerRunID: runID, Phase: "handoff", HandoffToken: "private-transfer-token"})
	if err != nil || string(raw) != `{}` {
		t.Fatalf("execution record is public JSON: %s, %v", raw, err)
	}
}
