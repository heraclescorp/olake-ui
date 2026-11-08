package etl

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	"github.com/datazip-inc/olake-ui/server/internal/types"
)

const validEnrollmentSettings = `{"auto_table_enrollment":{"enabled":true,"schemas":[{"namespace":"public","destination_database":"warehouse:public"}],"defaults":{"normalization":false,"append_mode":false,"update_type":"pos","sync_new_columns":false}}}`

func enrollmentSettings(t *testing.T, raw string) *dto.AdvancedSettings {
	t.Helper()
	var settings dto.AdvancedSettings
	if err := json.Unmarshal([]byte(raw), &settings); err != nil {
		t.Fatal(err)
	}
	return &settings
}

func enrollmentJob(t *testing.T, split bool) *models.Job {
	t.Helper()
	available := `{"streams":[{"stream":{"namespace":"public","name":"orders"}},{"stream":{"namespace":"other","name":"history"}}]}`
	selected := `{"selected_streams":{"public":[{"stream_name":"orders","destination_database":"existing","destination_table":"custom_orders","selected_columns":{"columns":["id"],"sync_new_columns":false}}]}}`
	job := &models.Job{
		ID: 17, ProjectID: "project-a", Active: true,
		State:    `{"global":{"lsn":"0/ABC"},"streams":{"public.orders":{"cursor":42}}}`,
		SourceID: 101, DestID: 202,
		Source:      &models.Source{ID: 101, Type: "postgres", Version: "v1.0.0"},
		Destination: &models.Destination{ID: 202, DestType: "iceberg", Version: "v1.0.0"},
	}
	if split {
		job.AvailableStreamsConfig, job.SelectedStreamsConfig = &available, &selected
	} else {
		var catalog map[string]json.RawMessage
		if err := json.Unmarshal([]byte(available), &catalog); err != nil {
			t.Fatal(err)
		}
		var selection map[string]json.RawMessage
		if err := json.Unmarshal([]byte(selected), &selection); err != nil {
			t.Fatal(err)
		}
		catalog["selected_streams"] = selection["selected_streams"]
		data, err := json.Marshal(catalog)
		if err != nil {
			t.Fatal(err)
		}
		legacy := string(data)
		job.StreamsConfig = &legacy
	}
	return job
}

func TestEnrollmentAcceptsExplicitFalseDefaultsAndUnselectedExistingSchemas(t *testing.T) {
	for _, split := range []bool{false, true} {
		job := enrollmentJob(t, split)
		settings := enrollmentSettings(t, validEnrollmentSettings)
		settings.AutoTableEnrollment.Schemas = append(settings.AutoTableEnrollment.Schemas, dto.AutoTableEnrollmentSchema{Namespace: "other", DestinationDatabase: "warehouse:other"})
		// Approval is based on all saved available schemas, not current selection.
		if split {
			selected := `{"selected_streams":{}}`
			job.SelectedStreamsConfig = &selected
		}
		before, err := json.Marshal(job)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateEnrollmentPolicy(settings, "postgres", "iceberg", job.StreamsCatalog()); err != nil {
			t.Fatal(err)
		}
		after, err := json.Marshal(job)
		if err != nil {
			t.Fatal(err)
		}
		if string(before) != string(after) {
			t.Fatal("policy validation rewrote catalog, selection, driver refs or CDC state")
		}
	}
}

func TestEnrollmentRejectsInvalidPolicy(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, tc := range []struct {
			name                string
			change              func(*dto.AdvancedSettings)
			source, destination string
		}{
			{name: "source", source: "mysql"},
			{name: "destination", destination: "s3"},
			{name: "empty approvals", change: func(s *dto.AdvancedSettings) { s.AutoTableEnrollment.Schemas = nil }},
			{name: "blank namespace", change: func(s *dto.AdvancedSettings) { s.AutoTableEnrollment.Schemas[0].Namespace = " " }},
			{name: "unknown schema", change: func(s *dto.AdvancedSettings) { s.AutoTableEnrollment.Schemas[0].Namespace = "new_schema" }},
			{name: "blank mapping", change: func(s *dto.AdvancedSettings) { s.AutoTableEnrollment.Schemas[0].DestinationDatabase = "\t" }},
			{name: "duplicate schema", change: func(s *dto.AdvancedSettings) {
				s.AutoTableEnrollment.Schemas = append(s.AutoTableEnrollment.Schemas, s.AutoTableEnrollment.Schemas[0])
			}},
			{name: "missing defaults", change: func(s *dto.AdvancedSettings) { s.AutoTableEnrollment.Defaults = nil }},
			{name: "missing normalization", change: func(s *dto.AdvancedSettings) { s.AutoTableEnrollment.Defaults.Normalization = nil }},
			{name: "missing append mode", change: func(s *dto.AdvancedSettings) { s.AutoTableEnrollment.Defaults.AppendMode = nil }},
			{name: "missing sync columns", change: func(s *dto.AdvancedSettings) { s.AutoTableEnrollment.Defaults.SyncNewColumns = nil }},
			{name: "missing delete type", change: func(s *dto.AdvancedSettings) { s.AutoTableEnrollment.Defaults.UpdateType = "" }},
			{name: "unknown delete type", change: func(s *dto.AdvancedSettings) { s.AutoTableEnrollment.Defaults.UpdateType = "equality" }},
			{name: "incompatible reader", change: func(s *dto.AdvancedSettings) { s.TargetQueryEngines = []string{"databricks"} }},
			{name: "unknown reader", change: func(s *dto.AdvancedSettings) { s.TargetQueryEngines = []string{"not-an-engine"} }},
		} {
			t.Run(map[bool]string{false: "legacy/", true: "split/"}[split]+tc.name, func(t *testing.T) {
				settings := enrollmentSettings(t, validEnrollmentSettings)
				if tc.change != nil {
					tc.change(settings)
				}
				source, destination := tc.source, tc.destination
				if source == "" {
					source = "postgres"
				}
				if destination == "" {
					destination = "iceberg"
				}
				if err := validateEnrollmentPolicy(settings, source, destination, enrollmentJob(t, split).StreamsCatalog()); !errors.Is(err, constants.ErrInvalidEnrollmentPolicy) {
					t.Fatalf("unsafe policy accepted: %v", err)
				}
			})
		}
	}
}

func TestEnrollmentOffPreservesLegacyBehaviorWithoutPreflightValidation(t *testing.T) {
	for _, settings := range []*dto.AdvancedSettings{nil, {}, {AutoTableEnrollment: &dto.AutoTableEnrollmentPolicy{Enabled: false}}} {
		if err := validateEnrollmentPolicy(settings, "mysql", "s3", types.StreamsCatalog{}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEnrollmentReaderCompatibility(t *testing.T) {
	for _, tc := range []struct {
		engine, update  string
		append, allowed bool
	}{
		{"spark", "eq", false, true}, {"athena", "dv", false, false},
		{"snowflake", "eq", false, false}, {"snowflake", "dv", false, true},
		{" Databricks ", "dv", false, true}, {"dremio", "eq", false, false},
		{"dremio", "pos", false, true}, {"databricks", "pos", true, true},
	} {
		s := enrollmentSettings(t, validEnrollmentSettings)
		s.TargetQueryEngines = []string{tc.engine}
		s.AutoTableEnrollment.Defaults.UpdateType = tc.update
		s.AutoTableEnrollment.Defaults.AppendMode = &tc.append
		err := validateEnrollmentPolicy(s, "postgres", "iceberg", enrollmentJob(t, true).StreamsCatalog())
		if (err == nil) != tc.allowed {
			t.Fatalf("%+v: %v", tc, err)
		}
	}
}

func TestJobDetailExposesSavedRevisionWithoutResettingState(t *testing.T) {
	for _, split := range []bool{false, true} {
		job := enrollmentJob(t, split)
		job.SourceID, job.DestID = 101, 202
		settings := validEnrollmentSettings
		job.AdvancedSettings = &settings
		before, err := json.Marshal(job)
		if err != nil {
			t.Fatal(err)
		}
		response, err := (Service{}).buildJobResponse(job, nil, true)
		if err != nil {
			t.Fatal(err)
		}
		want, err := job.CatalogRevision()
		if err != nil {
			t.Fatal(err)
		}
		if response.CatalogRevision != want || response.AdvancedSettings.AutoTableEnrollment == nil {
			t.Fatalf("missing saved revision/policy: %+v", response)
		}
		after, err := json.Marshal(job)
		if err != nil || string(before) != string(after) {
			t.Fatalf("detail read reset saved state or references: %s, %v", after, err)
		}
	}
}
