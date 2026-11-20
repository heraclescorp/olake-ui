package dto

import "encoding/json"

// Preserve unrecognized settings through API reads and writes, including nested
// policy extensions. Known fields always take precedence over extensions.
func readExtensions(data []byte, known ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for _, key := range known {
		delete(fields, key)
	}
	return fields, nil
}

func writeExtensions(value any, extra map[string]json.RawMessage) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil || len(extra) == 0 {
		return data, err
	}
	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for key, value := range extra {
		if _, exists := fields[key]; !exists {
			fields[key] = value
		}
	}
	return json.Marshal(fields)
}

type advancedSettingsJSON AdvancedSettings

func (v *AdvancedSettings) UnmarshalJSON(data []byte) error {
	var value advancedSettingsJSON
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	extra, err := readExtensions(data, "max_discover_threads", "index_required", "target_query_engines", "auto_table_enrollment")
	if err != nil {
		return err
	}
	*v = AdvancedSettings(value)
	v.Extra = extra
	return nil
}

func (v AdvancedSettings) MarshalJSON() ([]byte, error) {
	return writeExtensions(advancedSettingsJSON(v), v.Extra)
}

type enrollmentPolicyJSON AutoTableEnrollmentPolicy

func (v *AutoTableEnrollmentPolicy) UnmarshalJSON(data []byte) error {
	var value enrollmentPolicyJSON
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	extra, err := readExtensions(data, "enabled", "schemas", "defaults")
	if err != nil {
		return err
	}
	*v = AutoTableEnrollmentPolicy(value)
	v.Extra = extra
	return nil
}

func (v AutoTableEnrollmentPolicy) MarshalJSON() ([]byte, error) {
	return writeExtensions(enrollmentPolicyJSON(v), v.Extra)
}

type enrollmentSchemaJSON AutoTableEnrollmentSchema

func (v *AutoTableEnrollmentSchema) UnmarshalJSON(data []byte) error {
	var value enrollmentSchemaJSON
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	extra, err := readExtensions(data, "namespace", "destination_database")
	if err != nil {
		return err
	}
	*v = AutoTableEnrollmentSchema(value)
	v.Extra = extra
	return nil
}

func (v AutoTableEnrollmentSchema) MarshalJSON() ([]byte, error) {
	return writeExtensions(enrollmentSchemaJSON(v), v.Extra)
}

type enrollmentDefaultsJSON AutoTableEnrollmentDefaults

func (v *AutoTableEnrollmentDefaults) UnmarshalJSON(data []byte) error {
	var value enrollmentDefaultsJSON
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	extra, err := readExtensions(data, "normalization", "append_mode", "update_type", "merge_strategy", "sync_new_columns", "drop_removed_columns")
	if err != nil {
		return err
	}
	*v = AutoTableEnrollmentDefaults(value)
	v.Extra = extra
	return nil
}

func (v AutoTableEnrollmentDefaults) MarshalJSON() ([]byte, error) {
	return writeExtensions(enrollmentDefaultsJSON(v), v.Extra)
}
