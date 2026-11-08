package models

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// CatalogRevision is shared with the worker: SHA256 of the JSON object containing
// all four saved JSON fields. UseNumber preserves number lexemes; encoding/json
// sorts object keys. SQL NULL, empty strings and JSON null all canonicalize to null.
// State and observational metadata outside these fields do not affect the token.
func (j *Job) CatalogRevision() (string, error) {
	fields := map[string]*string{
		"streams_config":           j.StreamsConfig,
		"available_streams_config": j.AvailableStreamsConfig,
		"selected_streams_config":  j.SelectedStreamsConfig,
		"advanced_settings":        j.AdvancedSettings,
	}
	canonical := make(map[string]any, len(fields))
	for name, raw := range fields {
		var value any
		if raw != nil && strings.TrimSpace(*raw) != "" {
			decoder := json.NewDecoder(strings.NewReader(*raw))
			decoder.UseNumber()
			if err := decoder.Decode(&value); err != nil {
				return "", fmt.Errorf("invalid saved %s: %w", name, err)
			}
			var extra any
			if err := decoder.Decode(&extra); err != io.EOF {
				return "", fmt.Errorf("invalid saved %s: trailing JSON", name)
			}
		}
		canonical[name] = value
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func (j *Job) AutoTableEnrollmentEnabled() (bool, error) {
	if j.AdvancedSettings == nil || strings.TrimSpace(*j.AdvancedSettings) == "" {
		return false, nil
	}
	var settings struct {
		Policy *struct {
			Enabled bool `json:"enabled"`
		} `json:"auto_table_enrollment"`
	}
	if err := json.Unmarshal([]byte(*j.AdvancedSettings), &settings); err != nil {
		return false, fmt.Errorf("invalid saved advanced_settings: %w", err)
	}
	return settings.Policy != nil && settings.Policy.Enabled, nil
}
