package etl

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	"github.com/datazip-inc/olake-ui/server/internal/types"
)

func enrollmentEnabled(settings *dto.AdvancedSettings) bool {
	return settings != nil && settings.AutoTableEnrollment != nil && settings.AutoTableEnrollment.Enabled
}

func validateEnrollmentPolicy(settings *dto.AdvancedSettings, sourceType, destinationType string, catalog types.StreamsCatalog) error {
	if !enrollmentEnabled(settings) {
		return nil
	}
	invalid := func(message string) error {
		return fmt.Errorf("%w: %s", constants.ErrInvalidEnrollmentPolicy, message)
	}
	if sourceType != "postgres" || destinationType != "iceberg" {
		return invalid("only PostgreSQL sources and Iceberg destinations support automatic enrollment")
	}
	policy := settings.AutoTableEnrollment
	if len(policy.Schemas) == 0 {
		return invalid("approve at least one existing source schema")
	}
	defaults := policy.Defaults
	if defaults == nil || defaults.Normalization == nil || defaults.AppendMode == nil || defaults.SyncNewColumns == nil {
		return invalid("normalization, append_mode and sync_new_columns defaults must be explicitly supplied")
	}
	switch defaults.UpdateType {
	case "eq", "pos", "dv":
	default:
		return invalid("update_type must explicitly be eq, pos or dv")
	}
	// This is the connector's query engine/delete-file compatibility matrix.
	// Empty engine lists are unconstrained. Append-only tables emit no deletes.
	for _, engine := range settings.TargetQueryEngines {
		var supported string
		switch strings.ToLower(strings.TrimSpace(engine)) {
		case "":
			continue
		case "spark", "flink", "trino", "duckdb", "clickhouse":
			supported = "eq,pos,dv"
		case "presto", "starrocks", "hive", "athena", "bigquery":
			supported = "eq,pos"
		case "snowflake":
			supported = "pos,dv"
		case "databricks":
			supported = "dv"
		case "dremio":
			supported = "pos"
		default:
			return invalid(fmt.Sprintf("unknown target query engine %q", engine))
		}
		if !*defaults.AppendMode && !strings.Contains(","+supported+",", ","+defaults.UpdateType+",") {
			return invalid(fmt.Sprintf("update_type %q is not supported by target query engine %q", defaults.UpdateType, engine))
		}
	}
	availableJSON := catalog.Streams
	if catalog.IsSplit() {
		availableJSON = catalog.Available
	}
	var available struct {
		Streams []struct {
			Stream struct {
				Namespace string `json:"namespace"`
			} `json:"stream"`
		} `json:"streams"`
	}
	if err := json.Unmarshal([]byte(availableJSON), &available); err != nil || available.Streams == nil {
		return invalid("a valid saved available catalog is required")
	}
	existing := make(map[string]bool)
	for _, entry := range available.Streams {
		existing[entry.Stream.Namespace] = true
	}
	approved := make(map[string]bool)
	for _, schema := range policy.Schemas {
		if strings.TrimSpace(schema.Namespace) == "" || strings.TrimSpace(schema.DestinationDatabase) == "" {
			return invalid("each approved schema needs an explicit namespace and destination_database")
		}
		if approved[schema.Namespace] {
			return invalid(fmt.Sprintf("schema %q is approved more than once", schema.Namespace))
		}
		if !existing[schema.Namespace] {
			return invalid(fmt.Sprintf("schema %q is not present in the saved available catalog", schema.Namespace))
		}
		approved[schema.Namespace] = true
	}
	return nil
}

// Resolve references for validation without creating or rewriting either driver.
// The request's display type is not authoritative when an ID is supplied.
func (s Service) enrollmentDriverTypes(source, destination *dto.DriverConfig, projectID string) (string, string, error) {
	if source == nil || destination == nil {
		return "", "", fmt.Errorf("%w: source and destination are required", constants.ErrInvalidEnrollmentPolicy)
	}
	sourceType, destinationType := source.Type, destination.Type
	if source.ID != nil {
		stored, err := s.db.GetSourceByID(*source.ID)
		if err != nil {
			return "", "", err
		}
		if stored.ProjectID != projectID {
			return "", "", constants.ErrSourceNotFound
		}
		sourceType = stored.Type
	}
	if destination.ID != nil {
		stored, err := s.db.GetDestinationByID(*destination.ID)
		if err != nil {
			return "", "", err
		}
		if stored.ProjectID != projectID {
			return "", "", constants.ErrDestinationNotFound
		}
		destinationType = stored.DestType
	}
	return sourceType, destinationType, nil
}
