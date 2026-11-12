package models

import (
	"time"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/types"
	"github.com/datazip-inc/olake-ui/server/internal/utils"
)

// BaseModel with common fields
type BaseModel struct {
	CreatedAt time.Time  `json:"created_at" gorm:"column:created_at;autoCreateTime"`
	UpdatedAt time.Time  `json:"updated_at" gorm:"column:updated_at;autoUpdateTime"`
	DeletedAt *time.Time `json:"deleted_at,omitempty" gorm:"column:deleted_at"`
}

// User represents the user entity
type User struct {
	BaseModel
	ID       int    `json:"id" gorm:"column:id;primaryKey;autoIncrement"`
	Username string `json:"username" gorm:"column:username;size:100;unique"`
	Password string `json:"password" gorm:"column:password;size:100"`
	Email    string `json:"email" gorm:"column:email;size:100;unique"`
}

func (u *User) TableName() string {
	return constants.TableNameMap[constants.UserTable]
}

// ProjectSettings stores configuration scoped per project.
type ProjectSettings struct {
	BaseModel
	ID              int    `json:"id" gorm:"column:id;primaryKey;autoIncrement"`
	ProjectID       string `json:"project_id" gorm:"column:project_id;size:255;unique"`
	WebhookAlertURL string `json:"webhook_alert_url" gorm:"column:webhook_alert_url;size:512"`
}

func (s *ProjectSettings) TableName() string {
	return constants.TableNameMap[constants.ProjectSettingsTable]
}

// Source entity referencing User for auditing fields
type Source struct {
	BaseModel
	ID          int    `json:"id" gorm:"column:id;primaryKey;autoIncrement"`
	Name        string `json:"name" gorm:"column:name;size:255"`
	ProjectID   string `json:"project_id" gorm:"column:project_id;size:255"`
	Config      string `json:"config" gorm:"column:config;type:jsonb"`
	Version     string `json:"version" gorm:"column:version;size:255"`
	Type        string `json:"type" gorm:"column:type;size:255"`
	CreatedByID int    `json:"-" gorm:"column:created_by_id"`
	UpdatedByID int    `json:"-" gorm:"column:updated_by_id"`

	CreatedBy *User `json:"created_by,omitempty" gorm:"foreignKey:CreatedByID;references:ID"`
	UpdatedBy *User `json:"updated_by,omitempty" gorm:"foreignKey:UpdatedByID;references:ID"`
}

func (s *Source) TableName() string {
	return constants.TableNameMap[constants.SourceTable]
}

// Destination entity referencing User
type Destination struct {
	BaseModel
	ID          int    `json:"id" gorm:"column:id;primaryKey;autoIncrement"`
	Name        string `json:"name" gorm:"column:name;size:255"`
	ProjectID   string `json:"project_id" gorm:"column:project_id;size:255"`
	DestType    string `json:"type" gorm:"column:dest_type;size:255"`
	Version     string `json:"version" gorm:"column:version;size:255"`
	Config      string `json:"config" gorm:"column:config;type:jsonb"`
	CreatedByID int    `json:"-" gorm:"column:created_by_id"`
	UpdatedByID int    `json:"-" gorm:"column:updated_by_id"`

	CreatedBy *User `json:"created_by,omitempty" gorm:"foreignKey:CreatedByID;references:ID"`
	UpdatedBy *User `json:"updated_by,omitempty" gorm:"foreignKey:UpdatedByID;references:ID"`
}

func (d *Destination) TableName() string {
	return constants.TableNameMap[constants.DestinationTable]
}

// Job represents a synchronization job
type Job struct {
	BaseModel
	ID                     int     `json:"id" gorm:"column:id;primaryKey;autoIncrement"`
	Name                   string  `json:"name" gorm:"column:name;size:100"`
	SourceID               int     `json:"source_id" gorm:"column:source_id"`
	DestID                 int     `json:"dest_id" gorm:"column:dest_id"`
	Active                 bool    `json:"active" gorm:"column:active"`
	Frequency              string  `json:"frequency" gorm:"column:frequency;size:255"`
	StreamsConfig          *string `json:"streams_config" gorm:"column:streams_config;type:jsonb"`
	AvailableStreamsConfig *string `json:"available_streams_config" gorm:"column:available_streams_config;type:jsonb"`
	SelectedStreamsConfig  *string `json:"selected_streams_config" gorm:"column:selected_streams_config;type:jsonb"`
	State                  string  `json:"state" gorm:"column:state;type:jsonb"`
	ExecutionWorkflowID    *string `json:"-" gorm:"column:execution_workflow_id;type:text"`
	ExecutionRunID         *string `json:"-" gorm:"column:execution_run_id;type:text"`
	AdvancedSettings       *string `json:"advanced_settings" gorm:"column:advanced_settings;type:jsonb"`
	CreatedByID            int     `json:"-" gorm:"column:created_by_id"`
	UpdatedByID            int     `json:"-" gorm:"column:updated_by_id"`
	ProjectID              string  `json:"project_id" gorm:"column:project_id;size:255"`

	Source      *Source      `json:"source,omitempty" gorm:"foreignKey:SourceID;references:ID"`
	Destination *Destination `json:"destination,omitempty" gorm:"foreignKey:DestID;references:ID"`
	CreatedBy   *User        `json:"created_by,omitempty" gorm:"foreignKey:CreatedByID;references:ID"`
	UpdatedBy   *User        `json:"updated_by,omitempty" gorm:"foreignKey:UpdatedByID;references:ID"`
}

func (j *Job) TableName() string {
	return constants.TableNameMap[constants.JobTable]
}

// JobExecutionRecord is durable worker authority, not part of the public API.
// Closed records are tombstones: a delayed activity must not reopen admission.
type JobExecutionRecord struct {
	JobID             int       `json:"-" gorm:"column:job_id;type:integer;size:32;primaryKey;autoIncrement:false;index:,composite:job_phase,priority:1"`
	WorkflowID        string    `json:"-" gorm:"column:workflow_id;type:text;primaryKey"`
	OwnerRunID        string    `json:"-" gorm:"column:owner_run_id;type:text;not null;default:''"`
	LaunchID          string    `json:"-" gorm:"column:launch_id;type:text;not null;default:''"`
	LauncherAttemptID string    `json:"-" gorm:"column:launcher_attempt_id;type:text;not null;default:''"`
	Phase             string    `json:"-" gorm:"column:phase;type:text;not null;default:'';index:,composite:job_phase,priority:2"`
	RuntimeID         string    `json:"-" gorm:"column:runtime_id;type:text;not null;default:''"`
	ResourceID        string    `json:"-" gorm:"column:resource_id;type:text;not null;default:''"`
	ResourceUID       string    `json:"-" gorm:"column:resource_uid;type:text;not null;default:''"`
	Outcome           string    `json:"-" gorm:"column:outcome;type:text;not null;default:''"`
	StateSHA256       string    `json:"-" gorm:"column:state_sha256;type:text;not null;default:''"`
	CatalogSHA256     string    `json:"-" gorm:"column:catalog_sha256;type:text;not null;default:''"`
	HandoffToken      string    `json:"-" gorm:"column:handoff_token;type:text;not null;default:''"`
	HandoffRunID      string    `json:"-" gorm:"column:handoff_run_id;type:text;not null;default:''"`
	HandoffActive     bool      `json:"-" gorm:"column:handoff_active;not null;default:false"`
	HandoffRequestID  string    `json:"-" gorm:"column:handoff_request_id;type:text;not null;default:''"`
	CreatedAt         time.Time `json:"-" gorm:"column:created_at;type:timestamptz;not null;default:now()"`
	UpdatedAt         time.Time `json:"-" gorm:"column:updated_at;type:timestamptz;not null;default:now()"`
}

func (j *JobExecutionRecord) TableName() string {
	return constants.TableNameMap[constants.JobTable] + "_execution_records"
}

// StreamsCatalog returns the catalog the job stores, in the job's format. A job keeps one format:
// available and selected streams are both set (split), or both empty (legacy).
func (j *Job) StreamsCatalog() types.StreamsCatalog {
	return types.StreamsCatalog{
		Streams:   utils.StringValue(j.StreamsConfig),
		Available: utils.StringValue(j.AvailableStreamsConfig),
		Selected:  utils.StringValue(j.SelectedStreamsConfig),
	}
}

type Catalog struct {
	BaseModel
	ID      int    `json:"id" gorm:"column:id;primaryKey;autoIncrement"`
	Type    string `json:"type" gorm:"column:type;size:50"`
	Name    string `json:"name" gorm:"column:name;size:100"`
	Specs   string `json:"specs" gorm:"column:specs;type:jsonb"`
	Version string `json:"version" gorm:"column:version;size:255"`
}

func (c *Catalog) TableName() string {
	return constants.TableNameMap[constants.CatalogTable]
}

type Session struct {
	SessionKey    string    `json:"session_key" gorm:"column:session_key;primaryKey;size:64"`
	SessionData   []byte    `json:"session_data" gorm:"column:session_data;type:bytea"`
	SessionExpiry time.Time `json:"session_expiry" gorm:"column:session_expiry"`
}

func (s *Session) TableName() string {
	return constants.TableNameMap[constants.SessionTable]
}
