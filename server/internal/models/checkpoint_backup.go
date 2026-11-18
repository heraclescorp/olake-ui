package models

import "time"

// CheckpointBackup is written in the same transaction as a forced checkpoint.
// It is never returned by the job-list API: checkpoints can contain cursor data.
type CheckpointBackup struct {
	ID            uint64 `gorm:"primaryKey;autoIncrement"`
	JobID         int    `gorm:"index;not null"`
	ProjectID     string `gorm:"not null"`
	ActorID       int    `gorm:"not null"`
	PreviousState string `gorm:"type:text;not null"`
	NewLSN        string `gorm:"not null"`
	CreatedAt     time.Time
}

func (*CheckpointBackup) TableName() string { return (&Job{}).TableName() + "_checkpoint_backups" }
