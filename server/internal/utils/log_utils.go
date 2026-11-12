package utils

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	"github.com/datazip-inc/olake-ui/server/internal/storage"
)

// LogEntry represents a log entry
type LogEntry struct {
	Level   string          `json:"level"`
	Time    time.Time       `json:"time"`
	Message json.RawMessage `json:"message"`       // store raw JSON
	Tip     bool            `json:"tip,omitempty"` // worker recommendation line (task logs page)
}

func ReadLogs(ctx context.Context, logDir string, cursor int64, limit int, direction string) (*dto.TaskLogsResponse, error) {
	switch storage.Mode() {
	case constants.StorageModeS3:
		if workRel, err := filepath.Rel(constants.DefaultConfigDir, logDir); err == nil {
			logDir = workRel
		}
		return readLogsFromS3(ctx, logDir, cursor, direction)
	default:
		return readLogsFromFilesystem(logDir, cursor, limit, direction)
	}
}

// GetLogArchiveFilename names an archive from the already-resolved execution.
func GetLogArchiveFilename(ctx context.Context, jobID int, baseDir string, execution *ExecutionRuntimeLogIdentity) (string, error) {
	connectorDir, _, err := ResolveExecutionRuntimeLogDirs(baseDir, execution)
	if err != nil {
		return "", err
	}
	if connectorDir == "" {
		return "", constants.ErrSyncFolderNotFound
	}
	syncFolder, err := GetAndValidateSyncFolder(ctx, connectorDir)
	if err != nil {
		return "", err
	}

	syncTimestamp := strings.ReplaceAll(strings.TrimPrefix(syncFolder, "sync_"), "_", "-")
	return fmt.Sprintf("job-%d-logs-%s.tar.gz", jobID, syncTimestamp), nil
}

// GetAndValidateLogBaseDir returns the log base path for the active storage mode.
func GetAndValidateLogBaseDir(ctx context.Context, workflowID string, execution *ExecutionRuntimeLogIdentity) (string, error) {
	// A recorded execution may have connector logs before the first stable
	// worker-log flush. Its SQL authority does not depend on a mutable pointer
	// or even an existing stable workflow prefix.
	if execution != nil {
		if storage.Mode() == constants.StorageModeS3 {
			return WorkflowHash(workflowID), nil
		}
		return filepath.Join(constants.DefaultConfigDir, WorkflowHash(workflowID)), nil
	}
	switch storage.Mode() {
	case constants.StorageModeS3:
		return GetAndValidateS3LogBaseDir(ctx, workflowID)
	default:
		return GetAndValidateFilesystemLogBaseDir(workflowID)
	}
}

// GetAndValidateSyncFolder returns the sync_* folder name for the active storage mode.
func GetAndValidateSyncFolder(ctx context.Context, baseDir string) (string, error) {
	switch storage.Mode() {
	case constants.StorageModeS3:
		return GetAndValidateS3SyncDir(ctx, baseDir)
	default:
		_, syncFolderName, err := GetAndValidateFilesystemSyncDir(baseDir)
		return syncFolderName, err
	}
}

// ExecutionRuntimeLogIdentity is a snapshot selected from the execution record
// scoped to the requested job and workflow. Nil means no record (legacy layout).
type ExecutionRuntimeLogIdentity struct {
	RuntimeID string
	LaunchID  string
}

// ResolveExecutionRuntimeLogDirs never reads a mutable runtime pointer. A late
// storage Put must not choose which admitted generation the API displays.
func ResolveExecutionRuntimeLogDirs(baseDir string, execution *ExecutionRuntimeLogIdentity) (string, string, error) {
	if execution == nil {
		return baseDir, baseDir, nil
	}
	if execution.RuntimeID == "" && execution.LaunchID == "" {
		// Admission has not launched a connector. Show stable worker logs only,
		// never a stale connector directory or a pointer-based fallback.
		return "", "", nil
	}
	if strings.TrimSpace(execution.RuntimeID) == "" || strings.TrimSpace(execution.LaunchID) == "" {
		return "", "", fmt.Errorf("invalid execution record runtime identity")
	}
	artifactRoot := filepath.Dir(baseDir)
	return filepath.Join(artifactRoot, fmt.Sprintf("%x", sha256.Sum256([]byte(execution.RuntimeID)))),
		filepath.Join(artifactRoot, fmt.Sprintf("%x", sha256.Sum256([]byte(execution.LaunchID)))), nil
}

// parseValidLogLine parses a line and reports whether it is a valid, non-debug log entry.
// Returns the parsed entry so callers can reuse it instead of unmarshalling the line again.
func parseValidLogLine(line string) (LogEntry, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return LogEntry{}, false
	}

	var logEntry LogEntry
	if err := json.Unmarshal([]byte(line), &logEntry); err != nil {
		return LogEntry{}, false
	}

	if logEntry.Level == "debug" {
		return LogEntry{}, false
	}

	return logEntry, true
}

func parseLines(entries []LogEntry) []map[string]interface{} {
	batch := make([]map[string]interface{}, 0, len(entries))
	for _, logEntry := range entries {
		batch = append(batch, map[string]interface{}{
			"level":   logEntry.Level,
			"time":    logEntry.Time.UTC().Format(time.RFC3339),
			"message": logMessageString(logEntry.Message),
		})
	}

	return batch
}

// logMessageString returns a log message as text: strings as-is, any other
// JSON value re-marshalled.
func logMessageString(raw json.RawMessage) string {
	var value interface{}
	if err := json.Unmarshal(raw, &value); err != nil {
		return string(raw)
	}
	if str, ok := value.(string); ok {
		return str
	}
	msgBytes, err := json.Marshal(value)
	if err != nil {
		return string(raw)
	}
	return string(msgBytes)
}

// AddFilesToArchive includes current connector artifacts, stable worker logs,
// and the original admitted launch when recovery uses a different runtime.
func AddFilesToArchive(ctx context.Context, baseDir string, execution *ExecutionRuntimeLogIdentity, tarWriter *tar.Writer) error {
	currentDir, launchDir, err := ResolveExecutionRuntimeLogDirs(baseDir, execution)
	if err != nil {
		return err
	}
	if currentDir == "" {
		return constants.ErrSyncFolderNotFound
	}
	addDirectory := func(dir, prefix string) error {
		if storage.Mode() == constants.StorageModeS3 {
			return addS3FilesToArchive(ctx, dir, prefix, tarWriter)
		}
		return addFilesystemFilesToArchive(dir, prefix, tarWriter)
	}
	if err := addDirectory(currentDir, ""); err != nil {
		return err
	}
	if currentDir != baseDir {
		if err := addDirectory(baseDir, "workflow"); err != nil {
			return err
		}
	}
	if launchDir != currentDir {
		return addDirectory(launchDir, "launch")
	}
	return nil
}

func writeToArchive(tarWriter *tar.Writer, header *tar.Header, reader io.Reader) (int64, error) {
	if err := tarWriter.WriteHeader(header); err != nil {
		return 0, fmt.Errorf("failed to write tar header for %s: %s", header.Name, err)
	}

	bytesWritten, err := io.Copy(tarWriter, reader)
	if err != nil {
		return 0, fmt.Errorf("failed to write file content for %s: %s", header.Name, err)
	}

	return bytesWritten, nil
}
