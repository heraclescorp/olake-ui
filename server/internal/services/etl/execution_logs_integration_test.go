package etl

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/datazip-inc/olake-ui/server/internal/storage"
	"github.com/datazip-inc/olake-ui/server/internal/utils"
	"github.com/lib/pq"
)

func TestExecutionLogsUseScopedSQLAuthorityDespiteLatePointerPut(t *testing.T) {
	service, job, _ := resyncFenceService(t)
	if storage.Mode() != constants.StorageModeNFS {
		t.Skip("filesystem artifact fixture requires nfs storage mode")
	}
	previousDir := constants.DefaultConfigDir
	constants.DefaultConfigDir = t.TempDir()
	t.Cleanup(func() { constants.DefaultConfigDir = previousDir })
	workflowID := fmt.Sprintf("sync-%s-%d-resync-log-operation", job.ProjectID, job.ID)
	stable, current, launch := utils.WorkflowHash(workflowID), utils.WorkflowHash("current-runtime"), utils.WorkflowHash("admitted-launch")
	write := func(root, name, content string) {
		t.Helper()
		file := filepath.Join(constants.DefaultConfigDir, root, name)
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	line := func(message string) string {
		t.Helper()
		raw, err := json.Marshal(map[string]string{"level": "info", "time": "2026-11-01T00:00:01Z", "message": message})
		if err != nil {
			t.Fatal(err)
		}
		return string(raw) + "\n"
	}
	write(stable, "logs/worker.log", line("stable worker"))
	write(stable, "logs/sync_legacy/olake.log", line("legacy connector"))
	write(stable, "state.json", `{"runtime":"legacy"}`)
	write(current, "logs/sync_2026_11_01/olake.log", line("SQL current connector"))
	write(current, "state.json", `{"runtime":"current"}`)
	write(launch, "logs/sync_2026_10_31/olake.log", line("original admitted connector"))
	write(launch, "state.json", `{"runtime":"original"}`)
	workerDB, err := sql.Open("pgx", os.Getenv("OLAKE_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer workerDB.Close()
	table := pq.QuoteIdentifier((&models.JobExecutionRecord{}).TableName())
	if _, err := workerDB.Exec("INSERT INTO "+table+" (job_id,workflow_id,owner_run_id,controlled,phase,runtime_id,launch_id) VALUES ($1,$2,'current-run',true,'admitted','current-runtime','admitted-launch')", job.ID, workflowID); err != nil {
		t.Fatal(err)
	}
	stalePointer, err := json.Marshal(map[string]any{"version": 1, "workflow_id": workflowID, "run_id": "old-run", "runtime_id": "admitted-launch", "launch_id": "admitted-launch", "subdir": launch})
	if err != nil {
		t.Fatal(err)
	}
	write(stable, "execution-runtime.json", string(stalePointer))
	logs, err := service.GetTaskLogs(context.Background(), job.ID, workflowID, utils.LogSourceSync, "", 20, "older")
	if err != nil || len(logs.Logs) != 1 || logs.Logs[0]["message"] != "SQL current connector" {
		t.Fatalf("mutable pointer displaced scoped SQL authority: %+v %v", logs, err)
	}
	wrongJob, err := service.db.GetJobExecutionRecord(context.Background(), job.ID+1, workflowID)
	if err != nil || wrongJob != nil {
		t.Fatalf("execution lookup escaped requested job scope: %+v %v", wrongJob, err)
	}
	filename, stream, err := service.PrepareLogArchive(context.Background(), job.ID, workflowID)
	if err != nil || filename != fmt.Sprintf("job-%d-logs-2026-11-01.tar.gz", job.ID) {
		t.Fatalf("archive preparation did not select SQL runtime: %s %v", filename, err)
	}
	// Recovery advances after archive preparation. The old mutable Put then
	// completes last; neither the new log read nor the prepared archive follows it.
	replacement := utils.WorkflowHash("replacement-runtime")
	write(replacement, "logs/sync_2026_11_02/olake.log", line("SQL replacement connector"))
	write(replacement, "state.json", `{"runtime":"replacement"}`)
	if _, err := workerDB.Exec("UPDATE "+table+" SET runtime_id='replacement-runtime',owner_run_id='replacement-run' WHERE job_id=$1 AND workflow_id=$2", job.ID, workflowID); err != nil {
		t.Fatal(err)
	}
	write(stable, "execution-runtime.json", string(stalePointer))
	logs, err = service.GetTaskLogs(context.Background(), job.ID, workflowID, utils.LogSourceSync, "", 20, "older")
	if err != nil || len(logs.Logs) != 1 || logs.Logs[0]["message"] != "SQL replacement connector" {
		t.Fatalf("late old pointer hid replacement runtime: %+v %v", logs, err)
	}
	var archive bytes.Buffer
	if err := stream(&archive); err != nil {
		t.Fatal(err)
	}
	gzipReader, err := gzip.NewReader(&archive)
	if err != nil {
		t.Fatal(err)
	}
	defer gzipReader.Close()
	reader := tar.NewReader(gzipReader)
	entries := map[string]string{}
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		content, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		entries[header.Name] = string(content)
	}
	if entries["state.json"] != `{"runtime":"current"}` || entries["launch/state.json"] != `{"runtime":"original"}` || entries["workflow/logs/worker.log"] == "" {
		t.Fatalf("archive lost its prepared SQL generation: %+v", entries)
	}
	if _, err := workerDB.Exec("UPDATE "+table+" SET runtime_id='',launch_id='',phase='unadmitted' WHERE job_id=$1 AND workflow_id=$2", job.ID, workflowID); err != nil {
		t.Fatal(err)
	}
	logs, err = service.GetTaskLogs(context.Background(), job.ID, workflowID, utils.LogSourceAll, "", 20, "older")
	if err != nil || len(logs.Logs) != 1 || logs.Logs[0]["message"] != "stable worker" {
		t.Fatalf("unadmitted record enabled legacy connector fallback: %+v %v", logs, err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.GetTaskLogs(cancelled, job.ID, workflowID, utils.LogSourceSync, "", 20, "older"); !errors.Is(err, context.Canceled) {
		t.Fatalf("authority query error enabled legacy fallback: %v", err)
	}
	if _, err := workerDB.Exec("UPDATE "+table+" SET controlled=false,runtime_id='must-not-downgrade' WHERE job_id=$1 AND workflow_id=$2", job.ID, workflowID); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetTaskLogs(context.Background(), job.ID, workflowID, utils.LogSourceSync, "", 20, "older"); err == nil {
		t.Fatal("legacy discriminator downgraded a retained runtime identity")
	}
	if _, err := workerDB.Exec("UPDATE "+table+" SET runtime_id='',launch_id='' WHERE job_id=$1 AND workflow_id=$2", job.ID, workflowID); err != nil {
		t.Fatal(err)
	}
	logs, err = service.GetTaskLogs(context.Background(), job.ID, workflowID, utils.LogSourceSync, "", 20, "older")
	if err != nil || len(logs.Logs) != 1 || logs.Logs[0]["message"] != "legacy connector" {
		t.Fatalf("explicit legacy record lost stable connector logs: %+v %v", logs, err)
	}
	legacyFilename, legacyStream, err := service.PrepareLogArchive(context.Background(), job.ID, workflowID)
	if err != nil || legacyFilename != fmt.Sprintf("job-%d-logs-legacy.tar.gz", job.ID) {
		t.Fatalf("explicit legacy archive did not use stable layout: %s %v", legacyFilename, err)
	}
	var legacyArchive bytes.Buffer
	if err := legacyStream(&legacyArchive); err != nil {
		t.Fatal(err)
	}
	legacyReader, err := gzip.NewReader(&legacyArchive)
	if err != nil {
		t.Fatal(err)
	}
	legacyContents, err := io.ReadAll(legacyReader)
	_ = legacyReader.Close()
	if err != nil || !bytes.Contains(legacyContents, []byte(`{"runtime":"legacy"}`)) || bytes.Contains(legacyContents, []byte(`{"runtime":"current"}`)) {
		t.Fatalf("legacy archive selected controlled artifacts: %v", err)
	}
	if _, err := workerDB.Exec("DELETE FROM "+table+" WHERE job_id=$1 AND workflow_id=$2", job.ID, workflowID); err != nil {
		t.Fatal(err)
	}
	write(stable, "execution-runtime.json", `{`)
	logs, err = service.GetTaskLogs(context.Background(), job.ID, workflowID, utils.LogSourceSync, "", 20, "older")
	if err != nil || len(logs.Logs) != 1 || logs.Logs[0]["message"] != "legacy connector" {
		t.Fatalf("genuine no-record history lost legacy layout: %+v %v", logs, err)
	}
	otherWorkflow := fmt.Sprintf("sync-%s-%d-resync-other-job", job.ProjectID, job.ID+1)
	if _, err := service.GetTaskLogs(context.Background(), job.ID, otherWorkflow, utils.LogSourceSync, "", 20, "older"); !errors.Is(err, constants.ErrJobNotFound) {
		t.Fatalf("legacy fallback accepted another job workflow: %v", err)
	}
}
