package utils

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/utils/logger"
)

// appconfig deliberately requires a real config file on first use. Bootstrap
// only this test process in a private directory; do not weaken production Load.
func TestMain(m *testing.M) {
	code := func() int {
		dir, err := os.MkdirTemp("", "olake-runtime-log-tests-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer os.RemoveAll(dir)
		originalDir, err := os.Getwd()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer os.Chdir(originalDir)
		if err := os.Mkdir(filepath.Join(dir, "config"), 0o700); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if err := os.WriteFile(filepath.Join(dir, "config", "app.yaml"), []byte("OLAKE_STORAGE_MODE: nfs\n"), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if err := os.Chdir(dir); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if err := os.Setenv("OLAKE_STORAGE_MODE", constants.StorageModeNFS); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		logger.Init()
		return m.Run()
	}()
	os.Exit(code)
}

func executionRuntimeLogFixture(t *testing.T) (string, string, *ExecutionRuntimeLogIdentity) {
	t.Helper()
	previousDir := constants.DefaultConfigDir
	constants.DefaultConfigDir = t.TempDir()
	t.Cleanup(func() { constants.DefaultConfigDir = previousDir })
	workflowID := "sync-project-17-resync-operation"
	baseDir := filepath.Join(constants.DefaultConfigDir, WorkflowHash(workflowID))
	execution := &ExecutionRuntimeLogIdentity{RuntimeID: "recovery-runtime", LaunchID: "admitted-launch"}
	write := func(name, content string) {
		t.Helper()
		file := filepath.Join(constants.DefaultConfigDir, name)
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	current, launch := WorkflowHash(execution.RuntimeID), WorkflowHash(execution.LaunchID)
	stable := filepath.Base(baseDir)
	write(filepath.Join(stable, "state.json"), `{"runtime":"stable"}`)
	write(filepath.Join(stable, "logs/worker.log"), "{\"level\":\"info\",\"time\":\"2026-11-01T00:00:01Z\",\"message\":\"stable worker\"}\n")
	write(filepath.Join(stable, "logs/sync_old/olake.log"), "{\"level\":\"info\",\"time\":\"2026-11-01T00:00:02Z\",\"message\":\"legacy stable connector\"}\n")
	write(filepath.Join(current, "logs/sync_2026_11_01/olake.log"), "{\"level\":\"info\",\"time\":\"2026-11-01T00:00:03Z\",\"message\":\"current recovery connector\"}\n")
	write(filepath.Join(current, "state.json"), `{"runtime":"current"}`)
	write(filepath.Join(launch, "logs/sync_2026_10_31/olake.log"), "{\"level\":\"info\",\"time\":\"2026-10-31T00:00:03Z\",\"message\":\"original admitted connector\"}\n")
	write(filepath.Join(launch, "state.json"), `{"runtime":"original"}`)
	// A valid old acknowledged Put may complete after recovery's pointer Put.
	// It must not select the current runtime, despite looking well formed.
	pointer, err := json.Marshal(map[string]any{
		"version": 1, "workflow_id": workflowID, "run_id": "stale-run",
		"runtime_id": execution.LaunchID, "launch_id": execution.LaunchID, "subdir": launch,
	})
	if err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(stable, "execution-runtime.json"), string(pointer))
	return baseDir, workflowID, execution
}

func TestTaskLogsUseSQLRuntimeIdentityAndKeepStableWorker(t *testing.T) {
	baseDir, _, execution := executionRuntimeLogFixture(t)
	logs, err := ReadTaskLogs(context.Background(), baseDir, execution, LogSourceAll, "", 20, "older")
	if err != nil {
		t.Fatal(err)
	}
	if len(logs.Logs) != 2 || logs.Logs[0]["message"] != "stable worker" || logs.Logs[1]["message"] != "current recovery connector" {
		t.Fatalf("stale pointer selected another runtime: %+v", logs.Logs)
	}
	if _, err := ReadTaskLogs(context.Background(), baseDir, execution, LogSourceAll, logs.OlderCursor, 20, "older"); err != nil {
		t.Fatalf("same-runtime cursor rejected: %v", err)
	}
	replacement := &ExecutionRuntimeLogIdentity{RuntimeID: "next-recovery-runtime", LaunchID: execution.LaunchID}
	if _, err := ReadTaskLogs(context.Background(), baseDir, replacement, LogSourceAll, logs.OlderCursor, 20, "older"); !errors.Is(err, constants.ErrInvalidLogCursor) {
		t.Fatalf("old runtime cursor was reused for replacement logs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(baseDir, "execution-runtime.json"), []byte(`{`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadTaskLogs(context.Background(), baseDir, execution, LogSourceSync, "", 20, "older"); err != nil {
		t.Fatalf("mutable pointer was consulted despite SQL authority: %v", err)
	}
}

func TestLegacyFallbackRequiresNoExecutionRecord(t *testing.T) {
	baseDir, _, _ := executionRuntimeLogFixture(t)
	legacy, err := ReadTaskLogs(context.Background(), baseDir, nil, LogSourceAll, "", 20, "older")
	if err != nil || len(legacy.Logs) != 2 || legacy.Logs[1]["message"] != "legacy stable connector" {
		t.Fatalf("no-record legacy layout was not preserved: %+v %v", legacy, err)
	}
	unadmitted := &ExecutionRuntimeLogIdentity{}
	current, launch, err := ResolveExecutionRuntimeLogDirs(baseDir, unadmitted)
	if err != nil || current != "" || launch != "" {
		t.Fatalf("unadmitted row fell back to legacy: %s %s %v", current, launch, err)
	}
	logs, err := ReadTaskLogs(context.Background(), baseDir, unadmitted, LogSourceAll, "", 20, "older")
	if err != nil || len(logs.Logs) != 1 || logs.Logs[0]["message"] != "stable worker" {
		t.Fatalf("unadmitted row exposed connector fallback: %+v %v", logs, err)
	}
	if _, err := GetLogArchiveFilename(context.Background(), 17, baseDir, unadmitted); !errors.Is(err, constants.ErrSyncFolderNotFound) {
		t.Fatalf("unadmitted archive used stale connector files: %v", err)
	}
	for _, incomplete := range []*ExecutionRuntimeLogIdentity{{RuntimeID: "runtime"}, {LaunchID: "launch"}, {RuntimeID: " ", LaunchID: "launch"}} {
		if _, _, err := ResolveExecutionRuntimeLogDirs(baseDir, incomplete); err == nil {
			t.Fatal("incomplete recorded identity silently enabled fallback")
		}
	}
}

func TestRuntimeLogDirectoriesHashIdentityWithoutTraversal(t *testing.T) {
	baseDir, _, _ := executionRuntimeLogFixture(t)
	execution := &ExecutionRuntimeLogIdentity{RuntimeID: "../outside", LaunchID: "/absolute/other"}
	current, launch, err := ResolveExecutionRuntimeLogDirs(baseDir, execution)
	if err != nil || filepath.Dir(current) != constants.DefaultConfigDir || filepath.Dir(launch) != constants.DefaultConfigDir || filepath.Base(current) != fmt.Sprintf("%x", sha256.Sum256([]byte(execution.RuntimeID))) {
		t.Fatalf("runtime identity escaped configured sibling namespace: %s %s %v", current, launch, err)
	}
}

func TestRecordedRuntimeLogsDoNotRequireStablePointerOrPrefix(t *testing.T) {
	baseDir, workflowID, execution := executionRuntimeLogFixture(t)
	if err := os.RemoveAll(baseDir); err != nil {
		t.Fatal(err)
	}
	resolvedBase, err := GetAndValidateLogBaseDir(context.Background(), workflowID, execution)
	if err != nil || resolvedBase != baseDir {
		t.Fatalf("SQL authority required a mutable stable prefix: %s %v", resolvedBase, err)
	}
	logs, err := ReadTaskLogs(context.Background(), resolvedBase, execution, LogSourceSync, "", 20, "older")
	if err != nil || len(logs.Logs) != 1 || logs.Logs[0]["message"] != "current recovery connector" {
		t.Fatalf("current runtime hidden before stable worker log flush: %+v %v", logs, err)
	}
}

func TestRuntimeArchiveIncludesSQLSelectedCurrentLaunchAndWorker(t *testing.T) {
	baseDir, _, execution := executionRuntimeLogFixture(t)
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := AddFilesToArchive(context.Background(), baseDir, execution, writer); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	entries := map[string]string{}
	reader := tar.NewReader(&archive)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if _, duplicate := entries[header.Name]; duplicate {
			t.Fatalf("archive overwrote a runtime namespace: %s", header.Name)
		}
		entries[header.Name] = string(body)
	}
	if entries["state.json"] != `{"runtime":"current"}` || entries["launch/state.json"] != `{"runtime":"original"}` || entries["logs/olake.log"] == "" || entries["launch/logs/olake.log"] == "" || entries["workflow/logs/worker.log"] == "" {
		t.Fatalf("archive selected stale pointer or lost worker/launch data: %+v", entries)
	}
	filename, err := GetLogArchiveFilename(context.Background(), 17, baseDir, execution)
	if err != nil || filename != "job-17-logs-2026-11-01.tar.gz" {
		t.Fatalf("archive filename did not follow SQL identity: %s %v", filename, err)
	}
}
