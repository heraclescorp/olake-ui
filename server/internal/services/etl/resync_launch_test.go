package etl

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	"github.com/datazip-inc/olake-ui/server/internal/services/temporal"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

type resyncLaunchClient struct {
	client.Client
	busy            string
	busyAfterPause  bool
	initiallyPaused bool
	describeErr     error
	pauseErr        error
	resumeErr       error
	startErr        error
	paused          bool
	resumed         bool
	started         bool
	request         *temporal.ExecutionRequest
	options         client.StartWorkflowOptions
}

func (f *resyncLaunchClient) ScheduleClient() client.ScheduleClient {
	return resyncScheduleClient{f: f}
}

type resyncScheduleClient struct {
	client.ScheduleClient
	f *resyncLaunchClient
}

func (f resyncScheduleClient) GetHandle(context.Context, string) client.ScheduleHandle {
	return resyncScheduleHandle{f: f.f}
}

type resyncScheduleHandle struct {
	client.ScheduleHandle
	f *resyncLaunchClient
}

func (h resyncScheduleHandle) Describe(context.Context) (*client.ScheduleDescription, error) {
	if h.f.describeErr != nil {
		return nil, h.f.describeErr
	}
	return &client.ScheduleDescription{Schedule: client.Schedule{State: &client.ScheduleState{Paused: h.f.initiallyPaused}}}, nil
}

func (h resyncScheduleHandle) Pause(context.Context, client.SchedulePauseOptions) error {
	if h.f.pauseErr != nil {
		return h.f.pauseErr
	}
	h.f.paused = true
	return nil
}

func (h resyncScheduleHandle) Unpause(context.Context, client.ScheduleUnpauseOptions) error {
	h.f.resumed = true
	return h.f.resumeErr
}

func (f *resyncLaunchClient) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	resp := &workflowservice.ListWorkflowExecutionsResponse{}
	if (f.busy != "" && strings.Contains(req.Query, "OperationType = '"+f.busy+"'")) || (f.busyAfterPause && f.paused) {
		resp.Executions = []*workflowpb.WorkflowExecutionInfo{{}}
	}
	return resp, nil
}

func (f *resyncLaunchClient) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	if workflow != temporal.RunSyncWorkflow {
		panic("unexpected workflow")
	}
	f.started = true
	f.options = options
	f.request = args[0].(*temporal.ExecutionRequest)
	return nil, f.startErr
}

func TestResyncLaunchPreservesJobAndScheduleOnErrors(t *testing.T) {
	failure := errors.New("injected failure")
	for _, tc := range []struct {
		name        string
		fake        resyncLaunchClient
		wantErr     bool
		wantPaused  bool
		wantResumed bool
		wantStarted bool
	}{
		{name: "launch", wantPaused: true, wantStarted: true},
		{name: "sync busy", fake: resyncLaunchClient{busy: "sync"}, wantErr: true},
		{name: "clear busy", fake: resyncLaunchClient{busy: "clear-destination"}, wantErr: true},
		{name: "resync busy", fake: resyncLaunchClient{busy: "resync"}, wantErr: true},
		{name: "already paused schedule", fake: resyncLaunchClient{initiallyPaused: true}, wantErr: true},
		{name: "schedule inspection failure", fake: resyncLaunchClient{describeErr: failure}, wantErr: true},
		{name: "pause failure", fake: resyncLaunchClient{pauseErr: failure}, wantErr: true},
		{name: "sync races pause", fake: resyncLaunchClient{busyAfterPause: true}, wantErr: true, wantPaused: true, wantResumed: true},
		{name: "resume failure", fake: resyncLaunchClient{busyAfterPause: true, resumeErr: failure}, wantErr: true, wantPaused: true, wantResumed: true},
		{name: "ambiguous workflow start", fake: resyncLaunchClient{startErr: failure}, wantErr: true, wantPaused: true, wantStarted: true},
		{name: "cancelled workflow start", fake: resyncLaunchClient{startErr: context.Canceled}, wantErr: true, wantPaused: true, wantStarted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := resyncJob(t, true)
			before, _ := json.Marshal(job)
			fake := &tc.fake
			service := Service{temporal: &temporal.Temporal{Client: fake}}
			err := service.startTableResync(context.Background(), job, dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("unexpected error: %v", err)
			}
			if fake.paused != tc.wantPaused || fake.resumed != tc.wantResumed || fake.started != tc.wantStarted {
				t.Fatalf("unsafe lifecycle: paused=%v resumed=%v started=%v", fake.paused, fake.resumed, fake.started)
			}
			after, _ := json.Marshal(job)
			if string(before) != string(after) {
				t.Fatal("launch changed saved catalog/state/job")
			}
			if fake.started {
				req := fake.request
				if req.Command != temporal.Resync || req.TempPath != "" || req.Configs != nil {
					t.Fatalf("destructive or scoped execution: %+v", req)
				}
				if !strings.HasPrefix(req.WorkflowID, "sync-project-a-17-") || req.WorkflowID != fake.options.ID {
					t.Fatalf("unstable workflow identity: %+v", req)
				}
				wantTail := []string{"--resync-namespace", "public", "--resync-table", "orders", "--resync-id", req.WorkflowID}
				if !reflect.DeepEqual(req.Args[len(req.Args)-6:], wantTail) {
					t.Fatalf("incorrect source target: %v", req.Args)
				}
			}
		})
	}
}

func TestResyncHistoryHasDistinctOperationType(t *testing.T) {
	if jobRunType(temporal.Resync) != "resync" || jobRunType(temporal.ClearDestination) != "clear" || jobRunType(temporal.Sync) != "sync" {
		t.Fatal("resync must not be reported as destructive clear")
	}
	owner := "sync-project-a-17-resync-pending"
	job := resyncJob(t, true)
	job.ExecutionWorkflowID = &owner
	if !errors.Is(ensureJobExecutionIdle(job), constants.ErrJobBusy) {
		t.Fatal("retained execution owner must block mutation")
	}
}

func TestResyncCapabilityGateDoesNotTouchUnsupportedJobs(t *testing.T) {
	for _, unsupported := range []struct{ source, destination string }{
		{source: "mysql", destination: "iceberg"},
		{source: "postgres", destination: "parquet"},
	} {
		t.Run(unsupported.source+"-"+unsupported.destination, func(t *testing.T) {
			job := resyncJob(t, true)
			job.Source.Type, job.Destination.DestType = unsupported.source, unsupported.destination
			before, _ := json.Marshal(job)
			// No Temporal client: even inspecting or pausing the schedule is forbidden.
			err := (Service{}).startTableResync(context.Background(), job, dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"})
			if !errors.Is(err, constants.ErrInvalidResync) {
				t.Fatalf("unsupported capability accepted: %v", err)
			}
			after, _ := json.Marshal(job)
			if string(before) != string(after) {
				t.Fatal("unsupported resync changed saved job state/configuration")
			}
		})
	}
}

func TestResyncSupportsConfiguredPostgresBackfillHost(t *testing.T) {
	job := resyncJob(t, true)
	job.Source.Config = `{"host":"writer.example","backfill_host":"reader.example"}`
	before, _ := json.Marshal(job)
	if _, err := resolveTableResync(job, job.ProjectID, dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"}); err != nil {
		t.Fatal(err)
	}
	fake := &resyncLaunchClient{}
	service := Service{temporal: &temporal.Temporal{Client: fake}}
	if err := service.startTableResync(context.Background(), job, dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"}); err != nil {
		t.Fatal(err)
	}
	if !fake.started || fake.request.Command != temporal.Resync {
		t.Fatal("supported PostgreSQL/Iceberg job did not start resync")
	}
	after, _ := json.Marshal(job)
	if string(before) != string(after) {
		t.Fatal("resync rewrote the saved reader/writer source configuration")
	}
}
