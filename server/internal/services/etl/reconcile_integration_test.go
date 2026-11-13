package etl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	etltemporal "github.com/datazip-inc/olake-ui/server/internal/services/temporal"
	"github.com/lib/pq"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
)

type recoveryHistory struct {
	events []*historypb.HistoryEvent
}

func (h *recoveryHistory) HasNext() bool { return len(h.events) != 0 }
func (h *recoveryHistory) Next() (*historypb.HistoryEvent, error) {
	event := h.events[0]
	h.events = h.events[1:]
	return event, nil
}

type recoveryClient struct {
	client.Client
	job                             *models.Job
	workflowID, ownerRun, latestRun string
	status, latestStatus            enumspb.WorkflowExecutionStatus
	requests                        []*workflowservice.ResetWorkflowExecutionRequest
	handle                          *recoveryHandle
	command                         etltemporal.Command
}

func (c *recoveryClient) DescribeWorkflowExecution(_ context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	status := c.status
	if run == "" {
		run, status = c.latestRun, c.latestStatus
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: run}, Status: status}}, nil
}
func (c *recoveryClient) GetWorkflowHistory(context.Context, string, string, bool, enumspb.HistoryEventFilterType) client.HistoryEventIterator {
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(etltemporal.ExecutionRequest{JobID: c.job.ID, ProjectID: c.job.ProjectID, Command: c.command})
	if err != nil {
		panic(err)
	}
	return &recoveryHistory{events: []*historypb.HistoryEvent{
		{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{WorkflowType: &commonpb.WorkflowType{Name: etltemporal.RunSyncWorkflow}, Input: payloads}}},
		{EventId: 4, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED},
		{EventId: 9, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{Failure: &failurepb.Failure{Message: "execution mutation outcome is not positively settled; retain job ownership"}}}},
	}}
}
func (c *recoveryClient) resetWorkflowExecution(_ context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	c.requests = append(c.requests, request)
	c.latestRun, c.latestStatus = fmt.Sprintf("recovery-%d", len(c.requests)), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	return &workflowservice.ResetWorkflowExecutionResponse{RunId: c.latestRun}, nil
}

func (c *recoveryClient) WorkflowService() workflowservice.WorkflowServiceClient {
	return recoveryWorkflowService{owner: c}
}

type recoveryWorkflowService struct {
	workflowservice.WorkflowServiceClient
	owner *recoveryClient
}

func (s recoveryWorkflowService) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	return s.owner.resetWorkflowExecution(ctx, request)
}
func (c *recoveryClient) ListWorkflow(context.Context, *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	return &workflowservice.ListWorkflowExecutionsResponse{}, nil
}
func (c *recoveryClient) ScheduleClient() client.ScheduleClient {
	return recoverySchedules{handle: c.handle}
}

type recoverySchedules struct {
	client.ScheduleClient
	handle *recoveryHandle
}

func (s recoverySchedules) GetHandle(context.Context, string) client.ScheduleHandle { return s.handle }

type recoveryHandle struct {
	client.ScheduleHandle
	schedule client.Schedule
	unpauses int
}

func (h *recoveryHandle) Describe(context.Context) (*client.ScheduleDescription, error) {
	return &client.ScheduleDescription{Schedule: h.schedule}, nil
}
func (h *recoveryHandle) Unpause(context.Context, client.ScheduleUnpauseOptions) error {
	h.unpauses++
	h.schedule.State.Paused = false
	return nil
}

func recoveryServiceFixture(t *testing.T, phase string) (Service, *models.Job, *models.User, *recoveryClient, *sql.DB) {
	t.Helper()
	service, job, actor := resyncFenceService(t)
	workflowID, run := fmt.Sprintf("sync-%s-%d-resync-original", job.ProjectID, job.ID), "original-run"
	if phase != "" {
		if err := service.db.UpdateJob(context.Background(), job.ID, map[string]any{"execution_workflow_id": workflowID, "execution_run_id": run}); err != nil {
			t.Fatal(err)
		}
	}
	connection, err := sql.Open("pgx", os.Getenv("OLAKE_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	if phase != "" {
		_, err := connection.Exec("INSERT INTO "+pq.QuoteIdentifier((&models.JobExecutionRecord{}).TableName())+" (job_id,workflow_id,owner_run_id,controlled,phase,runtime_id,launch_id,handoff_token,handoff_request_id,handoff_run_id,handoff_active) VALUES ($1,$2,$3,true,$4,'original-runtime','original-launch','bound-token','bound-request','original-run',true)", job.ID, workflowID, run, phase)
		if err != nil {
			t.Fatal(err)
		}
	}
	client := &recoveryClient{job: job, workflowID: workflowID, ownerRun: run, latestRun: run, status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, latestStatus: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, command: etltemporal.Resync,
		handle: &recoveryHandle{schedule: client.Schedule{State: &client.ScheduleState{Paused: true}, Action: &client.ScheduleWorkflowAction{ID: fmt.Sprintf("sync-%s-%d", job.ProjectID, job.ID), Workflow: etltemporal.RunSyncWorkflow, Args: []interface{}{etltemporal.ExecutionRequest{JobID: job.ID, ProjectID: job.ProjectID, Command: etltemporal.Sync}}}}}}
	service.temporal = &etltemporal.Temporal{Client: client, Namespace: "test-temporal-namespace"}
	return service, job, actor, client, connection
}

func TestReconcileCanceledExecutionIsAuthorizedStableAndNeverClearsOwner(t *testing.T) {
	for _, phase := range []string{"closing", "handoff"} {
		t.Run(phase, func(t *testing.T) {
			service, job, _, remote, _ := recoveryServiceFixture(t, phase)
			ctx := context.Background()
			for _, reset := range []bool{false, true} {
				if _, err := service.ReconcileJobExecution(ctx, "wrong-project", job.ID, reset); !errors.Is(err, constants.ErrJobNotFound) {
					t.Fatalf("cross-project recovery accepted: %v", err)
				}
			}
			status, err := service.ReconcileJobExecution(ctx, job.ProjectID, job.ID, false)
			if err != nil || !status.CanReconcile || status.Phase != phase || len(remote.requests) != 0 {
				t.Fatalf("observation failed: %+v %v", status, err)
			}
			accepted, err := service.ReconcileJobExecution(ctx, job.ProjectID, job.ID, true)
			if err != nil || accepted.Status != "reconciling" || accepted.RecoveryRunID == "" {
				t.Fatalf("reset not accepted truthfully: %+v %v", accepted, err)
			}
			request := remote.requests[0]
			if request.Namespace != "test-temporal-namespace" {
				t.Fatalf("reset omitted configured Temporal namespace: %q", request.Namespace)
			}
			if request.WorkflowExecution.WorkflowId != remote.workflowID || request.WorkflowExecution.RunId != remote.ownerRun || request.WorkflowTaskFinishEventId != 4 || request.RequestId == "" {
				t.Fatalf("reset did not bind original operation: %+v", request)
			}
			if _, err := service.ReconcileJobExecution(ctx, job.ProjectID, job.ID, true); !errors.Is(err, constants.ErrJobBusy) || len(remote.requests) != 1 {
				t.Fatalf("duplicate POST reset active recovery: %v", err)
			}
			after, err := service.db.GetJobByID(job.ID, false)
			if err != nil || after.ExecutionWorkflowID == nil || *after.ExecutionWorkflowID != remote.workflowID || *after.ExecutionRunID != remote.ownerRun {
				t.Fatalf("API changed SQL authority: %+v %v", after, err)
			}
			if phase == "handoff" {
				if _, release, err := service.db.BeginCatalogEdit(ctx, job.ID); !errors.Is(err, constants.ErrJobBusy) {
					if err == nil {
						release()
					}
					t.Fatalf("recovery weakened handoff edit fence: %v", err)
				}
			}
		})
	}
}

func TestUnknownOrdinaryRecoveryRemainsActionableWithoutFalseSuccess(t *testing.T) {
	service, job, _, remote, _ := recoveryServiceFixture(t, "closing")
	remote.command = etltemporal.Sync
	if _, err := service.ReconcileJobExecution(context.Background(), job.ProjectID, job.ID, true); err != nil {
		t.Fatal(err)
	}
	remote.latestStatus = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	status, err := service.ReconcileJobExecution(context.Background(), job.ProjectID, job.ID, false)
	if err != nil || status.Status != "blocked" || !status.CanReconcile || !strings.Contains(status.Message, "independently establish") {
		t.Fatalf("unknown outcome hidden or force-unlocked: %+v %v", status, err)
	}
	if _, err := service.ReconcileJobExecution(context.Background(), job.ProjectID, job.ID, true); err != nil {
		t.Fatal(err)
	}
	if remote.requests[0].RequestId == remote.requests[1].RequestId {
		t.Fatal("a completed recovery attempt prevented later evidence recheck")
	}
}

func TestExplicitResumeAfterRetirementRechecksOwnerAndNormalAction(t *testing.T) {
	service, job, actor, remote, connection := recoveryServiceFixture(t, "")
	status, err := service.ReconcileJobExecution(context.Background(), job.ProjectID, job.ID, false)
	if err != nil || status.Status != "idle" || !status.SchedulePaused {
		t.Fatalf("paused idle status unavailable: %+v %v", status, err)
	}
	if err := service.ActivateJob(context.Background(), job.ID, dto.JobStatusRequest{Activate: true}, &actor.ID); err != nil || remote.handle.unpauses != 1 {
		t.Fatalf("same-active resume was a no-op: %v", err)
	}
	remote.handle.schedule.Action.(*client.ScheduleWorkflowAction).Args = []interface{}{etltemporal.ExecutionRequest{JobID: job.ID, ProjectID: job.ProjectID, Command: etltemporal.Resync}}
	if err := service.ActivateJob(context.Background(), job.ID, dto.JobStatusRequest{Activate: true}, &actor.ID); err == nil || remote.handle.unpauses != 1 {
		t.Fatalf("resumed a stale resync action: %v", err)
	}
	if _, err := connection.Exec("UPDATE "+pq.QuoteIdentifier(job.TableName())+" SET execution_workflow_id='new-owner',execution_run_id='new-run' WHERE id=$1", job.ID); err != nil {
		t.Fatal(err)
	}
	if err := service.ActivateJob(context.Background(), job.ID, dto.JobStatusRequest{Activate: true}, &actor.ID); !errors.Is(err, constants.ErrJobBusy) || remote.handle.unpauses != 1 {
		t.Fatalf("stale idle UI bypassed owner check: %v", err)
	}
}

func TestReconcileNeverResetsRunningControllingWorkflow(t *testing.T) {
	service, job, _, remote, _ := recoveryServiceFixture(t, "closing")
	remote.status, remote.latestStatus = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	status, err := service.ReconcileJobExecution(context.Background(), job.ProjectID, job.ID, false)
	if err != nil || status.Status != "active" || status.CanReconcile {
		t.Fatalf("running operation was presented as resettable: %+v %v", status, err)
	}
	if _, err := service.ReconcileJobExecution(context.Background(), job.ProjectID, job.ID, true); !errors.Is(err, constants.ErrJobBusy) || len(remote.requests) != 0 {
		t.Fatalf("running workflow was reset: %v", err)
	}
}

func TestRecoveryObservationDoesNotCompeteWithWorkerFence(t *testing.T) {
	service, job, _, remote, connection := recoveryServiceFixture(t, "closing")
	remote.status, remote.latestStatus = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	ctx := context.Background()
	tx, err := connection.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", fmt.Sprintf("olake:catalog-edit:%d", job.ID)); err != nil {
		t.Fatal(err)
	}
	status, err := service.ReconcileJobExecution(ctx, job.ProjectID, job.ID, false)
	if err != nil || status.Status != "active" || status.CanReconcile || status.WorkflowID != remote.workflowID || status.RunID != remote.ownerRun {
		t.Fatalf("worker fence hid active execution: %+v %v", status, err)
	}
	if _, err := service.ReconcileJobExecution(ctx, job.ProjectID, job.ID, true); !errors.Is(err, constants.ErrJobBusy) || len(remote.requests) != 0 {
		t.Fatalf("reset escaped worker fence: %v", err)
	}
	if _, release, err := service.db.BeginCatalogEdit(ctx, job.ID); !errors.Is(err, constants.ErrJobBusy) {
		if err == nil {
			release()
		}
		t.Fatalf("observation weakened mutation fence: %v", err)
	}
}

func TestRecoverySnapshotNeverReportsOrphanHandoffIdle(t *testing.T) {
	service, job, _, remote, connection := recoveryServiceFixture(t, "handoff")
	if _, err := connection.Exec("UPDATE "+pq.QuoteIdentifier(job.TableName())+" SET execution_workflow_id=NULL,execution_run_id=NULL WHERE id=$1", job.ID); err != nil {
		t.Fatal(err)
	}
	status, err := service.ReconcileJobExecution(context.Background(), job.ProjectID, job.ID, false)
	if err != nil || status.Status != "blocked" || status.WorkflowID != remote.workflowID || status.RunID != remote.ownerRun || status.Phase != "handoff" || status.CanReconcile {
		t.Fatalf("orphaned transfer did not remain blocked: %+v %v", status, err)
	}
	if _, err := service.ReconcileJobExecution(context.Background(), job.ProjectID, job.ID, true); !errors.Is(err, constants.ErrJobBusy) || len(remote.requests) != 0 {
		t.Fatalf("orphaned transfer was reset or released: %v", err)
	}
}
