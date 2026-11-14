package etl

import (
	"context"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	etltemporal "github.com/datazip-inc/olake-ui/server/internal/services/temporal"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
)

func terminalExecution(status enumspb.WorkflowExecutionStatus) bool {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return true
	default:
		return false
	}
}

func recoveryFailure(failure *failurepb.Failure) string {
	var messages []string
	for depth := 0; failure != nil && depth < 8; depth++ {
		if failure.Message != "" {
			messages = append(messages, failure.Message)
		}
		failure = failure.Cause
	}
	return strings.Join(messages, ": ")
}

// GET is observation only. POST resets the exact retained operation; admission
// still requires worker-side fence closure, exact resource retirement and proof.
func (s Service) ReconcileJobExecution(ctx context.Context, projectID string, jobID int, reset bool) (dto.JobExecutionRecovery, error) {
	if reset {
		var release func()
		var err error
		ctx, release, err = s.db.BeginExecutionRecovery(ctx, jobID)
		if err != nil {
			return dto.JobExecutionRecovery{}, err
		}
		defer release()
	}
	job, record, err := s.db.GetExecutionRecoverySnapshot(ctx, jobID)
	if err != nil {
		return dto.JobExecutionRecovery{}, err
	}
	if job.ProjectID != projectID {
		return dto.JobExecutionRecovery{}, constants.ErrJobNotFound
	}
	result := dto.JobExecutionRecovery{Status: "idle", Message: "No retained execution owner. New sync and table resync requests use the normal admission checks."}
	if job.ExecutionWorkflowID == nil && job.ExecutionRunID == nil {
		if record != nil {
			result.Status, result.WorkflowID, result.RunID, result.Phase = "blocked", record.WorkflowID, record.OwnerRunID, record.Phase
			result.Message = "A published checkpoint handoff remains bound, but the job owner is missing. Restore the original ownership binding with the worker/database operator; no resume, new data launch or owner clear is permitted."
			return recoveryNotReady(result, reset)
		}
		if reset {
			return result, fmt.Errorf("%w: no retained execution to reconcile", constants.ErrJobBusy)
		}
		paused, err := s.temporal.InspectSyncSchedule(ctx, projectID, jobID)
		if err != nil {
			return result, err
		}
		result.SchedulePaused = paused
		if result.SchedulePaused {
			result.Message = "No retained execution owner. The ordinary schedule remains paused. Explicitly resume the schedule before requesting a new sync or table resync."
		}
		return result, nil
	}
	result.Status, result.Message = "blocked", "Execution ownership is incomplete. An operator must reconcile the original worker authority; ownership has not been cleared."
	if job.ExecutionWorkflowID == nil || job.ExecutionRunID == nil || *job.ExecutionWorkflowID == "" || *job.ExecutionRunID == "" {
		return recoveryNotReady(result, reset)
	}
	result.WorkflowID, result.RunID = *job.ExecutionWorkflowID, *job.ExecutionRunID
	if record == nil || record.OwnerRunID != result.RunID || !record.Controlled || !strings.HasPrefix(result.WorkflowID, fmt.Sprintf("sync-%s-%d-", projectID, jobID)) {
		result.Message = "The retained owner has no matching controlled execution record. Reconcile it with its original worker; this API cannot safely downgrade or force-unlock it."
		return recoveryNotReady(result, reset)
	}
	result.Phase, result.Outcome = record.Phase, record.Outcome
	old, err := s.temporal.Client.DescribeWorkflowExecution(ctx, result.WorkflowID, result.RunID)
	if err != nil {
		return result, fmt.Errorf("cannot inspect controlling execution: %w", err)
	}
	if old == nil || old.WorkflowExecutionInfo == nil || old.WorkflowExecutionInfo.Execution == nil || old.WorkflowExecutionInfo.Execution.WorkflowId != result.WorkflowID || old.WorkflowExecutionInfo.Execution.RunId != result.RunID {
		return result, fmt.Errorf("controlling execution identity could not be confirmed")
	}
	result.TemporalStatus = old.WorkflowExecutionInfo.Status.String()
	if !terminalExecution(old.WorkflowExecutionInfo.Status) {
		result.Status, result.Message = "active", "The controlling workflow is still active or not positively terminal. Cancellation must finish before reconciliation; no running workflow will be reset."
		return recoveryNotReady(result, reset)
	}
	latest, err := s.temporal.Client.DescribeWorkflowExecution(ctx, result.WorkflowID, "")
	if err != nil {
		return result, fmt.Errorf("cannot inspect latest operation run: %w", err)
	}
	if latest == nil || latest.WorkflowExecutionInfo == nil || latest.WorkflowExecutionInfo.Execution == nil || latest.WorkflowExecutionInfo.Execution.WorkflowId != result.WorkflowID {
		return result, fmt.Errorf("latest operation identity could not be confirmed")
	}
	latestRunID := latest.WorkflowExecutionInfo.Execution.RunId
	if latestRunID == "" {
		return result, fmt.Errorf("latest operation run identity is missing")
	}
	previousRecoveryFailed := latestRunID != result.RunID
	if previousRecoveryFailed && !terminalExecution(latest.WorkflowExecutionInfo.Status) {
		result.Status, result.Message = "reconciling", "A recovery run is active. Ownership remains bound until the worker positively confirms retirement or publication and completes any pending handoff."
		return recoveryNotReady(result, reset)
	}
	var resetEvent int64
	validInput := false
	iterator := s.temporal.Client.GetWorkflowHistory(ctx, result.WorkflowID, result.RunID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return result, fmt.Errorf("cannot read original operation history: %w", err)
		}
		if started := event.GetWorkflowExecutionStartedEventAttributes(); started != nil {
			var input etltemporal.ExecutionRequest
			if started.WorkflowType != nil && started.WorkflowType.Name == etltemporal.RunSyncWorkflow && converter.GetDefaultDataConverter().FromPayloads(started.Input, &input) == nil {
				validInput = input.JobID == jobID && input.ProjectID == projectID && (input.Command == etltemporal.Sync || input.Command == etltemporal.ClearDestination || input.Command == etltemporal.Resync)
			}
		}
		if resetEvent == 0 && event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			resetEvent = event.EventId
		}
		if failed := event.GetActivityTaskFailedEventAttributes(); failed != nil {
			result.LastError = recoveryFailure(failed.Failure)
		}
		if failed := event.GetWorkflowExecutionFailedEventAttributes(); failed != nil {
			result.LastError = recoveryFailure(failed.Failure)
		}
	}
	if !validInput || resetEvent == 0 {
		result.Message = "The original workflow history does not contain a matching supported job request and safe reset point. Restore access to the original operation history and worker; no replacement data launch has been started."
		return recoveryNotReady(result, reset)
	}
	result.Status, result.CanReconcile = "awaiting_reconciliation", true
	result.Message = "Reconcile this same operation to check its immutable completion evidence. Known unpublished resyncs and settled ordinary runs can retire; uncertain resyncs use read-only publication recovery. Unknown ordinary receipts require independently established remote-outcome evidence and cannot be force-unlocked or reclassified from logs. A pending handoff remains bound."
	if previousRecoveryFailed || record.Outcome == "recovery" {
		result.Status = "blocked"
		result.Message = "The last recovery run ended without retiring this owner. The remote outcome is still unconfirmed. Inspect recovery worker logs and restore access to the immutable receipt or independently establish the remote outcome with the connector/catalog operator. After that prerequisite is resolved, Reconcile rechecks the same operation; it never force-clears ownership or treats logs as proof."
	}
	if !reset {
		return result, nil
	}
	response, err := s.temporal.Client.WorkflowService().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.temporal.Namespace,
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: result.WorkflowID, RunId: result.RunID},
		WorkflowTaskFinishEventId: resetEvent,
		RequestId:                 uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("olake-reconcile/%d/%s/%s/%s", jobID, result.WorkflowID, result.RunID, latestRunID))).String(),
		Reason:                    "Operator requested proof-backed recovery of the retained job execution; no fresh data launch",
		ResetReapplyType:          enumspb.RESET_REAPPLY_TYPE_NONE,
	})
	if err != nil {
		return result, fmt.Errorf("reconciliation reset was not confirmed; refresh status before retrying: %w", err)
	}
	if response == nil || response.RunId == "" {
		return result, fmt.Errorf("reconciliation reset returned no controlling run identity; refresh status")
	}
	result.Status, result.CanReconcile, result.RecoveryRunID = "reconciling", false, response.RunId
	result.Message = "Reconciliation was accepted, not completed. The worker must still prove the remote outcome and exact resource retirement; ownership and any pending handoff remain protected until then."
	return result, nil
}

func recoveryNotReady(result dto.JobExecutionRecovery, reset bool) (dto.JobExecutionRecovery, error) {
	if reset {
		return result, fmt.Errorf("%w: %s", constants.ErrJobBusy, result.Message)
	}
	return result, nil
}
