package temporal

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// InspectSyncSchedule must be called under the job's catalog exclusion fence.
// Never unpause an old clear/resync action after its execution owner is retired.
func (t *Temporal) InspectSyncSchedule(ctx context.Context, projectID string, jobID int) (bool, error) {
	workflowID, scheduleID := t.WorkflowAndScheduleID(projectID, jobID)
	description, err := t.Client.ScheduleClient().GetHandle(ctx, scheduleID).Describe(ctx)
	if err != nil {
		return false, fmt.Errorf("cannot inspect normal sync schedule: %w", err)
	}
	if description == nil || description.Schedule.State == nil {
		return false, fmt.Errorf("schedule pause state is unavailable")
	}
	action, ok := description.Schedule.Action.(*client.ScheduleWorkflowAction)
	if !ok || action.ID != workflowID || action.Workflow != RunSyncWorkflow || len(action.Args) != 1 {
		return false, fmt.Errorf("%w: schedule action is not the normal sync action; reconcile the original operation before resuming", constants.ErrJobBusy)
	}
	var request ExecutionRequest
	switch argument := action.Args[0].(type) {
	case *commonpb.Payload:
		if err := converter.GetDefaultDataConverter().FromPayload(argument, &request); err != nil {
			return false, fmt.Errorf("cannot decode schedule action: %w", err)
		}
	case ExecutionRequest:
		request = argument
	case *ExecutionRequest:
		if argument == nil {
			return false, fmt.Errorf("schedule sync request is missing")
		}
		request = *argument
	default:
		return false, fmt.Errorf("schedule sync request cannot be positively identified")
	}
	if request.Command != Sync || request.JobID != jobID || request.ProjectID != projectID {
		return false, fmt.Errorf("%w: schedule is bound to another command or job; reconcile before resuming", constants.ErrJobBusy)
	}
	return description.Schedule.State.Paused, nil
}
