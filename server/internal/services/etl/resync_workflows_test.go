package etl

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/services/temporal"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

type runningResyncWorkflowClient struct {
	client.Client
	requests []*workflowservice.ListWorkflowExecutionsRequest
	running  bool
}

func (c *runningResyncWorkflowClient) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	c.requests = append(c.requests, req)
	response := &workflowservice.ListWorkflowExecutionsResponse{}
	if c.running {
		response.Executions = []*workflowpb.WorkflowExecutionInfo{{}}
	}
	return response, nil
}

func TestResyncWaitRejectsRunningSyncWithoutWaitingOrCancelling(t *testing.T) {
	for _, running := range []bool{false, true} {
		fake := &runningResyncWorkflowClient{running: running}
		err := waitForSyncToStop(context.Background(), &temporal.Temporal{Client: fake}, "project-a", 17, 0)
		if running && !errors.Is(err, constants.ErrJobBusy) {
			t.Fatalf("running sync was not rejected: %v", err)
		}
		if !running && err != nil {
			t.Fatal(err)
		}
		if len(fake.requests) != 1 {
			t.Fatalf("unexpected polling: %d", len(fake.requests))
		}
		query := fake.requests[0].Query
		for _, required := range []string{"sync-project-a-17-", "OperationType = 'sync'", "ExecutionStatus = 'Running'"} {
			if !strings.Contains(query, required) {
				t.Fatalf("busy check is not job/project/operation scoped: %s", query)
			}
		}
	}
}

func TestResyncChecksClearOperationWithSameJobScope(t *testing.T) {
	fake := &runningResyncWorkflowClient{running: true}
	running, _, err := isWorkflowRunning(context.Background(), &temporal.Temporal{Client: fake}, "project-a", 17, temporal.ClearDestination)
	if err != nil {
		t.Fatal(err)
	}
	if !running || len(fake.requests) != 1 {
		t.Fatal("running clear was not found")
	}
	query := fake.requests[0].Query
	for _, required := range []string{"sync-project-a-17-", "OperationType = 'clear-destination'", "ExecutionStatus = 'Running'"} {
		if !strings.Contains(query, required) {
			t.Fatalf("clear check is not job/project scoped: %s", query)
		}
	}
}
