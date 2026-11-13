package etl

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestExecutionRecoveryRejectsUnauthenticatedRequestsBeforeServiceAccess(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			handler := &Handler{}
			router := gin.New()
			if method == http.MethodGet {
				router.GET("/project/:projectid/jobs/:id/reconcile", handler.GetJobExecutionRecovery)
			} else {
				router.POST("/project/:projectid/jobs/:id/reconcile", handler.ReconcileJobExecution)
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(method, "/project/project-a/jobs/42/reconcile", nil))
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated recovery reached service: status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}
