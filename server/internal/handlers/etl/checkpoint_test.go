package etl

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/gin-gonic/gin"
)

func TestForceLSNRejectsUnauthenticatedAndMalformedRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		body          string
		authenticated bool
		want          int
	}{
		{`{"lsn":"0/123"}`, false, 401},
		{`{"lsn":"0/123","streams":["injected"]}`, true, 400},
		{`{"lsn":"0/123"} {}`, true, 400},
		{`{`, true, 400},
	} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("PUT", "/", strings.NewReader(tc.body))
		c.Params = gin.Params{{Key: "projectid", Value: "123"}, {Key: "id", Value: "9"}}
		if tc.authenticated {
			c.Set(constants.ContextUserIDKey, 7)
		}
		(&Handler{}).ForceLSN(c)
		if w.Code != tc.want {
			t.Fatalf("%s got %d want %d", tc.body, w.Code, tc.want)
		}
	}
}
