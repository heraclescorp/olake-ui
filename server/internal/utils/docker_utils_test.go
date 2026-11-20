package utils

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestECRImageTagsPagination(t *testing.T) {
	cases := []struct {
		name         string
		firstPage    string
		secondStatus int
		secondPage   string
		want         []string
		wantError    string
	}{
		{
			name:         "stable tags after an empty page",
			firstPage:    `{"imageDetails":[],"nextToken":"page-two"}`,
			secondStatus: http.StatusOK,
			secondPage:   `{"imageDetails":[{"imageTags":["latest","dev-test","stag-test","<none>","v1.1.1","v0.9.5"]}]}`,
			want:         []string{"v1.1.1", "v0.9.5"},
		},
		{
			name:         "later page failure rejects partial versions",
			firstPage:    `{"imageDetails":[{"imageTags":["v1.0.0"]}],"nextToken":"page-two"}`,
			secondStatus: http.StatusBadRequest,
			secondPage:   `{"__type":"InvalidParameterException","message":"later page failed"}`,
			wantError:    "later page failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var input struct {
					NextToken string `json:"nextToken"`
					Filter    struct {
						TagStatus string `json:"tagStatus"`
					} `json:"filter"`
				}
				if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
					http.Error(w, "invalid request", http.StatusBadRequest)
					return
				}
				if input.Filter.TagStatus != "TAGGED" {
					http.Error(w, "tagged-image filter required", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/x-amz-json-1.1")
				switch input.NextToken {
				case "":
					_, _ = w.Write([]byte(tc.firstPage))
				case "page-two":
					w.WriteHeader(tc.secondStatus)
					_, _ = w.Write([]byte(tc.secondPage))
				default:
					http.Error(w, "unexpected pagination token", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			for key, value := range map[string]string{
				"AWS_ACCESS_KEY_ID":                   "test",
				"AWS_SECRET_ACCESS_KEY":               "test",
				"AWS_SESSION_TOKEN":                   "",
				"AWS_PROFILE":                         "",
				"AWS_DEFAULT_PROFILE":                 "",
				"AWS_EC2_METADATA_DISABLED":           "true",
				"AWS_ENDPOINT_URL_ECR":                server.URL,
				"AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false",
				"AWS_MAX_ATTEMPTS":                    "1",
			} {
				t.Setenv(key, value)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tags, err := getECRImageTags(ctx, "123456789012.dkr.ecr.us-east-2.amazonaws.com/olakego/source-postgres")
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("error = %v, want %q", err, tc.wantError)
				}
				if tags != nil {
					t.Fatalf("returned partial versions on failure: %v", tags)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			sort.Strings(tags)
			sort.Strings(tc.want)
			if !reflect.DeepEqual(tags, tc.want) {
				t.Fatalf("tags = %v, want %v", tags, tc.want)
			}
		})
	}
}
