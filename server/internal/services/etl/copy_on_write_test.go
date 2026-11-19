package etl

import (
	"errors"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
)

func TestValidateStreamsFormatCOWStructure(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, tc := range []struct {
			name, fields string
			valid        bool
		}{
			{name: "old default", valid: true},
			{name: "explicit MOR", fields: `,"merge_strategy":"merge-on-read"`, valid: true},
			{name: "COW", fields: `,"merge_strategy":"copy-on-write"`, valid: true},
			{name: "COW with extensions", fields: `,"merge_strategy":"copy-on-write","update_type":"dv","extension":{"large":9007199254740993}`, valid: true},
			{name: "inactive append policy", fields: `,"append_mode":true,"merge_strategy":"copy-on-write"`, valid: true},
			{name: "unknown strategy", fields: `,"merge_strategy":"cow"`},
			{name: "null strategy", fields: `,"merge_strategy":null`},
			{name: "nonstring strategy", fields: `,"merge_strategy":1`},
		} {
			t.Run(map[bool]string{false: "legacy/", true: "split/"}[split]+tc.name, func(t *testing.T) {
				selected := `{"default_sort_order":[],"extension":{"large":9007199254740993},"selected_streams":{"public":[{"stream_name":"orders","sort_order":[]` + tc.fields + `}]}}`
				legacy, available := "", `{"streams":[{"stream":{"name":"orders","namespace":"public","supports_iceberg_copy_on_write":true}}]}`
				if !split {
					legacy, available, selected = strings.Replace(selected, `{`, `{"streams":[],`, 1), "", ""
				}
				err := validateStreamsFormat(legacy, available, selected, "v1.0.0", nil)
				if tc.valid && err != nil {
					t.Fatal(err)
				}
				if !tc.valid && !errors.Is(err, constants.ErrStreamsFormat) {
					t.Fatalf("malformed COW configuration accepted: %v", err)
				}
			})
		}
	}
}
