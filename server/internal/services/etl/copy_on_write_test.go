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
			{name: "COW defaults", fields: `,"merge_strategy":"copy-on-write","cow_options":{}`, valid: true},
			{name: "complete COW options with extensions", fields: `,"merge_strategy":"copy-on-write","update_type":"dv","cow_options":{"flush_interval_seconds":300,"max_batch_rows":100000,"max_batch_bytes":67108864,"rewrite_concurrency":2,"materialize_existing_deletes":true,"extension":{"large":9007199254740993}}`, valid: true},
			{name: "inactive append policy", fields: `,"append_mode":true,"merge_strategy":"copy-on-write","cow_options":{"materialize_existing_deletes":false}`, valid: true},
			{name: "unknown strategy", fields: `,"merge_strategy":"cow"`},
			{name: "null strategy", fields: `,"merge_strategy":null`},
			{name: "nonstring strategy", fields: `,"merge_strategy":1`},
			{name: "null options", fields: `,"cow_options":null`},
			{name: "array options", fields: `,"cow_options":[]`},
			{name: "string options", fields: `,"cow_options":"defaults"`},
			{name: "zero interval", fields: `,"cow_options":{"flush_interval_seconds":0}`},
			{name: "maximum interval", fields: `,"cow_options":{"flush_interval_seconds":9223372036}`, valid: true},
			{name: "overflowing interval", fields: `,"cow_options":{"flush_interval_seconds":9223372037}`},
			{name: "negative rows", fields: `,"cow_options":{"max_batch_rows":-1}`},
			{name: "fractional bytes", fields: `,"cow_options":{"max_batch_bytes":1.5}`},
			{name: "fractional concurrency", fields: `,"cow_options":{"rewrite_concurrency":2.5}`},
			{name: "string integer", fields: `,"cow_options":{"max_batch_rows":"100"}`},
			{name: "null integer", fields: `,"cow_options":{"max_batch_rows":null}`},
			{name: "unsafe integer", fields: `,"cow_options":{"max_batch_bytes":9007199254740992}`},
			{name: "string permission", fields: `,"cow_options":{"materialize_existing_deletes":"true"}`},
			{name: "null permission", fields: `,"cow_options":{"materialize_existing_deletes":null}`},
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
