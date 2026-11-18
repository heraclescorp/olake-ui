package etl

import (
	"errors"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
)

const catalogSortPreset = `[{"column":"created_at","direction":"asc","null_order":"nulls-last"}]`
const catalogCustomSort = `[{"column":"amount","direction":"desc","null_order":"nulls-first"},{"column":"id","direction":"asc","null_order":"nulls-last"}]`

func TestValidateStreamsFormatSortStructure(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, owner := range []string{"default", "table"} {
			for _, tc := range []struct {
				name, order string
				valid       bool
			}{
				{name: "omitted", valid: true},
				{name: "unsorted", order: `[]`, valid: true},
				{name: "preset", order: catalogSortPreset, valid: true},
				{name: "null", order: `null`},
				{name: "object", order: `{}`},
				{name: "string", order: `"created_at"`},
				{name: "null field", order: `[null]`},
				{name: "string field", order: `["created_at"]`},
				{name: "blank column", order: `[{"column":" ","direction":"asc","null_order":"nulls-last"}]`},
				{name: "nonstring column", order: `[{"column":4,"direction":"asc","null_order":"nulls-last"}]`},
				{name: "missing direction", order: `[{"column":"created_at","null_order":"nulls-last"}]`},
				{name: "invalid direction", order: `[{"column":"created_at","direction":"ASC","null_order":"nulls-last"}]`},
				{name: "missing null order", order: `[{"column":"created_at","direction":"asc"}]`},
				{name: "invalid null order", order: `[{"column":"created_at","direction":"asc","null_order":"last"}]`},
				{name: "duplicate column", order: `[{"column":"created_at","direction":"asc","null_order":"nulls-last"},{"column":"created_at","direction":"desc","null_order":"nulls-first"}]`},
			} {
				t.Run(map[bool]string{false: "legacy/", true: "split/"}[split]+owner+"/"+tc.name, func(t *testing.T) {
					root, table := "", ""
					if tc.order != "" {
						if owner == "default" {
							root = `"default_sort_order":` + tc.order + `,`
						} else {
							table = `,"sort_order":` + tc.order
						}
					}
					selected := `{` + root + `"selected_streams":{"public":[{"stream_name":"orders"` + table + `}]}}`
					legacy, available := "", `{"streams":[]}`
					if !split {
						legacy, available, selected = strings.Replace(selected, `{`, `{"streams":[],`, 1), "", ""
					}
					err := validateStreamsFormat(legacy, available, selected, "v1.0.0", nil)
					if tc.valid && err != nil {
						t.Fatal(err)
					}
					if !tc.valid && !errors.Is(err, constants.ErrStreamsFormat) {
						t.Fatalf("malformed sort configuration accepted: %v", err)
					}
				})
			}
		}
	}
}
