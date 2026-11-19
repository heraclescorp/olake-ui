package etl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
)

// Validate the wire shape without reserializing the raw catalog: extension
// properties, large JSON numbers and omitted/default/empty policies must survive
// discovery, API saves and GitOps reconciliation unchanged. The connector owns
// actual source capability, native destination, key and execution-fence checks.
func validateCatalogCOW(selection string) error {
	var catalog struct {
		SelectedStreams map[string][]struct {
			StreamName    string          `json:"stream_name"`
			MergeStrategy json.RawMessage `json:"merge_strategy"`
			COWOptions    json.RawMessage `json:"cow_options"`
		} `json:"selected_streams"`
	}
	if err := json.Unmarshal([]byte(selection), &catalog); err != nil {
		return fmt.Errorf("%w: invalid selected catalog: %s", constants.ErrStreamsFormat, err)
	}
	for namespace, streams := range catalog.SelectedStreams {
		for _, stream := range streams {
			field := namespace + "." + stream.StreamName
			invalid := func(reason string) error {
				return fmt.Errorf("%w: %s %s", constants.ErrStreamsFormat, field, reason)
			}
			if len(stream.MergeStrategy) != 0 {
				var strategy string
				if err := json.Unmarshal(stream.MergeStrategy, &strategy); err != nil || (strategy != "merge-on-read" && strategy != "copy-on-write") {
					return invalid("merge_strategy must be merge-on-read or copy-on-write (omit for merge-on-read)")
				}
			}
			if len(stream.COWOptions) == 0 {
				continue
			}
			var options map[string]json.RawMessage
			if err := json.Unmarshal(stream.COWOptions, &options); err != nil || options == nil {
				return invalid("cow_options must be an object")
			}
			for _, key := range []string{"flush_interval_seconds", "max_batch_rows", "max_batch_bytes", "rewrite_concurrency"} {
				raw, exists := options[key]
				if !exists {
					continue
				}
				value, err := strconv.ParseInt(string(bytes.TrimSpace(raw)), 10, 64)
				if err != nil || value <= 0 || value > 9007199254740991 {
					return invalid("cow_options." + key + " must be a positive safe integer")
				}
				if key == "flush_interval_seconds" && value > 9223372036 {
					return invalid("cow_options.flush_interval_seconds exceeds the supported duration")
				}
			}
			if raw, exists := options["materialize_existing_deletes"]; exists {
				raw = bytes.TrimSpace(raw)
				if !bytes.Equal(raw, []byte("true")) && !bytes.Equal(raw, []byte("false")) {
					return invalid("cow_options.materialize_existing_deletes must be a boolean")
				}
			}
		}
	}
	return nil
}
