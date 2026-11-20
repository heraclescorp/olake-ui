package etl

import (
	"encoding/json"
	"fmt"

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
		}
	}
	return nil
}
