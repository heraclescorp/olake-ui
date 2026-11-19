// Legacy streams.json adapters keep selected catalog policy and extension metadata.
import {
	LegacyStreamsCatalog,
	SelectedStreamsByNamespace,
	StreamsDataStructure,
	StreamsV2Catalog,
} from "@/modules/ingestion/common/types"

export const fromLegacyCatalog = ({
	streams,
	selected_streams,
	default_sort_order,
	...metadata
}: LegacyStreamsCatalog): StreamsV2Catalog => ({
	available_streams: { streams },
	default_sort_order,
	catalogMetadata: { available: {}, selected: metadata },
	selected_streams: Object.fromEntries(
		Object.entries(selected_streams ?? {}).map(([namespace, entries]) => [
			namespace,
			entries.map(entry => ({
				...entry,
				append_mode: entry.append_mode ?? false,
			})),
		]),
	),
})

// Legacy drivers read routing and sync fields from streams[].
export const toLegacyStreamsConfig = (
	streamsData: StreamsDataStructure,
	selectedStreams: SelectedStreamsByNamespace,
): string => {
	const { catalogMetadata, default_sort_order, ...catalog } = streamsData
	const streams = streamsData.streams.map(s => {
		const entry = streamsData.selected_streams[s.stream.namespace || ""]?.find(
			e => e.stream_name === s.stream.name,
		)
		if (!entry) return s
		const stream = {
			...s.stream,
			sync_mode: entry.sync_mode,
			destination_database: entry.destination_database,
			destination_table: entry.destination_table ?? s.stream.destination_table,
		}
		if (entry.cursor_field) stream.cursor_field = entry.cursor_field
		else delete stream.cursor_field
		return { ...s, stream }
	})

	return JSON.stringify({
		...catalog,
		...catalogMetadata?.selected,
		streams,
		selected_streams: selectedStreams,
		default_sort_order,
	})
}
