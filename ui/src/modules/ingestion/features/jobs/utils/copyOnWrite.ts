import type {
	SelectedStream,
	StreamData,
	StreamsDataStructure,
} from "@/modules/ingestion/common/types"

export const COW_CAPABILITY_WARNING =
	"This source build did not advertise supports_iceberg_copy_on_write. The saved choice is preserved, not silently applied as merge-on-read. Update the connector and refresh discovery, or explicitly select Merge-on-read to disable COW."

export interface COWConnectionSettings {
	sourceConfig?: unknown
	destinationConfig?: unknown
}

const configObject = (config: unknown): Record<string, unknown> | null => {
	if (typeof config === "string") {
		try {
			return configObject(JSON.parse(config))
		} catch {
			return null
		}
	}
	return config !== null && typeof config === "object" && !Array.isArray(config)
		? (config as Record<string, unknown>)
		: null
}

export const copyOnWriteEnabled = (selected: SelectedStream): boolean =>
	!selected.append_mode && selected.merge_strategy === "copy-on-write"

// Discovery advertises the writer; runtime validates source and table compatibility.
export const getCOWEligibilityError = ({
	stream,
	selected,
	sourceType,
	destinationType,
	sourceConfig,
	destinationConfig,
}: COWConnectionSettings & {
	stream: StreamData
	selected: SelectedStream
	sourceType?: string
	destinationType?: string
}): string | null => {
	if (sourceType?.toLowerCase() !== "postgres") {
		return "Copy-on-write requires PostgreSQL CDC."
	}
	if (destinationType?.toLowerCase() !== "iceberg") {
		return "Copy-on-write requires native Iceberg."
	}
	if (stream.stream.supports_iceberg_copy_on_write !== true) {
		return COW_CAPABILITY_WARNING
	}
	if (
		!(
			selected.normalization ??
			stream.stream.default_stream_properties?.normalization
		)
	) {
		return "Enable normalization before selecting copy-on-write."
	}
	const mode = selected.sync_mode ?? stream.stream.sync_mode
	if (mode !== "cdc" && mode !== "strict_cdc") {
		return "Copy-on-write requires PostgreSQL CDC."
	}
	if (sourceConfig !== undefined) {
		const config = configObject(sourceConfig)
		const update = configObject(config?.update_method)
		if (
			update?.toast_value_source !== undefined &&
			update.toast_value_source !== "" &&
			update.toast_value_source !== "source"
		) {
			return "Copy-on-write requires source TOAST hydration; destination hydration needs a persistent index."
		}
	}
	if (destinationConfig !== undefined && !configObject(destinationConfig)) {
		return "Native Iceberg destination configuration is unavailable."
	}
	const keys = stream.stream.source_defined_primary_key ?? []
	if (keys.length === 0) return "Copy-on-write requires a source primary key."
	return null
}

export const validateCatalogCOW = (
	catalog: StreamsDataStructure,
	sourceType?: string,
	destinationType?: string,
	connections: COWConnectionSettings = {},
): string | null => {
	for (const [namespace, entries] of Object.entries(catalog.selected_streams)) {
		for (const selected of entries) {
			if (selected.disabled) continue
			const name = `[${namespace ? `${namespace}.` : ""}${selected.stream_name}]`
			if (
				selected.merge_strategy !== undefined &&
				selected.merge_strategy !== "merge-on-read" &&
				selected.merge_strategy !== "copy-on-write"
			) {
				return `${name} merge_strategy must be merge-on-read or copy-on-write.`
			}
			if (!copyOnWriteEnabled(selected)) continue
			const stream = catalog.streams.find(
				entry =>
					entry.stream.name === selected.stream_name &&
					(entry.stream.namespace ?? "") === namespace,
			)
			if (!stream) continue // Retain unavailable tables exactly; runtime validates them.
			const error = getCOWEligibilityError({
				stream,
				selected,
				sourceType,
				destinationType,
				...connections,
			})
			if (error) return `${name} ${error}`
		}
	}
	return null
}

export const withDefaultCopyOnWrite = ({
	catalog,
	existingJob,
	sourceType,
	destinationType,
	...connections
}: COWConnectionSettings & {
	catalog: StreamsDataStructure
	existingJob: boolean
	sourceType?: string
	destinationType?: string
}): StreamsDataStructure => {
	if (existingJob) return catalog
	return {
		...catalog,
		selected_streams: Object.fromEntries(
			Object.entries(catalog.selected_streams).map(([namespace, entries]) => [
				namespace,
				entries.map(selected => {
					if (selected.merge_strategy !== undefined || selected.append_mode)
						return selected
					const stream = catalog.streams.find(
						entry =>
							entry.stream.name === selected.stream_name &&
							(entry.stream.namespace ?? "") === namespace,
					)
					if (
						!stream ||
						getCOWEligibilityError({
							stream,
							selected,
							sourceType,
							destinationType,
							...connections,
						})
					)
						return selected
					return { ...selected, merge_strategy: "copy-on-write" }
				}),
			]),
		),
	}
}
