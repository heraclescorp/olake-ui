import type {
	COWOptions,
	SelectedStream,
	StreamData,
	StreamsDataStructure,
} from "@/modules/ingestion/common/types"

import { normalizeSortColumnName } from "./sortOrder"

export const COW_MAX_FLUSH_INTERVAL_SECONDS = 9223372036

export const COW_DEFAULTS = {
	flush_interval_seconds: 300,
	max_batch_rows: 100000,
	max_batch_bytes: 67108864,
	rewrite_concurrency: 2,
	materialize_existing_deletes: false,
} as const

export const COW_CAPABILITY_WARNING =
	"This source build did not advertise supports_iceberg_copy_on_write. The saved choice is preserved, not silently applied as merge-on-read. Update the connector and refresh discovery, or explicitly select Merge-on-read to disable COW."

const COW_KEY_TYPES: Record<string, true> = {
	boolean: true,
	integer_small: true,
	integer: true,
	string: true,
	binary: true,
	timestamp: true,
	timestamp_milli: true,
	timestamp_micro: true,
	timestamp_nano: true,
}

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

export const validateCOWOptions = (options?: COWOptions): string | null => {
	if (options === undefined) return null
	if (!options || typeof options !== "object" || Array.isArray(options)) {
		return "cow_options must be an object."
	}
	for (const key of [
		"flush_interval_seconds",
		"max_batch_rows",
		"max_batch_bytes",
		"rewrite_concurrency",
	] as const) {
		const value = options[key]
		if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
			return `${key} must be a positive safe integer.`
		}
		if (
			key === "flush_interval_seconds" &&
			value !== undefined &&
			value > COW_MAX_FLUSH_INTERVAL_SECONDS
		) {
			return `flush_interval_seconds must not exceed ${COW_MAX_FLUSH_INTERVAL_SECONDS}.`
		}
	}
	if (
		options.materialize_existing_deletes !== undefined &&
		typeof options.materialize_existing_deletes !== "boolean"
	) {
		return "materialize_existing_deletes must be a boolean."
	}
	return null
}

// Discovery is build evidence, not permission to bypass the runtime's actual
// source/plugin, key-schema, ownership and native-table shape validation.
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
		return "Copy-on-write requires PostgreSQL CDC with complete pgoutput transactions."
	}
	if (sourceConfig !== undefined) {
		const config = configObject(sourceConfig)
		const update = configObject(config?.update_method)
		if (!update?.replication_slot || !update.publication) {
			return "Copy-on-write requires an existing pgoutput replication slot and publication. Configure the source without resetting its slot or checkpoint."
		}
		if (
			update.toast_value_source !== undefined &&
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
	const properties = stream.stream.type_schema?.properties ?? {}
	for (const key of keys) {
		const schema = properties[key]
		const types = schema?.type ?? []
		if (
			!schema ||
			schema.olake_column ||
			types.length !== 1 ||
			(COW_KEY_TYPES[types[0]] !== true &&
				!/^fixed_binary\([1-9]\d*\)$/.test(types[0]))
		) {
			return `Primary key ${key} requires one supported, nonnull exact-filter source type for copy-on-write.`
		}
		const names = Object.keys(properties).filter(name =>
			selected.use_source_column_names
				? name === key
				: normalizeSortColumnName(name) === normalizeSortColumnName(key),
		)
		if (names.length !== 1)
			return `Primary key ${key} has an ambiguous normalized destination name.`
		if (
			selected.selected_columns?.columns.length &&
			!selected.selected_columns.columns.some(column =>
				selected.use_source_column_names
					? column === key
					: normalizeSortColumnName(column) === normalizeSortColumnName(key),
			)
		) {
			return `Select source primary key column ${key} before enabling copy-on-write.`
		}
	}
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
			const optionError = validateCOWOptions(selected.cow_options)
			if (optionError) return `${name} ${optionError}`
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
