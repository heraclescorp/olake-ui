import { DESTINATION_INTERNAL_TYPES } from "@/modules/ingestion/common/constants"
import type {
	SelectedStream,
	SortField,
	StreamData,
	StreamsDataStructure,
} from "@/modules/ingestion/common/types"

export const CREATED_AT_SORT_ORDER: SortField[] = [
	{ column: "created_at", direction: "asc", null_order: "nulls-last" },
]

export const SORT_CREATION_WARNING =
	"Creation only: applies only when a genuinely new native Iceberg table is created. Existing table identity, data, schema, partition spec, sort order and properties stay unchanged. A new job targeting an existing table does not change its layout; use a distinct destination namespace for separate tables."

export const SORT_COMPACTION_WARNING =
	"This configures Iceberg sort metadata for S3 Tables background auto/sort compaction. It does not sort newly written Parquet batches, rewrite historical files immediately, trigger a resync or backfill, or migrate existing tables."

export const SORT_CAPABILITY_WARNING =
	"Sorting requires a matching sort-aware source connector build. Update the source connector and refresh streams to obtain Iceberg sort-order capability evidence; a version label alone is not sufficient. Saved policy is preserved, but an older connector may ignore it."

const SORTABLE_PRIMITIVES: Record<string, true> = {
	boolean: true,
	integer_small: true,
	integer: true,
	number_small: true,
	number: true,
	string: true,
	timestamp: true,
	timestamp_milli: true,
	timestamp_micro: true,
	timestamp_nano: true,
	binary: true,
}

const TEMPORAL_PRIMITIVES: Record<string, true> = {
	timestamp: true,
	timestamp_milli: true,
	timestamp_micro: true,
	timestamp_nano: true,
}

export interface SortColumnOption {
	value: string
	sourceColumn: string
	destinationColumn: string
	label: string
	disabled: boolean
	reason: string | null
	temporal: boolean
}

export const normalizeSortColumnName = (column: string): string =>
	column.toLowerCase().replace(/[^a-z0-9]/gu, "_")

export const getSortColumnOptions = ({
	stream,
	selected,
}: {
	stream: StreamData
	selected: SelectedStream
}): SortColumnOption[] => {
	const properties = Object.entries(stream.stream.type_schema?.properties ?? {})
	const destinationNames = new Map<string, number>()
	for (const [column] of properties) {
		const destination = selected.use_source_column_names
			? column
			: normalizeSortColumnName(column)
		destinationNames.set(
			destination,
			(destinationNames.get(destination) ?? 0) + 1,
		)
	}
	const options = properties.map(([column, schema]): SortColumnOption => {
		const types = [...new Set(schema.type.filter(type => type !== "null"))]
		const primitive = types[0] ?? ""
		const compatible =
			types.length === 1 &&
			(SORTABLE_PRIMITIVES[primitive] === true ||
				/^fixed_binary\([1-9]\d*\)$/.test(primitive))
		const destinationColumn = selected.use_source_column_names
			? column
			: normalizeSortColumnName(column)
		const selectedColumns = selected.selected_columns?.columns
		const included =
			!selectedColumns?.length ||
			selectedColumns.some(selectedColumn =>
				selected.use_source_column_names
					? selectedColumn === column
					: normalizeSortColumnName(selectedColumn) === destinationColumn,
			)
		let reason: string | null = null
		if (schema.olake_column) {
			reason = "OLake metadata column"
		} else if (!included) {
			reason = "Excluded from selected columns"
		} else if (!compatible) {
			reason = "Requires one supported primitive type"
		} else if ((destinationNames.get(destinationColumn) ?? 0) > 1) {
			reason = "Ambiguous normalized source column"
		}
		return {
			value: column,
			sourceColumn: column,
			destinationColumn,
			label: reason ? `${column} — ${reason}` : column,
			disabled: reason !== null,
			reason,
			temporal: types.length === 1 && TEMPORAL_PRIMITIVES[primitive] === true,
		}
	})
	if (!selected.use_source_column_names) {
		for (const field of selected.sort_order ?? []) {
			if (options.some(option => option.value === field.column)) {
				continue
			}
			const destination = normalizeSortColumnName(field.column)
			const matches = options.filter(
				option =>
					option.sourceColumn === option.value &&
					option.destinationColumn === destination,
			)
			if (matches.length === 1) {
				const source = matches[0]
				options.push({
					...source,
					value: field.column,
					label: `${field.column} (source: ${source.sourceColumn})${source.reason ? ` — ${source.reason}` : ""}`,
				})
			}
		}
	}
	return options
}

export const isNormalizedSortStream = ({
	stream,
	selected,
}: {
	stream: StreamData
	selected: SelectedStream
}): boolean =>
	selected.normalization ??
	stream.stream.default_stream_properties?.normalization ??
	false

export const validateExplicitSortOrder = ({
	stream,
	selected,
	destinationType,
}: {
	stream: StreamData
	selected: SelectedStream
	destinationType?: string
}): string | null => {
	const order = selected.sort_order
	if (!order?.length) {
		return null
	}
	if (destinationType !== DESTINATION_INTERNAL_TYPES.ICEBERG) {
		return "Custom sort order requires an Iceberg destination. Saved settings are preserved."
	}
	if (stream.stream.supports_iceberg_sort_order !== true) {
		return SORT_CAPABILITY_WARNING
	}
	if (!isNormalizedSortStream({ stream, selected })) {
		return "Custom sort order requires normalization. Enable normalization or choose Inherit / Explicitly unsorted."
	}
	const columns = getSortColumnOptions({ stream, selected })
	const seen = new Set<string>()
	for (const field of order) {
		const column = columns.find(option => option.value === field.column)
		const identity = column?.destinationColumn ?? field.column
		if (seen.has(identity)) {
			return `Duplicate sort column: ${field.column}.`
		}
		seen.add(identity)
		if (!column) {
			return `Sort column ${field.column || "(empty)"} is absent from the discovered schema.`
		}
		if (column.reason) {
			return `Sort column ${field.column}: ${column.reason}.`
		}
		if (field.direction !== "asc" && field.direction !== "desc") {
			return `Sort column ${field.column}: direction must be asc or desc.`
		}
		if (
			field.null_order !== "nulls-first" &&
			field.null_order !== "nulls-last"
		) {
			return `Sort column ${field.column}: null ordering must be nulls-first or nulls-last.`
		}
	}
	return null
}

export interface InheritedSortEligibility {
	eligible: boolean
	description: string
}

export const getInheritedSortEligibility = ({
	stream,
	selected,
	defaultOrder,
}: {
	stream: StreamData
	selected: SelectedStream
	defaultOrder?: SortField[]
}): InheritedSortEligibility => {
	if (!defaultOrder?.length) {
		return {
			eligible: false,
			description: "Inherits: UNSORTED (catalog default is off).",
		}
	}
	if (stream.stream.supports_iceberg_sort_order !== true) {
		return { eligible: false, description: SORT_CAPABILITY_WARNING }
	}
	if (!isNormalizedSortStream({ stream, selected })) {
		return {
			eligible: false,
			description:
				"Default skipped: normalization is disabled. Resolves to UNSORTED.",
		}
	}
	const isPreset =
		defaultOrder.length === 1 &&
		defaultOrder[0].column === "created_at" &&
		defaultOrder[0].direction === "asc" &&
		defaultOrder[0].null_order === "nulls-last"
	if (!isPreset) {
		return {
			eligible: false,
			description: `Inherits saved catalog sort policy: ${defaultOrder.map(field => `${field.column} ${field.direction.toUpperCase()} ${field.null_order}`).join(", ")}. This UI configures only the created_at preset; the saved policy is preserved.`,
		}
	}
	const options = getSortColumnOptions({ stream, selected })
	const exact = options.find(option => option.value === "created_at")
	const normalizedMatches = selected.use_source_column_names
		? []
		: options.filter(option => option.destinationColumn === "created_at")
	const column =
		exact ?? (normalizedMatches.length === 1 ? normalizedMatches[0] : undefined)
	if (!column) {
		return {
			eligible: false,
			description:
				"Default skipped: created_at is missing or ambiguous. Resolves to UNSORTED.",
		}
	}
	if (column.reason) {
		return {
			eligible: false,
			description: `Default skipped: ${column.value} — ${column.reason}. Resolves to UNSORTED.`,
		}
	}
	if (!column.temporal) {
		return {
			eligible: false,
			description: `Default skipped: ${column.value} is not a supported temporal primitive. Resolves to UNSORTED.`,
		}
	}
	return {
		eligible: true,
		description: `Eligible: inherits ${column.value} ASC NULLS LAST for new tables.`,
	}
}

export const validateCatalogSortOrders = (
	catalog: StreamsDataStructure,
	destinationType?: string,
): string | null => {
	for (const { stream } of catalog.streams) {
		const selected = catalog.selected_streams[stream.namespace ?? ""]?.find(
			entry => entry.stream_name === stream.name,
		)
		if (!selected || selected.disabled) {
			continue
		}
		if (
			catalog.default_sort_order?.length &&
			selected.sort_order === undefined &&
			destinationType === DESTINATION_INTERNAL_TYPES.ICEBERG &&
			isNormalizedSortStream({ stream: { stream }, selected }) &&
			stream.supports_iceberg_sort_order !== true
		) {
			return SORT_CAPABILITY_WARNING
		}
		const error = validateExplicitSortOrder({
			stream: { stream },
			selected,
			destinationType,
		})
		if (error) {
			return `[${stream.namespace ? `${stream.namespace}.` : ""}${stream.name}] ${error}`
		}
	}
	return null
}

export interface BulkSortConfiguration {
	columns: SortColumnOption[]
	disabled: boolean
	error: string | null
	disabledReason?: string
}

export const getBulkSortConfiguration = ({
	catalog,
	streams,
	order,
	normalization,
	destinationType,
}: {
	catalog: StreamsDataStructure | null
	streams: { streamName: string; namespace: string }[]
	order: SortField[] | undefined
	normalization: boolean | undefined
	destinationType?: string
}): BulkSortConfiguration => {
	const rows = streams.flatMap(id => {
		const stream = catalog?.streams.find(
			entry =>
				entry.stream.name === id.streamName &&
				(entry.stream.namespace ?? "") === id.namespace,
		)
		const selected = catalog?.selected_streams[id.namespace]?.find(
			entry => entry.stream_name === id.streamName,
		)
		if (!stream || !selected) {
			return []
		}
		return [
			{
				stream,
				selected: {
					...selected,
					...(normalization !== undefined && { normalization }),
					sort_order: order,
				},
			},
		]
	})
	const first = rows[0]
	if (!first) {
		return { columns: [], disabled: true, error: null }
	}
	const options = rows.map(row => getSortColumnOptions(row))
	const columns = options[0].filter(column =>
		options.every(choices =>
			choices.some(choice => choice.value === column.value && !choice.disabled),
		),
	)
	let error: string | null = null
	for (const row of rows) {
		const validationError = validateExplicitSortOrder({
			...row,
			destinationType,
		})
		if (validationError) {
			error = `[${row.stream.stream.name}] ${validationError}`
			break
		}
	}
	const missingCapability = rows.some(
		row => row.stream.stream.supports_iceberg_sort_order !== true,
	)
	const missingNormalization = rows.some(row => !isNormalizedSortStream(row))
	return {
		columns,
		disabled: missingCapability || missingNormalization,
		disabledReason: missingCapability
			? SORT_CAPABILITY_WARNING
			: missingNormalization
				? "Enable normalization for every selected table to configure sorting."
				: undefined,
		error,
	}
}
