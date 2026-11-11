import { QueryEngineCatalog } from "@/common/types"
import {
	StreamsDataStructure,
	UpsertType,
} from "@/modules/ingestion/common/types"
import { getConnectorInLowerCase } from "@/modules/ingestion/common/utils"

import { NEW_TABLE_DEFAULTS } from "../constants"
import {
	AutoTableEnrollmentDefaults,
	AutoTableEnrollmentPolicy,
	AutoTableEnrollmentSchema,
	Job,
} from "../types"
import {
	configuredDestinationDatabase,
	destinationDatabaseFor,
	effectiveSelectedStreamsRouting,
} from "./destination-database"
import { isStreamsV2Job } from "./jobs"

export interface AutoTableEnrollmentDraft {
	enabled: boolean
	schemas: AutoTableEnrollmentSchema[]
	defaults: AutoTableEnrollmentDefaults
}

export interface EnrollmentCatalog {
	namespaces: string[]
	schemas: AutoTableEnrollmentSchema[]
	error?: string
}

export interface EnrollmentCapabilities {
	availableUpdateTypes: UpsertType[]
	error?: string
}

export type EnrollmentPolicyResult =
	| { valid: true; policy: AutoTableEnrollmentPolicy }
	| { valid: false; error: string }

export const isAutoTableEnrollmentSupported = (
	sourceType: string,
	destinationType: string,
): boolean =>
	getConnectorInLowerCase(sourceType) === "postgres" &&
	getConnectorInLowerCase(destinationType) === "iceberg"

export const getEnrollmentCapabilities = (
	catalog: QueryEngineCatalog | undefined,
	targetQueryEngines: string[],
): EnrollmentCapabilities => {
	if (
		!catalog ||
		!Array.isArray(catalog.engines) ||
		!Array.isArray(catalog.writable_update_types)
	) {
		return {
			availableUpdateTypes: [],
			error:
				"Delete-format capabilities are unavailable. Retry fetching query engines before saving automatic enrollment.",
		}
	}
	const readers = targetQueryEngines
		.map(engine => engine.trim().toLowerCase())
		.filter(Boolean)
		.map(engine =>
			catalog.engines.find(
				reader => reader.engine.trim().toLowerCase() === engine,
			),
		)
	if (readers.some(reader => !reader || !Array.isArray(reader.supports))) {
		return {
			availableUpdateTypes: [],
			error:
				"A target query engine is unavailable in the current source version. Update the target query engines before saving automatic enrollment.",
		}
	}
	return {
		availableUpdateTypes: Object.values(UpsertType).filter(
			type =>
				catalog.writable_update_types.includes(type) &&
				readers.every(reader => reader?.supports.includes(type)),
		),
	}
}

export const enrollmentDraftFromPolicy = (
	policy?: AutoTableEnrollmentPolicy,
	supported = false,
	availableUpdateTypes?: UpsertType[],
): AutoTableEnrollmentDraft => ({
	enabled: policy == null ? supported : policy.enabled === true,
	schemas: policy?.schemas?.map(schema => ({ ...schema })) ?? [],
	defaults: {
		...NEW_TABLE_DEFAULTS,
		update_type:
			availableUpdateTypes &&
			!availableUpdateTypes.includes(NEW_TABLE_DEFAULTS.update_type)
				? (availableUpdateTypes[0] ?? NEW_TABLE_DEFAULTS.update_type)
				: NEW_TABLE_DEFAULTS.update_type,
		...policy?.defaults,
	},
})

// Read the complete available catalog, not the selection: known but unselected
// tables are deliberately excluded from enrollment, and never become defaults.
export const enrollmentCatalogFromStreams = (
	catalog: StreamsDataStructure | null,
): EnrollmentCatalog => {
	if (!catalog || !Array.isArray(catalog.streams)) {
		return {
			namespaces: [],
			schemas: [],
			error:
				"The available catalog is unavailable. Refresh the table list before saving automatic enrollment.",
		}
	}
	const enabledSelections = effectiveSelectedStreamsRouting(catalog)
	const configured = configuredDestinationDatabase(enabledSelections)
	const namespaces = [
		...new Set(catalog.streams.map(entry => entry.stream.namespace ?? "")),
	].sort()
	const schemas: AutoTableEnrollmentSchema[] = []
	for (const namespace of namespaces) {
		const selectedDatabases = new Set(
			(enabledSelections[namespace] ?? []).map(
				stream => stream.destination_database?.trim() ?? "",
			),
		)
		const databases =
			selectedDatabases.size > 0
				? selectedDatabases
				: new Set(
						catalog.streams
							.filter(entry => entry.stream.namespace === namespace)
							.map(
								entry =>
									destinationDatabaseFor(entry.stream, configured)?.trim() ??
									"",
							),
					)
		if (!namespace.trim() || databases.size !== 1 || databases.has("")) {
			return {
				namespaces,
				schemas: [],
				error: `Cannot infer one destination database for source schema ${namespace || "(empty)"}. Resolve conflicting or missing stream routing before saving automatic enrollment.`,
			}
		}
		for (const destination_database of databases) {
			schemas.push({ namespace, destination_database })
		}
	}
	return { namespaces, schemas }
}

export const getEnrollmentCatalog = (job: Job): EnrollmentCatalog => {
	try {
		const available = JSON.parse(
			(isStreamsV2Job(job)
				? job.available_streams_config
				: job.streams_config) || "{}",
		)
		const selection = isStreamsV2Job(job)
			? JSON.parse(job.selected_streams_config || "{}")
			: available
		return enrollmentCatalogFromStreams({
			streams: available.streams,
			selected_streams: selection.selected_streams ?? {},
		})
	} catch {
		return {
			namespaces: [],
			schemas: [],
			error:
				"The saved available catalog could not be read. Refresh the job before saving automatic enrollment.",
		}
	}
}

export const buildEnrollmentPolicy = ({
	draft,
	supported,
	catalog,
	capabilities,
	catalogRevision,
	requireRevision = false,
}: {
	draft: AutoTableEnrollmentDraft
	supported: boolean
	catalog: EnrollmentCatalog
	capabilities: EnrollmentCapabilities
	catalogRevision?: string
	requireRevision?: boolean
}): EnrollmentPolicyResult => {
	if (requireRevision && !catalogRevision) {
		return {
			valid: false,
			error:
				"Refresh this job to obtain its catalog revision before enabling automatic table enrollment.",
		}
	}
	if (!draft.enabled) {
		return {
			valid: true,
			policy: {
				enabled: false,
				schemas: draft.schemas.map(schema => ({ ...schema })),
				defaults: { ...draft.defaults },
			},
		}
	}
	if (!supported) {
		return {
			valid: false,
			error:
				"Automatic table enrollment is supported only for PostgreSQL to Iceberg jobs.",
		}
	}
	if (catalog.error) {
		return { valid: false, error: catalog.error }
	}
	if (catalog.schemas.length === 0) {
		return {
			valid: false,
			error:
				"No source schemas are available. Refresh the table list before saving automatic enrollment.",
		}
	}
	if (capabilities.error) {
		return { valid: false, error: capabilities.error }
	}
	if (
		!draft.defaults.append_mode &&
		!capabilities.availableUpdateTypes.includes(draft.defaults.update_type)
	) {
		return {
			valid: false,
			error:
				"Choose a delete format supported by the current source version and target query engines.",
		}
	}
	return {
		valid: true,
		policy: {
			enabled: true,
			schemas: catalog.schemas.map(schema => ({ ...schema })),
			defaults: { ...draft.defaults },
		},
	}
}
