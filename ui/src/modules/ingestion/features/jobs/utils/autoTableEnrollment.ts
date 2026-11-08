import {
	StreamsDataStructure,
	UpsertType,
} from "@/modules/ingestion/common/types"
import { getConnectorInLowerCase } from "@/modules/ingestion/common/utils"

import { DEFAULT_AVAILABLE_UPDATE_TYPES } from "../constants"
import {
	AutoTableEnrollmentDefaults,
	AutoTableEnrollmentPolicy,
	AutoTableEnrollmentSchema,
	Job,
} from "../types"
import { isStreamsV2Job } from "./jobs"

export interface AutoTableEnrollmentDraft {
	enabled: boolean
	schemas: AutoTableEnrollmentSchema[]
	defaults: Partial<AutoTableEnrollmentDefaults>
}

export interface EnrollmentCatalog {
	namespaces: string[]
	availableUpdateTypes: UpsertType[]
	error?: string
}

export type EnrollmentPolicyResult =
	| { valid: true; policy: AutoTableEnrollmentPolicy }
	| { valid: false; error: string }

export const isAutoTableEnrollmentSupported = (job: Job): boolean =>
	getConnectorInLowerCase(job.source.type) === "postgres" &&
	getConnectorInLowerCase(job.destination.type) === "iceberg"

export const enrollmentDraftFromPolicy = (
	policy?: AutoTableEnrollmentPolicy,
): AutoTableEnrollmentDraft => ({
	enabled: policy?.enabled === true,
	schemas: policy?.schemas?.map(schema => ({ ...schema })) ?? [],
	defaults: { ...policy?.defaults },
})

// Read the complete available catalog, not the selection: known but unselected
// tables are deliberately excluded from enrollment, and never become defaults.
export const getEnrollmentCatalog = (job: Job): EnrollmentCatalog => {
	try {
		const catalog: StreamsDataStructure = JSON.parse(
			(isStreamsV2Job(job)
				? job.available_streams_config
				: job.streams_config) || "{}",
		)
		if (!Array.isArray(catalog.streams)) {
			return {
				namespaces: [],
				availableUpdateTypes: [],
				error:
					"The saved available catalog is unavailable. Refresh the job before approving schemas.",
			}
		}
		const namespaces = new Set<string>()
		let availableUpdateTypes = Object.values(UpsertType)
		for (const entry of catalog.streams) {
			if (entry.stream.namespace?.trim()) {
				namespaces.add(entry.stream.namespace)
			}
			const supported =
				entry.stream.available_update_types ?? DEFAULT_AVAILABLE_UPDATE_TYPES
			availableUpdateTypes = availableUpdateTypes.filter(type =>
				supported.includes(type),
			)
		}
		return {
			namespaces: [...namespaces].sort(),
			availableUpdateTypes,
		}
	} catch {
		return {
			namespaces: [],
			availableUpdateTypes: [],
			error:
				"The saved available catalog could not be read. Refresh the job before approving schemas.",
		}
	}
}

export const buildEnrollmentPolicy = ({
	draft,
	job,
	catalog,
}: {
	draft: AutoTableEnrollmentDraft
	job: Job
	catalog: EnrollmentCatalog
}): EnrollmentPolicyResult => {
	if (
		!draft.enabled &&
		Object.values(draft.defaults).every(value => value === undefined)
	) {
		return { valid: true, policy: { enabled: false } }
	}
	if (!isAutoTableEnrollmentSupported(job)) {
		return {
			valid: false,
			error:
				"Automatic table enrollment is supported only for PostgreSQL to Iceberg jobs.",
		}
	}
	if (draft.enabled && catalog.error) {
		return { valid: false, error: catalog.error }
	}
	if (draft.enabled && !job.catalog_revision) {
		return {
			valid: false,
			error:
				"Refresh this job to obtain its catalog revision before enabling automatic table enrollment.",
		}
	}
	if (draft.enabled && draft.schemas.length === 0) {
		return {
			valid: false,
			error: "Approve at least one existing source schema.",
		}
	}
	const namespaces = new Set<string>()
	for (const schema of draft.schemas) {
		if (!catalog.namespaces.includes(schema.namespace)) {
			return {
				valid: false,
				error: `Schema ${schema.namespace} is not in the saved available catalog. Refresh the job.`,
			}
		}
		if (namespaces.has(schema.namespace)) {
			return { valid: false, error: "Approved source schemas must be unique." }
		}
		namespaces.add(schema.namespace)
		if (!schema.destination_database.trim()) {
			return {
				valid: false,
				error: `Enter a destination database for ${schema.namespace}.`,
			}
		}
	}
	const { normalization, append_mode, update_type, sync_new_columns } =
		draft.defaults
	if (
		typeof normalization !== "boolean" ||
		typeof append_mode !== "boolean" ||
		update_type === undefined ||
		typeof sync_new_columns !== "boolean"
	) {
		return {
			valid: false,
			error:
				"Explicitly choose normalization, write mode, delete format, and new-column behavior for new tables.",
		}
	}
	if (!append_mode && !catalog.availableUpdateTypes.includes(update_type)) {
		return {
			valid: false,
			error:
				"Choose a delete format supported by the saved catalog and target query engines. Edit Streams to rediscover after changing query engines.",
		}
	}
	return {
		valid: true,
		policy: {
			enabled: draft.enabled,
			schemas: draft.schemas.map(schema => ({
				namespace: schema.namespace,
				destination_database: schema.destination_database.trim(),
			})),
			defaults: { normalization, append_mode, update_type, sync_new_columns },
		},
	}
}
