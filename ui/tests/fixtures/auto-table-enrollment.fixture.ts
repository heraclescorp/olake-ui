import type { Page } from "@playwright/test"

import { UpsertType } from "../../src/modules/ingestion/common/types"
import type {
	SelectedStreams,
	SortField,
} from "../../src/modules/ingestion/common/types"
import type {
	AutoTableEnrollmentPolicy,
	Job,
	JobBase,
} from "../../src/modules/ingestion/features/jobs/types/jobTypes"
import { JobType } from "../../src/modules/ingestion/features/jobs/types/jobTypes"

export type EnrollmentCatalogFormat = "legacy" | "split"

interface EnrollmentFixtureOptions {
	format?: EnrollmentCatalogFormat
	policy?: AutoTableEnrollmentPolicy
	rejectStale?: boolean
	invalidCatalog?: boolean
	omitRevision?: boolean
	sourceType?: string
	routing?: "dynamic" | "custom" | "conflicting" | "available-conflicting"
	staleUpdateTypes?: UpsertType[] | null
	refreshedUpdateTypes?: UpsertType[]
	bulkAllDV?: boolean
	capabilityState?: "error" | "loading"
	readerSupports?: UpsertType[]
	writableTypes?: UpsertType[]
	targetQueryEngines?: string[]
	sparseSelection?: boolean
	sparseDestinationDatabase?: string
	sparseNamespace?: string
	sparsePolicy?: boolean
	sortScenario?: boolean
	defaultSortOrder?: SortField[]
	sortOrder?: SortField[]
	destinationType?: string
	unsupportedSortConnector?: boolean
	excludeCreatedAt?: boolean
	malformedSortField?: "direction" | "null-order"
	normalizedSortNames?: boolean
	preserveSortSourceNames?: boolean
	ambiguousSortNames?: boolean
	refreshedSortSupport?: boolean
}

export interface EnrollmentFixtureRequests {
	job: Job
	updates: JobBase[]
	creations: JobBase[]
	discoveries: Record<string, unknown>[]
	resyncs: unknown[]
	writes: string[]
	jobReads: number
	specRequests: number
	specBodies: Record<string, unknown>[]
	releaseCapabilities: () => void
}

// This is a fetched opaque token, not a client-side revision implementation.
export const ENROLLMENT_CATALOG_REVISION = "a1".repeat(32)
export const ENROLLMENT_STALE_MESSAGE =
	"Job catalog changed since it was loaded. Refresh the job before saving."

export const savedEnrollmentPolicy = {
	enabled: true,
	schemas: [
		{ namespace: "public", destination_database: "warehouse:public_new" },
	],
	defaults: {
		normalization: true,
		append_mode: false,
		update_type: UpsertType.POSITIONAL,
		sync_new_columns: true,
	},
} satisfies AutoTableEnrollmentPolicy

export const installEnrollmentFixture = async (
	page: Page,
	options: EnrollmentFixtureOptions = {},
): Promise<EnrollmentFixtureRequests> => {
	const selectedDatabase =
		options.routing === "custom" ? "custom_warehouse" : "warehouse:public_new"
	const streams = [
		["public", "orders"],
		["public", "previously_enrolled"],
		["public", "disabled_table"],
		["public", "known_unselected"],
		["archive", "unselected_history"],
		...(options.sortScenario ? [["public", "excluded_time"]] : []),
		...(options.sparseSelection
			? [[options.sparseNamespace ?? "public", "sparse_saved"]]
			: []),
		...(options.routing === "available-conflicting"
			? [["archive", "other_history"]]
			: []),
	].map(([namespace, name]) => ({
		stream: {
			name,
			namespace,
			json_schema: { properties: { id: { type: "integer" } } },
			type_schema: {
				properties:
					options.sortScenario && name !== "unselected_history"
						? {
								id: { type: ["integer"] },
								[options.normalizedSortNames && name === "orders"
									? "Created At"
									: "created_at"]: {
									type: [
										name === "previously_enrolled"
											? "string"
											: "timestamp_micro",
										"null",
									],
								},
								...(options.ambiguousSortNames && name === "orders"
									? { "Created-At": { type: ["timestamp_micro"] } }
									: {}),
								priority: { type: ["integer"] },
								payload: { type: ["object"] },
								_olake_sort_fixture: { type: ["integer"], olake_column: true },
							}
						: { id: { type: ["integer"] } },
			},
			supported_sync_modes: ["cdc", "full_refresh"],
			supports_iceberg_sort_order:
				!!options.sortScenario && !options.unsupportedSortConnector,
			sync_mode: "cdc",
			destination_database:
				name === "sparse_saved"
					? (options.sparseDestinationDatabase ?? selectedDatabase)
					: name === "other_history"
						? "conflict:archive"
						: `saved:${namespace}`,
			destination_table: `${name}_saved`,
			default_stream_properties: {
				normalization: !!options.sortScenario,
				append_mode: true,
				update_type: "eq",
			},
			available_update_types:
				options.staleUpdateTypes === undefined
					? ["eq", "pos", "dv"]
					: options.staleUpdateTypes,
		},
	}))
	const selected_streams: Record<string, Record<string, unknown>[]> = {
		public: [
			{
				stream_name: "orders",
				sync_mode: "cdc",
				normalization: false,
				append_mode: true,
				update_type: "eq",
				selected_columns: { columns: ["id"], sync_new_columns: false },
				destination_database: selectedDatabase,
				destination_table: "orders_selected",
			},
			{
				stream_name: "previously_enrolled",
				sync_mode: "cdc",
				normalization: true,
				append_mode: false,
				update_type: "pos",
				selected_columns: { columns: ["id"], sync_new_columns: true },
				destination_database:
					options.routing === "conflicting"
						? "conflicting:public"
						: selectedDatabase,
				destination_table: "previously_enrolled_saved",
			},
			{ stream_name: "disabled_table", disabled: true },
		],
	}
	if (options.sortScenario) {
		for (const { stream } of streams) {
			if (stream.namespace === "public") {
				stream.destination_database = selectedDatabase
			}
		}
		selected_streams.public[0] = {
			...selected_streams.public[0],
			normalization: true,
			selected_columns: {
				columns: options.excludeCreatedAt
					? ["id", "priority", "payload"]
					: [
							"id",
							options.normalizedSortNames && options.preserveSortSourceNames
								? "Created At"
								: "created_at",
							"priority",
							"payload",
							...(options.ambiguousSortNames ? ["Created-At"] : []),
						],
				sync_new_columns: false,
			},
			...(options.preserveSortSourceNames && { use_source_column_names: true }),
			future_extension: { kept: true },
			...(options.sortOrder !== undefined && { sort_order: options.sortOrder }),
		}
		if (options.malformedSortField) {
			const malformedFields = {
				direction: { direction: "sideways" },
				"null-order": { null_order: "middle" },
			}
			selected_streams.public[0].sort_order = [
				{
					column: "id",
					direction: "asc",
					null_order: "nulls-last",
					...malformedFields[options.malformedSortField],
				},
			]
		}
		selected_streams.public[1].selected_columns = {
			columns: ["id", "created_at", "priority"],
			sync_new_columns: true,
		}
		selected_streams.public.push({
			stream_name: "excluded_time",
			normalization: true,
			selected_columns: { columns: ["id"], sync_new_columns: false },
			destination_database: selectedDatabase,
		})
		selected_streams.archive = [
			{ stream_name: "unselected_history", normalization: true },
		]
		selected_streams.unavailable = [
			{
				stream_name: "missing_from_discovery",
				sort_order: [],
				future_extension: { kept: true },
			},
			{
				stream_name: "disabled_missing",
				disabled: true,
				sort_order: [],
				future_extension: { kept: true },
			},
		]
	}
	const sortMetadata = options.sortScenario
		? {
				default_sort_order: options.defaultSortOrder,
				selected_extension: { kept: true },
				catalogMetadata: { fixture_extension: "kept" },
			}
		: {}
	const availableMetadata = options.sortScenario
		? { available_extension: { kept: true } }
		: {}
	if (options.bulkAllDV) {
		for (const entry of selected_streams.public.filter(e => !e.disabled)) {
			entry.append_mode = false
			entry.update_type = "dv"
		}
	}
	if (options.sparseSelection) {
		const namespace = options.sparseNamespace ?? "public"
		selected_streams[namespace] ??= []
		selected_streams[namespace].push({ stream_name: "sparse_saved" })
	}
	if (options.routing === "available-conflicting") {
		selected_streams.public[0].destination_database = undefined
		selected_streams.public[1].destination_database = undefined
	}
	const job: Job = {
		id: 42,
		name: "Fixture auto-enrollment job",
		source: {
			id: 11,
			name: "Fixture PostgreSQL",
			type: options.sourceType ?? "postgres",
			version: "v0.8.2",
			config: JSON.stringify({ host: "fixture.invalid", database: "source" }),
		},
		destination: {
			id: 22,
			name: "Fixture Iceberg",
			type: options.destinationType ?? "iceberg",
			version: "v0.8.2",
			config: JSON.stringify({ catalog_type: "rest", warehouse: "fixture" }),
		},
		catalog_revision: options.omitRevision
			? undefined
			: ENROLLMENT_CATALOG_REVISION,
		frequency: "* * * * *",
		last_run_type: JobType.Sync,
		last_run_state: "completed",
		last_run_time: "2026-11-01T00:00:00Z",
		created_at: "2026-11-01T00:00:00Z",
		updated_at: "2026-11-01T00:00:00Z",
		created_by: "fixture",
		updated_by: "fixture",
		activate: true,
		advanced_settings: {
			max_discover_threads: 7,
			index_required: true,
			target_query_engines: options.targetQueryEngines ?? ["trino"],
			...(options.policy ? { auto_table_enrollment: options.policy } : {}),
		},
		...(options.format === "split"
			? {
					available_streams_config: options.invalidCatalog
						? "not json"
						: JSON.stringify({ ...availableMetadata, streams }),
					selected_streams_config: JSON.stringify({
						...sortMetadata,
						selected_streams,
					}),
				}
			: {
					streams_config: options.invalidCatalog
						? "not json"
						: JSON.stringify({ ...sortMetadata, streams, selected_streams }),
				}),
	}
	let currentJob = job
	let discoveryIsNew = false
	let releaseCapabilities = (): void => {}
	const capabilityGate = new Promise<void>(resolve => {
		releaseCapabilities = resolve
	})
	const requests: EnrollmentFixtureRequests = {
		job,
		updates: [],
		creations: [],
		discoveries: [],
		resyncs: [],
		writes: [],
		jobReads: 0,
		specRequests: 0,
		specBodies: [],
		releaseCapabilities: (): void => releaseCapabilities(),
	}
	await page.addInitScript((): void => {
		localStorage.setItem("token", "fixture-authenticated")
		localStorage.setItem(
			"auth-storage",
			JSON.stringify({ state: { isAuthenticated: true }, version: 0 }),
		)
	})
	// Never intercept document/assets: this wildcard is intentionally API-only.
	await page.route("**/api/v1/**", async route => {
		const request = route.request()
		const path = new URL(request.url()).pathname
		if (path.endsWith("/sources/spec") && request.method() === "POST") {
			// Spec is a read-only operation despite its submission verb.
			requests.specRequests += 1
			requests.specBodies.push(request.postDataJSON())
			if (options.capabilityState === "loading") {
				await capabilityGate
			}
			if (options.capabilityState === "error") {
				await route.fulfill({
					status: 503,
					json: { message: "Fixture capability failure" },
				})
			} else {
				await route.fulfill({
					status: 202,
					json: { data: { operation_id: "fixture-enrollment-spec" } },
				})
			}
			return
		}
		if (request.method() === "POST" && path.endsWith("/check-unique")) {
			await route.fulfill({ json: { data: { unique: true } } })
			return
		}
		if (
			request.method() === "POST" &&
			(path.endsWith("/sources/test") || path.endsWith("/destinations/test"))
		) {
			await route.fulfill({
				status: 202,
				json: { data: { operation_id: "fixture-test" } },
			})
			return
		}
		if (request.method() === "POST" && path.endsWith("/sources/streams")) {
			const discovery: Record<string, unknown> = request.postDataJSON()
			requests.discoveries.push(discovery)
			discoveryIsNew = discovery.job_id === -1
			await route.fulfill({
				status: 202,
				json: { data: { operation_id: "fixture-discovery" } },
			})
			return
		}
		if (request.method() === "POST" && path.endsWith("/stream-difference")) {
			await route.fulfill({
				status: 202,
				json: { data: { operation_id: "fixture-difference" } },
			})
			return
		}
		if (request.method() !== "GET") {
			requests.writes.push(`${request.method()} ${path}`)
			if (path.endsWith("/jobs/42") && request.method() === "PUT") {
				const update: JobBase = request.postDataJSON()
				requests.updates.push(update)
				if (options.rejectStale) {
					await route.fulfill({
						status: 409,
						json: { message: ENROLLMENT_STALE_MESSAGE, data: null },
					})
					return
				}
				currentJob = { ...currentJob, ...update }
				await route.fulfill({
					json: { message: "Job updated", data: currentJob },
				})
				return
			}
			if (path.endsWith("/jobs") && request.method() === "POST") {
				requests.creations.push(request.postDataJSON())
				await route.fulfill({
					json: {
						message: "Job created",
						data: { ...job, ...requests.creations[0] },
					},
				})
				return
			}
			if (
				path.endsWith("/jobs/42/resync-table") &&
				request.method() === "POST"
			) {
				requests.resyncs.push(request.postDataJSON())
				await route.fulfill({
					json: { message: "Table resync started", data: null },
				})
				return
			}
			await route.fulfill({
				status: 405,
				json: { message: "Fixture blocks every other write" },
			})
			return
		}
		let data: unknown = []
		if (path.endsWith("/jobs/42")) {
			requests.jobReads += 1
			data =
				options.sparsePolicy &&
				!currentJob.advanced_settings?.auto_table_enrollment
					? {
							...currentJob,
							advanced_settings: {
								...currentJob.advanced_settings,
								auto_table_enrollment: {
									defaults: savedEnrollmentPolicy.defaults,
								},
							},
						}
					: currentJob
		} else if (path.endsWith("/jobs/42/clear-destination")) {
			data = { running: false }
		} else if (path.endsWith("/jobs/42/reconcile")) {
			data = {
				status: "idle",
				can_reconcile: false,
				schedule_paused: false,
				message: "No retained execution owner.",
			}
		} else if (path.endsWith("/jobs")) {
			data = [currentJob]
		} else if (path.endsWith("/sources")) {
			data = [job.source]
		} else if (path.endsWith("/destinations")) {
			data = [job.destination]
		} else if (path.endsWith("/releases")) {
			data = null
		} else if (path.endsWith("/opt/status")) {
			data = { enabled: false }
		} else if (/\/operations\/[^/]+$/.test(path)) {
			data = { status: "succeeded" }
		} else if (path.endsWith("/operations/fixture-enrollment-spec/result")) {
			data = {
				spec: {
					query_engines: {
						engines: [
							{
								engine: "trino",
								label: "Trino",
								supports: options.readerSupports ?? ["eq", "pos", "dv"],
							},
							{
								engine: "equality-reader",
								label: "Equality reader",
								supports: ["eq"],
							},
							{
								engine: "dv-only-reader",
								label: "DV-only reader",
								supports: ["dv"],
							},
						],
						writable_update_types: options.writableTypes ?? ["eq", "pos", "dv"],
					},
				},
			}
		} else if (path.endsWith("/operations/fixture-test/result")) {
			data = {
				connection_result: { status: "SUCCEEDED", message: "Fixture" },
				logs: [],
			}
		} else if (path.endsWith("/operations/fixture-difference/result")) {
			data = { difference_streams: { selected_streams: {} } }
		} else if (path.endsWith("/operations/fixture-discovery/result")) {
			const refreshedStreams =
				options.refreshedUpdateTypes && requests.discoveries.length > 1
					? streams.map(entry => ({
							...entry,
							stream: {
								...entry.stream,
								available_update_types: options.refreshedUpdateTypes,
							},
						}))
					: streams
			const discoveredStreams =
				options.refreshedSortSupport && requests.discoveries.length > 1
					? refreshedStreams.map(entry => ({
							...entry,
							stream: { ...entry.stream, supports_iceberg_sort_order: true },
						}))
					: refreshedStreams
			const savedSelection: SelectedStreams = JSON.parse(
				(options.format === "split"
					? currentJob.selected_streams_config
					: currentJob.streams_config) ?? "{}",
			)
			const discoveredSelection = discoveryIsNew
				? {
						public: [
							{
								stream_name: "orders",
								sync_mode: "cdc",
								normalization: false,
								append_mode: true,
								update_type: "eq",
								selected_columns: { columns: ["id"], sync_new_columns: false },
								destination_database: selectedDatabase,
							},
						],
					}
				: options.sortScenario
					? savedSelection.selected_streams
					: selected_streams
			data =
				options.format === "split"
					? {
							streams_config: null,
							available_streams: {
								...availableMetadata,
								streams: discoveredStreams,
							},
							selected_streams: {
								...(options.sortScenario && !discoveryIsNew
									? savedSelection
									: {}),
								selected_streams: discoveredSelection,
							},
						}
					: {
							streams_config: {
								...(options.sortScenario && !discoveryIsNew
									? savedSelection
									: {}),
								streams: discoveredStreams,
								selected_streams: discoveredSelection,
							},
							available_streams: null,
							selected_streams: null,
						}
		}
		await route.fulfill({ json: { message: "Fixture", data } })
	})
	return requests
}
