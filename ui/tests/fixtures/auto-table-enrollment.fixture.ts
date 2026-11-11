import type { Page } from "@playwright/test"

import { UpsertType } from "../../src/modules/ingestion/common/types"
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
}

export interface EnrollmentFixtureRequests {
	job: Job
	updates: JobBase[]
	writes: string[]
	jobReads: number
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
	const streams = [
		["public", "orders"],
		["public", "previously_enrolled"],
		["public", "disabled_table"],
		["public", "known_unselected"],
		["archive", "unselected_history"],
	].map(([namespace, name]) => ({
		stream: {
			name,
			namespace,
			json_schema: { properties: { id: { type: "integer" } } },
			sync_mode: "cdc",
			destination_database: `saved:${namespace}`,
			destination_table: `${name}_saved`,
			normalization: false,
			append_mode: true,
			update_type: "eq",
			available_update_types: ["eq", "pos", "dv"],
		},
	}))
	const selected_streams = {
		public: [
			{
				stream_name: "orders",
				sync_mode: "cdc",
				normalization: false,
				append_mode: true,
				update_type: "eq",
				selected_columns: {
					columns: ["id"],
					sync_new_columns: false,
				},
				destination_database: "existing:override",
				destination_table: "orders_selected",
			},
			{
				stream_name: "previously_enrolled",
				sync_mode: "cdc",
				normalization: true,
				append_mode: false,
				update_type: "pos",
				selected_columns: { columns: ["id"], sync_new_columns: true },
				destination_database: "warehouse:public_new",
				destination_table: "previously_enrolled_saved",
			},
			{ stream_name: "disabled_table", disabled: true },
		],
	}
	const job: Job = {
		id: 42,
		name: "Fixture auto-enrollment job",
		source: {
			id: 11,
			name: "Fixture PostgreSQL",
			type: "postgres",
			version: "v0.8.2",
			config: JSON.stringify({ host: "fixture.invalid", database: "source" }),
		},
		destination: {
			id: 22,
			name: "Fixture Iceberg",
			type: "iceberg",
			version: "v0.8.2",
			config: JSON.stringify({ catalog_type: "rest", warehouse: "fixture" }),
		},
		catalog_revision: ENROLLMENT_CATALOG_REVISION,
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
			target_query_engines: ["trino"],
			...(options.policy ? { auto_table_enrollment: options.policy } : {}),
		},
		...(options.format === "split"
			? {
					available_streams_config: JSON.stringify({ streams }),
					selected_streams_config: JSON.stringify({ selected_streams }),
				}
			: { streams_config: JSON.stringify({ streams, selected_streams }) }),
	}
	let currentJob = job
	const requests: EnrollmentFixtureRequests = {
		job,
		updates: [],
		writes: [],
		jobReads: 0,
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
			await route.fulfill({
				status: 202,
				json: { data: { operation_id: "fixture-enrollment-spec" } },
			})
			return
		}
		if (request.method() !== "GET") {
			requests.writes.push(`${request.method()} ${path}`)
			if (path.endsWith("/jobs/42") && request.method() === "PUT") {
				const update = request.postDataJSON() as JobBase
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
			await route.fulfill({
				status: 405,
				json: { message: "Fixture blocks every other write" },
			})
			return
		}
		let data: unknown = []
		if (path.endsWith("/jobs/42")) {
			requests.jobReads += 1
			data = currentJob
		} else if (path.endsWith("/jobs/42/clear-destination")) {
			data = { running: false }
		} else if (path.endsWith("/jobs")) {
			data = [currentJob]
		} else if (path.endsWith("/releases")) {
			data = null
		} else if (path.endsWith("/opt/status")) {
			data = { enabled: false }
		} else if (path.endsWith("/operations/fixture-enrollment-spec")) {
			data = { status: "succeeded" }
		} else if (path.endsWith("/operations/fixture-enrollment-spec/result")) {
			data = {
				spec: {
					query_engines: { engines: [{ engine: "trino", label: "Trino" }] },
				},
			}
		}
		await route.fulfill({ json: { message: "Fixture", data } })
	})
	return requests
}
