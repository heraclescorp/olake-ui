import type { Page } from "@playwright/test"

import type { Job } from "../../src/modules/ingestion/features/jobs/types/jobTypes"
import { JobType } from "../../src/modules/ingestion/features/jobs/types/jobTypes"
import { test, expect } from "../fixtures/base.fixture"

type CatalogFormat = "legacy" | "split"

interface ResyncFixture {
	format?: CatalogFormat
	paused?: boolean
	running?: boolean
	clearing?: boolean
	reject?: boolean
	invalidCatalog?: boolean
}

interface FixtureRequests {
	resyncBodies: unknown[]
	writes: string[]
	jobReads: number
}

const installResyncFixture = async (
	page: Page,
	options: ResyncFixture = {},
): Promise<FixtureRequests> => {
	const selected_streams = {
		public: [
			{
				stream_name: "orders",
				sync_mode: "cdc",
				selected_columns: { columns: ["id", "total"], sync_new_columns: false },
				destination_database:
					options.format === "split" ? "warehouse:override" : undefined,
				destination_table:
					options.format === "split" ? "orders_selected" : undefined,
			},
			{
				stream_name: "events",
				sync_mode: options.format === "split" ? "strict_cdc" : undefined,
			},
			{ stream_name: "disabled_table", disabled: true },
		],
		archive: [{ stream_name: "orders", sync_mode: "full_refresh" }],
	}
	const streams = ["orders", "events", "disabled_table", "unselected_table"]
		.map(name => ({
			stream: {
				name,
				namespace: "public",
				json_schema: {},
				sync_mode: name === "events" ? "strict_cdc" : "cdc",
				destination_database: "analytics:public",
				destination_table: `${name}_catalog`,
			},
		}))
		.concat([
			{
				stream: {
					name: "orders",
					namespace: "archive",
					json_schema: {},
					sync_mode: "full_refresh",
					destination_database: "analytics:archive",
					destination_table: "orders_archive",
				},
			},
		])
	const job: Job = {
		id: 42,
		name: "Fixture CDC job",
		source: {
			name: "Fixture PostgreSQL",
			type: "postgres",
			version: "v0.8.2",
			config: "{}",
		},
		destination: {
			name: "Fixture Iceberg",
			type: "iceberg",
			version: "v0.8.2",
			config: "{}",
		},
		frequency: "* * * * *",
		catalog_revision: "a1".repeat(32),
		last_run_type: JobType.Sync,
		last_run_state: options.running ? "running" : "completed",
		last_run_time: "2026-11-01T00:00:00Z",
		created_at: "2026-11-01T00:00:00Z",
		updated_at: "2026-11-01T00:00:00Z",
		created_by: "fixture",
		updated_by: "fixture",
		activate: !options.paused,
		advanced_settings: { max_discover_threads: 5 },
		...(options.format === "split"
			? {
					available_streams_config: JSON.stringify({ streams }),
					selected_streams_config: options.invalidCatalog
						? "not json"
						: JSON.stringify({ selected_streams }),
				}
			: {
					streams_config: options.invalidCatalog
						? "not json"
						: JSON.stringify({ streams, selected_streams }),
				}),
	}
	const requests: FixtureRequests = {
		resyncBodies: [],
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
	await page.route("**/api/v1/**", async route => {
		const request = route.request()
		const path = new URL(request.url()).pathname
		if (path.endsWith("/sources/spec") && request.method() === "POST") {
			await route.fulfill({
				status: 202,
				json: { data: { operation_id: "fixture-spec" } },
			})
			return
		}
		if (request.method() !== "GET") {
			requests.writes.push(`${request.method()} ${path}`)
			if (
				path.endsWith("/jobs/42/resync-table") &&
				request.method() === "POST"
			) {
				requests.resyncBodies.push(request.postDataJSON())
				await route.fulfill({
					status: options.reject ? 409 : 200,
					json: {
						message: options.reject
							? "Job is already running"
							: "Table resync started",
						data: null,
					},
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
			data = job
		} else if (path.endsWith("/jobs/42/clear-destination")) {
			data = { running: !!options.clearing }
		} else if (path.endsWith("/jobs")) {
			data = [job]
		} else if (path.endsWith("/releases")) {
			data = null
		} else if (path.endsWith("/opt/status")) {
			data = { enabled: false }
		} else if (path.endsWith("/operations/fixture-spec")) {
			data = { status: "succeeded" }
		} else if (path.endsWith("/operations/fixture-spec/result")) {
			data = {
				spec: {
					query_engines: { engines: [], writable_update_types: ["eq", "pos"] },
				},
			}
		}
		await route.fulfill({ json: { message: "Fixture", data } })
	})
	return requests
}

const chooseOrders = async (page: Page): Promise<void> => {
	const select = page.getByRole("combobox", { name: "Table to resync" })
	await select.fill("public.orders")
	await page.getByTitle("public.orders", { exact: true }).click()
}

test.describe("Table resync (intercepted APIs only)", () => {
	test.setTimeout(30_000)

	for (const format of ["legacy", "split"] satisfies CatalogFormat[]) {
		test(`${format}: searches saved tables, confirms scope, cancels safely, then submits exactly one table`, async ({
			page,
		}) => {
			const requests = await installResyncFixture(page, { format })
			await page.goto("/jobs/42/settings")
			const select = page.getByRole("combobox", { name: "Table to resync" })
			await select.click()
			await expect(
				page.getByTitle("public.unselected_table", { exact: true }),
			).toHaveCount(0)
			await expect(
				page.getByTitle("public.disabled_table", { exact: true }),
			).toHaveCount(0)
			await expect(
				page.getByTitle("archive.orders", { exact: true }),
			).toBeVisible()
			await expect(
				page.getByTitle("public.events (backfill unavailable)"),
			).toHaveClass(/ant-select-item-option-disabled/)
			await chooseOrders(page)
			await page
				.getByRole("button", { name: "Resync table", exact: true })
				.click()
			const dialog = page.getByRole("dialog", { name: "Confirm table resync" })
			await expect(dialog).toContainText("Fixture PostgreSQL")
			await expect(dialog).toContainText("Fixture Iceberg")
			await expect(dialog).toContainText("public.orders")
			await expect(dialog).toContainText(
				format === "split"
					? "warehouse_override/orders_selected"
					: "analytics_public/orders_catalog",
			)
			await expect(dialog).toContainText(
				"destination data, index, and backfill progress",
			)
			await expect(dialog).toContainText("global LSN / CDC position")
			await expect(dialog).toContainText("CDC for the whole job waits")
			await expect(dialog).toContainText("starts automatically")
			await expect(dialog).toContainText(
				"Other streams retain their state and follow their configured sync modes",
			)
			await expect(dialog).toContainText(
				"unfinished backfills or full-refresh streams may also run",
			)
			await expect(dialog).toContainText("CDC catch-up follows")
			await expect(dialog).not.toContainText("only this table is backfilled")
			await expect(dialog).toContainText(
				"Column-only historical backfill is unsupported",
			)
			await dialog.getByRole("button", { name: "Cancel", exact: true }).click()
			expect(requests.writes).toEqual([])
			await page
				.getByRole("button", { name: "Resync table", exact: true })
				.click()
			await dialog
				.getByRole("button", { name: "Clear and resync table", exact: true })
				.click()
			await expect(page).toHaveURL(/\/jobs\/42\/history$/)
			expect(requests.resyncBodies).toEqual([
				{ namespace: "public", stream_name: "orders" },
			])
			expect(requests.writes).toEqual([
				"POST /api/v1/project/123/jobs/42/resync-table",
			])
			await expect.poll(() => requests.jobReads).toBeGreaterThan(1)
		})
	}

	for (const scenario of [
		{
			name: "running",
			options: { running: true },
			reason: "Wait for the running sync",
		},
		{
			name: "paused",
			options: { paused: true },
			reason: "Resume this paused job",
		},
		{
			name: "clearing",
			options: { clearing: true },
			reason: "Wait for the running sync",
		},
		{
			name: "invalid catalog",
			options: { invalidCatalog: true },
			reason: "Saved table configuration could not be read",
		},
	]) {
		test(`blocks resync for ${scenario.name}`, async ({ page }) => {
			const requests = await installResyncFixture(page, scenario.options)
			await page.goto("/jobs/42/settings")
			await expect(
				page.getByRole("button", { name: "Resync table", exact: true }),
			).toBeDisabled()
			await expect(
				page.getByRole("status").filter({ hasText: scenario.reason }),
			).toHaveCount(1)
			expect(requests.writes).toEqual([])
		})
	}

	test("blocks unsaved settings and re-enables after discarding them", async ({
		page,
	}) => {
		const requests = await installResyncFixture(page)
		await page.goto("/jobs/42/settings")
		await chooseOrders(page)
		const button = page.getByRole("button", {
			name: "Resync table",
			exact: true,
		})
		await expect(button).toBeEnabled()
		await page.getByRole("spinbutton").fill("7")
		await expect(button).toBeDisabled()
		await expect(page.getByRole("status")).toContainText(
			"Save or discard unsaved settings",
		)
		await page.getByRole("spinbutton").fill("5")
		await expect(button).toBeEnabled()
		const frequency = page.getByRole("combobox").first()
		await frequency.focus()
		await frequency.press("ArrowDown")
		await page.getByTitle("Every Hour", { exact: true }).click()
		await expect(button).toBeDisabled()
		await expect(page.getByRole("status")).toContainText(
			"Save or discard unsaved settings",
		)
		expect(requests.writes).toEqual([])
	})

	test("shows backend rejection without navigating or issuing other operations", async ({
		page,
	}) => {
		const requests = await installResyncFixture(page, { reject: true })
		await page.goto("/jobs/42/settings")
		await chooseOrders(page)
		await page
			.getByRole("button", { name: "Resync table", exact: true })
			.click()
		await page
			.getByRole("button", { name: "Clear and resync table", exact: true })
			.click()
		await expect(
			page.getByText("Job is already running", { exact: true }),
		).toBeVisible()
		await expect(page).toHaveURL(/\/jobs\/42\/settings$/)
		await expect(
			page.getByRole("dialog", { name: "Confirm table resync" }),
		).toBeVisible()
		expect(requests.writes).toEqual([
			"POST /api/v1/project/123/jobs/42/resync-table",
		])
	})
})
