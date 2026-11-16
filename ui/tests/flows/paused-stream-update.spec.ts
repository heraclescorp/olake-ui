import { test, expect } from "@playwright/test"
import type { Page } from "@playwright/test"

import { UpsertType } from "../../src/modules/ingestion/common/types"
import { installEnrollmentFixture } from "../fixtures/auto-table-enrollment.fixture"
import type { EnrollmentCatalogFormat } from "../fixtures/auto-table-enrollment.fixture"

const openDeleteFormats = async (page: Page) => {
	const select = page.getByRole("combobox", {
		name: "Upsert Type",
		exact: true,
	})
	await expect(select).toBeVisible()
	await page
		.locator(".ant-select")
		.filter({ has: select })
		.locator(".ant-select-selector")
		.click()
	return page.locator(".ant-select-dropdown:visible")
}

for (const format of ["legacy", "split"] satisfies EnrollmentCatalogFormat[]) {
	test(`${format}: paused stream can refresh formats and save equality without resuming`, async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			format,
			staleUpdateTypes: [UpsertType.POSITIONAL, UpsertType.DELETION_VECTOR],
			refreshedUpdateTypes: [
				UpsertType.EQUALITY,
				UpsertType.POSITIONAL,
				UpsertType.DELETION_VECTOR,
			],
		})
		requests.job.activate = false
		requests.job.source.config = JSON.stringify({
			host: "fixture.invalid",
			update_method: {
				replication_slot: "existing_slot",
				toast_value_source: "source",
			},
		})
		await page.goto("/jobs")
		await page.getByRole("tab", { name: /Inactive/ }).click()
		await page
			.getByRole("tabpanel", { name: "Inactive jobs", exact: true })
			.getByTestId(`job-${requests.job.name}`)
			.click()
		await expect(
			page.getByRole("menuitem", { name: "Edit Streams" }),
		).not.toHaveAttribute("aria-disabled", "true")
		await page.getByRole("menuitem", { name: "Edit Streams" }).click()
		await expect(page).toHaveURL(/\/jobs\/42\/edit$/)
		await page
			.getByRole("button", { name: "previously_enrolled", exact: true })
			.click()
		let dropdown = await openDeleteFormats(page)
		await expect(
			dropdown.getByText("Equality Deletes", { exact: true }),
		).toHaveCount(0)
		await page.keyboard.press("Escape")
		const before = requests.discoveries.length
		await page
			.getByRole("button", { name: "Refresh streams", exact: true })
			.click()
		await expect(page.getByRole("dialog")).toContainText(
			"discards unsaved stream edits",
		)
		await page.getByRole("button", { name: "Cancel", exact: true }).click()
		await expect(page.getByRole("dialog")).toHaveCount(0)
		expect(requests.discoveries).toHaveLength(before)
		await page
			.getByRole("button", { name: "Refresh streams", exact: true })
			.click()
		await page
			.getByRole("dialog")
			.getByRole("button", { name: "Refresh streams", exact: true })
			.click()
		await expect.poll(() => requests.discoveries.length).toBe(before + 1)
		expect(requests.updates).toHaveLength(0)
		expect(requests.discoveries.at(-1)).toMatchObject({
			job_id: 42,
			target_query_engines: ["trino"],
		})
		expect(
			JSON.parse(requests.discoveries.at(-1)!.config as string).update_method,
		).toMatchObject({
			toast_value_source: "source",
			replication_slot: "existing_slot",
		})
		await page
			.getByRole("button", { name: "previously_enrolled", exact: true })
			.click()
		dropdown = await openDeleteFormats(page)
		await dropdown.getByText("Equality Deletes", { exact: true }).click()
		await page.getByRole("button", { name: "Finish", exact: true }).click()
		await expect.poll(() => requests.updates.length).toBe(1)
		const update = requests.updates[0]
		expect(update.activate).toBe(false)
		expect(update.catalog_revision).toBe(requests.job.catalog_revision)
		expect(update.source).toEqual(requests.job.source)
		expect(update.destination).toEqual(requests.job.destination)
		const catalog = JSON.parse(
			(format === "split"
				? update.selected_streams_config
				: update.streams_config)!,
		)
		expect(
			catalog.selected_streams.public.find(
				(s: { stream_name: string }) => s.stream_name === "previously_enrolled",
			),
		).toMatchObject({
			update_type: "eq",
			append_mode: false,
			destination_table: "previously_enrolled_saved",
		})
		expect(requests.resyncs).toHaveLength(0)
		expect(
			requests.writes.every(
				write => write === "PUT /api/v1/project/123/jobs/42",
			),
		).toBe(true)
	})
}

test("running job cannot open Edit Streams from the menu", async ({ page }) => {
	const requests = await installEnrollmentFixture(page)
	requests.job.last_run_state = "running"
	await page.goto("/jobs")
	await page
		.getByRole("tabpanel", { name: "Active jobs", exact: true })
		.getByTestId(`job-${requests.job.name}`)
		.click()
	await expect(
		page.getByRole("menuitem", { name: "Edit Streams" }),
	).toHaveAttribute("aria-disabled", "true")
})

test("failed refresh cannot enable equality or save guessed capabilities", async ({
	page,
}) => {
	const requests = await installEnrollmentFixture(page, {
		format: "split",
		staleUpdateTypes: [UpsertType.POSITIONAL, UpsertType.DELETION_VECTOR],
	})
	await page.goto("/jobs/42/edit")
	await expect(
		page.getByRole("button", { name: "Refresh streams", exact: true }),
	).toBeEnabled()
	await page.route("**/sources/streams", route =>
		route.fulfill({ status: 503, json: { message: "Discovery unavailable" } }),
	)
	await page
		.getByRole("button", { name: "Refresh streams", exact: true })
		.click()
	await page
		.getByRole("dialog")
		.getByRole("button", { name: "Refresh streams", exact: true })
		.click()
	await expect(
		page.getByText("Discovery unavailable", { exact: true }),
	).toBeVisible()
	await expect(
		page.getByRole("button", { name: "Finish", exact: true }),
	).toBeDisabled()
	expect(requests.updates).toHaveLength(0)
})
