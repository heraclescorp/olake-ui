import type { Page } from "@playwright/test"

import type {
	AutoTableEnrollmentPolicy,
	JobBase,
} from "../../src/modules/ingestion/features/jobs/types/jobTypes"
import {
	ENROLLMENT_CATALOG_REVISION,
	ENROLLMENT_STALE_MESSAGE,
	installEnrollmentFixture,
	savedEnrollmentPolicy,
} from "../fixtures/auto-table-enrollment.fixture"
import type {
	EnrollmentCatalogFormat,
	EnrollmentFixtureRequests,
} from "../fixtures/auto-table-enrollment.fixture"
import { test, expect } from "../fixtures/base.fixture"

const DEFAULT_CHOICES = [
	["Normalization for new tables", "Enabled"],
	["Write mode for new tables", "Upsert"],
	["Delete format for new tables", "Positional Deletes"],
	["Automatically sync new columns", "Enabled"],
] as const

const EXPLICIT_DEFAULTS_ERROR =
	"Explicitly choose normalization, write mode, delete format, and new-column behavior for new tables."

const enrollmentPanel = (page: Page) =>
	page.getByRole("region", {
		name: "Automatically add new tables",
		exact: true,
	})

const chooseDefault = async (
	page: Page,
	label: string,
	choice: string,
): Promise<void> => {
	await enrollmentPanel(page)
		.locator(".ant-select")
		.filter({ has: page.getByRole("combobox", { name: label, exact: true }) })
		.click()
	await page
		.locator(".ant-select-dropdown:visible")
		.getByTitle(choice, { exact: true })
		.click()
}

const chooseExplicitDefaults = async (
	page: Page,
	omit?: string,
): Promise<void> => {
	for (const [label, choice] of DEFAULT_CHOICES) {
		if (label !== omit) await chooseDefault(page, label, choice)
	}
}

const approveSchema = async (
	page: Page,
	namespace: string,
	destinationDatabase: string,
): Promise<void> => {
	const panel = enrollmentPanel(page)
	await panel
		.getByRole("checkbox", { name: `Approve ${namespace}`, exact: true })
		.check()
	await panel
		.getByRole("textbox", {
			name: `Destination database for ${namespace}`,
			exact: true,
		})
		.fill(destinationDatabase)
}

const enableEnrollment = async (page: Page): Promise<void> => {
	await enrollmentPanel(page)
		.getByRole("switch", {
			name: "Automatically add new tables",
		})
		.click()
}

const expectSavedConfigurationPreserved = (
	requests: EnrollmentFixtureRequests,
	update: JobBase,
): void => {
	const { job } = requests
	expect(update).toMatchObject({
		name: job.name,
		frequency: job.frequency,
		activate: job.activate,
		source: job.source,
		destination: job.destination,
		catalog_revision: ENROLLMENT_CATALOG_REVISION,
		difference_streams: "{}",
	})
	// Exact serialized round trips protect overrides, existing enrollment, disabled
	// selections, known-but-unselected tables, and available-only namespaces.
	expect(update.streams_config).toBe(job.streams_config)
	expect(update.available_streams_config).toBe(job.available_streams_config)
	expect(update.selected_streams_config).toBe(job.selected_streams_config)
	if (job.streams_config) {
		expect(update).not.toHaveProperty("available_streams_config")
		expect(update).not.toHaveProperty("selected_streams_config")
	} else {
		expect(update).not.toHaveProperty("streams_config")
	}
}

const expectOnlySettingsWrites = (
	requests: EnrollmentFixtureRequests,
	count = 1,
): void => {
	expect(requests.writes).toHaveLength(count)
	for (const write of requests.writes) {
		expect(write).toMatch(/^PUT .*\/jobs\/42$/)
	}
}

const saveSettings = async (page: Page): Promise<void> => {
	await page.getByRole("button", { name: "Save", exact: true }).click()
}

test.describe("Automatic table enrollment (intercepted APIs only)", () => {
	test.setTimeout(30_000)
	test.use({ actionTimeout: 10_000 })

	for (const policy of [undefined, { enabled: false }] satisfies (
		| AutoTableEnrollmentPolicy
		| undefined
	)[]) {
		test(`${policy ? "disabled" : "missing"} policy stays off and ordinary settings save does not opt in`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, { policy })
			await page.goto("/jobs/42/settings")
			const panel = enrollmentPanel(page)
			await expect(
				panel.getByRole("switch", {
					name: "Automatically add new tables",
				}),
			).toHaveAttribute("aria-checked", "false")
			await expect(panel.getByRole("checkbox")).toHaveCount(0)
			expect(requests.writes).toEqual([])

			await saveSettings(page)
			await expect(page).toHaveURL(/\/jobs$/)
			expect(requests.updates).toHaveLength(1)
			expect(requests.updates[0].advanced_settings).toEqual(
				requests.job.advanced_settings,
			)
			expectSavedConfigurationPreserved(requests, requests.updates[0])
			expectOnlySettingsWrites(requests)
		})
	}

	test("saves new-table defaults while automatic enrollment stays disabled", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page)
		await page.goto("/jobs/42/settings")
		await chooseExplicitDefaults(page)
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		const savedPolicy = {
			enabled: false,
			schemas: [],
			defaults: savedEnrollmentPolicy.defaults,
		}
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment,
		).toEqual(savedPolicy)
		expectSavedConfigurationPreserved(requests, requests.updates[0])
		expectOnlySettingsWrites(requests)
		await page.goto("/jobs/42/settings")
		await expect(
			enrollmentPanel(page).getByRole("switch", {
				name: "Automatically add new tables",
			}),
		).toHaveAttribute("aria-checked", "false")
		await enableEnrollment(page)
		await approveSchema(page, "public", "warehouse:public_new")
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(
			requests.updates[1].advanced_settings?.auto_table_enrollment,
		).toEqual(savedEnrollmentPolicy)
		expectSavedConfigurationPreserved(requests, requests.updates[1])
		expectOnlySettingsWrites(requests, 2)
	})

	for (const format of [
		"legacy",
		"split",
	] satisfies EnrollmentCatalogFormat[]) {
		test(`${format}: saves approved schema mappings and explicit defaults with revision without changing existing settings`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, { format })
			await page.goto("/jobs/42/settings")
			await enableEnrollment(page)
			const panel = enrollmentPanel(page)
			await expect(
				panel.getByRole("checkbox", { name: "Approve public", exact: true }),
			).not.toBeChecked()
			// archive has no selected tables: approval must use the full available catalog.
			await expect(
				panel.getByRole("checkbox", { name: "Approve archive", exact: true }),
			).not.toBeChecked()
			await expect(panel.getByRole("checkbox")).toHaveCount(2)
			await expect(panel.getByRole("textbox")).toHaveCount(0)
			await approveSchema(page, "public", "  warehouse:approved_public  ")
			await approveSchema(page, "archive", "warehouse:approved_archive")
			await chooseExplicitDefaults(page)
			expect(requests.writes).toEqual([])

			await saveSettings(page)
			await expect(page).toHaveURL(/\/jobs$/)
			expect(requests.updates).toHaveLength(1)
			const update = requests.updates[0]
			expectSavedConfigurationPreserved(requests, update)
			expect(update.advanced_settings).toEqual({
				...requests.job.advanced_settings,
				auto_table_enrollment: {
					enabled: true,
					schemas: [
						{
							namespace: "public",
							destination_database: "warehouse:approved_public",
						},
						{
							namespace: "archive",
							destination_database: "warehouse:approved_archive",
						},
					],
					defaults: savedEnrollmentPolicy.defaults,
				},
			})
			expectOnlySettingsWrites(requests)
		})

		test(`${format}: disabling stops enrollment without deselecting previously enrolled or disabled tables`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, {
				format,
				policy: savedEnrollmentPolicy,
			})
			await page.goto("/jobs/42/settings")
			const toggle = enrollmentPanel(page).getByRole("switch", {
				name: "Automatically add new tables",
			})
			await expect(toggle).toHaveAttribute("aria-checked", "true")
			await toggle.click()
			await expect(toggle).toHaveAttribute("aria-checked", "false")
			await expect(enrollmentPanel(page).getByRole("checkbox")).toHaveCount(0)
			await saveSettings(page)
			await expect(page).toHaveURL(/\/jobs$/)
			expect(requests.updates).toHaveLength(1)
			const update = requests.updates[0]
			expectSavedConfigurationPreserved(requests, update)
			expect(update.advanced_settings).toEqual({
				...requests.job.advanced_settings,
				auto_table_enrollment: { ...savedEnrollmentPolicy, enabled: false },
			})
			expectOnlySettingsWrites(requests)
		})
	}

	test("rejects empty schema approval and blank destination mapping before any settings write", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page)
		await page.goto("/jobs/42/settings")
		await enableEnrollment(page)
		await chooseExplicitDefaults(page)
		await saveSettings(page)
		await expect(
			page.getByText("Approve at least one existing source schema.", {
				exact: true,
			}),
		).toBeVisible()
		expect(requests.writes).toEqual([])

		await approveSchema(page, "public", "   ")
		await saveSettings(page)
		await expect(
			page.getByText("Enter a destination database for public.", {
				exact: true,
			}),
		).toBeVisible()
		await expect(
			enrollmentPanel(page).getByRole("textbox", {
				name: "Destination database for public",
				exact: true,
			}),
		).toHaveValue("   ")
		await expect(page).toHaveURL(/\/jobs\/42\/settings$/)
		expect(requests.updates).toEqual([])
		expect(requests.writes).toEqual([])
	})

	for (const [omittedDefault] of DEFAULT_CHOICES) {
		test(`requires an explicit ${omittedDefault} choice instead of inheriting a selected table setting`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page)
			await page.goto("/jobs/42/settings")
			await enableEnrollment(page)
			await approveSchema(page, "public", "warehouse:approved_public")
			await chooseExplicitDefaults(page, omittedDefault)
			await saveSettings(page)
			await expect(
				page.getByText(EXPLICIT_DEFAULTS_ERROR, { exact: true }),
			).toBeVisible()
			await expect(
				enrollmentPanel(page).getByRole("checkbox", {
					name: "Approve public",
					exact: true,
				}),
			).toBeChecked()
			await expect(page).toHaveURL(/\/jobs\/42\/settings$/)
			expect(requests.updates).toEqual([])
			expect(requests.writes).toEqual([])
		})
	}

	test("stale 409 retains unsaved mappings and defaults without refetching, resetting, or retrying automatically", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			format: "split",
			policy: savedEnrollmentPolicy,
			rejectStale: true,
		})
		await page.goto("/jobs/42/settings")
		const panel = enrollmentPanel(page)
		await panel
			.getByRole("textbox", {
				name: "Destination database for public",
				exact: true,
			})
			.fill("warehouse:unsaved_public")
		await approveSchema(page, "archive", "warehouse:unsaved_archive")
		await chooseDefault(page, "Normalization for new tables", "Disabled")
		await chooseDefault(page, "Write mode for new tables", "Append")
		await chooseDefault(page, "Delete format for new tables", "Deletion Vector")
		await chooseDefault(page, "Automatically sync new columns", "Disabled")
		await saveSettings(page)
		await expect(
			page.getByText(ENROLLMENT_STALE_MESSAGE, { exact: true }),
		).toBeVisible()
		await expect(page).toHaveURL(/\/jobs\/42\/settings$/)
		await expect(
			page.getByRole("button", { name: "Save", exact: true }),
		).toBeEnabled()
		await expect(
			panel.getByRole("switch", { name: "Automatically add new tables" }),
		).toHaveAttribute("aria-checked", "true")
		await expect(
			panel.getByRole("checkbox", { name: "Approve public", exact: true }),
		).toBeChecked()
		await expect(
			panel.getByRole("checkbox", { name: "Approve archive", exact: true }),
		).toBeChecked()
		await expect(
			panel.getByRole("textbox", {
				name: "Destination database for public",
				exact: true,
			}),
		).toHaveValue("warehouse:unsaved_public")
		await expect(
			panel.getByRole("textbox", {
				name: "Destination database for archive",
				exact: true,
			}),
		).toHaveValue("warehouse:unsaved_archive")
		await expect(
			panel.getByText("Disabled", { exact: true }).filter({ visible: true }),
		).toHaveCount(2)
		await expect(
			panel.getByText("Append", { exact: true }).filter({ visible: true }),
		).toBeVisible()
		await expect(
			panel
				.getByText("Deletion Vector", { exact: true })
				.filter({ visible: true }),
		).toBeVisible()
		expect(requests.jobReads).toBe(1)
		expect(requests.updates).toHaveLength(1)
		expectOnlySettingsWrites(requests)
		expectSavedConfigurationPreserved(requests, requests.updates[0])
		expect(requests.updates[0].advanced_settings).toEqual({
			...requests.job.advanced_settings,
			auto_table_enrollment: {
				enabled: true,
				schemas: [
					{
						namespace: "public",
						destination_database: "warehouse:unsaved_public",
					},
					{
						namespace: "archive",
						destination_database: "warehouse:unsaved_archive",
					},
				],
				defaults: {
					normalization: false,
					append_mode: true,
					update_type: "dv",
					sync_new_columns: false,
				},
			},
		})

		// A second *user* save must still submit the retained edit/revision, not
		// silently adopt a newer catalog or restore the originally loaded policy.
		const secondConflict = page.waitForResponse(
			response =>
				response.request().method() === "PUT" &&
				new URL(response.url()).pathname.endsWith("/jobs/42") &&
				response.status() === 409,
		)
		await saveSettings(page)
		await secondConflict
		expect(requests.updates).toHaveLength(2)
		expect(requests.updates[1]).toEqual(requests.updates[0])
		expect(requests.jobReads).toBe(1)
		expectOnlySettingsWrites(requests, 2)
	})
})
