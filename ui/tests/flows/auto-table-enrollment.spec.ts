import type { Locator, Page } from "@playwright/test"

import { UpsertType } from "../../src/modules/ingestion/common/types"
import type { StreamsDataStructure } from "../../src/modules/ingestion/common/types"
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

const enrollmentPanel = (page: Page): Locator =>
	page.getByRole("region", {
		name: "Automatically add new tables",
		exact: true,
	})

const enrollmentToggle = (page: Page): Locator =>
	enrollmentPanel(page).getByRole("switch", {
		name: "Automatically add new tables",
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

const expectNoObsoleteControls = async (page: Page): Promise<void> => {
	await expect(enrollmentPanel(page).getByRole("checkbox")).toHaveCount(0)
	await expect(enrollmentPanel(page).getByRole("textbox")).toHaveCount(0)
	await expect(
		page.getByText("Source schemas to check", { exact: true }),
	).toHaveCount(0)
	await expect(
		page.getByRole("textbox", { name: /Destination database for/ }),
	).toHaveCount(0)
}

const expectRequestedDefaults = async (page: Page): Promise<void> => {
	const panel = enrollmentPanel(page)
	await expect(enrollmentToggle(page)).toHaveAttribute("aria-checked", "true")
	await expect(panel.getByText("Enabled", { exact: true })).toHaveCount(2)
	await expect(panel.getByText("Upsert", { exact: true })).toBeVisible()
	await expect(
		panel.getByText("Positional Deletes", { exact: true }),
	).toBeVisible()
	await expectNoObsoleteControls(page)
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
		catalog_revision: job.catalog_revision ?? "",
		difference_streams: "{}",
	})
	// Exact serialized round trips protect overrides, existing enrollment, disabled
	// selections, known-but-unselected tables, and available-only namespaces.
	expect(update.streams_config).toBe(job.streams_config)
	expect(update.available_streams_config).toBe(job.available_streams_config)
	expect(update.selected_streams_config).toBe(job.selected_streams_config)
}

const inferredSchemas = [
	{ namespace: "archive", destination_database: "warehouse:archive" },
	{ namespace: "public", destination_database: "warehouse:public_new" },
]

const saveSettings = async (page: Page): Promise<void> => {
	await page.getByRole("button", { name: "Save", exact: true }).click()
}

const selectCreationPair = async (page: Page): Promise<void> => {
	await page.goto("/jobs/new")
	await page.getByTestId("source-connector-select").click()
	await page
		.locator(".ant-select-dropdown:visible")
		.getByTestId("connector-option-postgres")
		.click()
	await page.getByTestId("existing-source").click()
	await page
		.locator(".ant-select-dropdown:visible")
		.getByText("Fixture PostgreSQL", { exact: true })
		.click()
	await page.getByTestId("destination-connector-select").click()
	await page
		.locator(".ant-select-dropdown:visible")
		.getByTestId("connector-option-iceberg")
		.click()
	await page.getByTestId("existing-destination").click()
	await page
		.locator(".ant-select-dropdown:visible")
		.getByText("Fixture Iceberg", { exact: true })
		.click()
	await page.locator("input").first().fill("enrollment_creation")
}

const advanceCreation = async (page: Page): Promise<void> => {
	await page.getByTestId("create-job-wizard-submit").click()
	await expect(
		page.getByRole("button", { name: "Create Job", exact: true }),
	).toBeVisible()
	await expect(
		page.getByRole("switch", { name: "Normalization", exact: true }),
	).toBeVisible()
}

const createJob = async (page: Page, expectIndex = true): Promise<void> => {
	await page.getByTestId("create-job-wizard-submit").click()
	if (expectIndex) {
		await page
			.getByRole("dialog")
			.getByRole("button", { name: "Confirm", exact: true })
			.click()
	}
}

const submittedCatalog = (job: JobBase): StreamsDataStructure =>
	job.streams_config
		? JSON.parse(job.streams_config)
		: {
				...JSON.parse(job.available_streams_config ?? "{}"),
				...JSON.parse(job.selected_streams_config ?? "{}"),
			}

test.describe("Automatic table enrollment (intercepted APIs only)", () => {
	test.setTimeout(30_000)
	test.use({ actionTimeout: 10_000 })

	for (const format of [
		"legacy",
		"split",
	] satisfies EnrollmentCatalogFormat[]) {
		test(`${format}: absent policy displays default-on and infers routing only on explicit save`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, { format })
			await page.goto("/jobs/42/settings")
			await expectRequestedDefaults(page)
			expect(requests.writes).toEqual([])
			await expect(
				page.getByRole("combobox", { name: "Target Query Engines" }),
			).toBeDisabled()
			await saveSettings(page)
			await expect(page).toHaveURL(/\/jobs$/)
			expectSavedConfigurationPreserved(requests, requests.updates[0])
			expect(requests.updates[0].advanced_settings).toEqual({
				...requests.job.advanced_settings,
				auto_table_enrollment: {
					enabled: true,
					schemas: inferredSchemas,
					defaults: savedEnrollmentPolicy.defaults,
				},
			})
			expect(requests.specRequests).toBe(1)
			expect(requests.specBodies).toEqual([
				{
					type: requests.job.source.type,
					version: requests.job.source.version,
					available_query_engines: true,
				},
			])
			expect(requests.writes).toEqual(["PUT /api/v1/project/123/jobs/42"])
		})

		test(`${format}: creation applies requested defaults over connector-generated settings and submits inferred policy`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, { format })
			await selectCreationPair(page)
			await expectRequestedDefaults(page)
			await advanceCreation(page)
			await expectRequestedDefaults(page)
			await expect(
				page.getByRole("switch", { name: "Normalization", exact: true }),
			).toHaveAttribute("aria-checked", "true")
			await createJob(page)
			await expect.poll(() => requests.creations.length).toBe(1)
			const created = requests.creations[0]
			expect(created.advanced_settings?.auto_table_enrollment).toEqual({
				enabled: true,
				schemas: inferredSchemas,
				defaults: savedEnrollmentPolicy.defaults,
			})
			const catalog = submittedCatalog(created)
			expect(catalog.selected_streams.public).toEqual([
				expect.objectContaining({
					stream_name: "orders",
					normalization: true,
					append_mode: false,
					update_type: "pos",
					selected_columns: { columns: ["id"], sync_new_columns: true },
				}),
			])
			expect(catalog.selected_streams.archive ?? []).toHaveLength(0)
			expect(created.advanced_settings?.index_required).toBe(true)
			expect(requests.specRequests).toBe(1)
		})

		test(`${format}: stream editing exposes defaults without rewriting saved selections or activating disabled tables`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, { format })
			await page.goto("/jobs/42/edit")
			await expectRequestedDefaults(page)
			await expect(
				page.getByRole("switch", { name: "Normalization", exact: true }),
			).toHaveAttribute("aria-checked", "false")
			await page.getByRole("button", { name: "Finish", exact: true }).click()
			await expect(page).toHaveURL(/\/jobs$/)
			const update = requests.updates[0]
			expect(update.catalog_revision).toBe(ENROLLMENT_CATALOG_REVISION)
			expect(update.advanced_settings?.auto_table_enrollment).toEqual({
				enabled: true,
				schemas: inferredSchemas,
				defaults: savedEnrollmentPolicy.defaults,
			})
			const selected = submittedCatalog(update).selected_streams.public
			expect(
				selected.find(stream => stream.stream_name === "orders"),
			).toMatchObject({
				normalization: false,
				append_mode: true,
				selected_columns: { sync_new_columns: false },
			})
			expect(
				selected.find(stream => stream.stream_name === "previously_enrolled"),
			).toMatchObject({
				normalization: true,
				append_mode: false,
				update_type: "pos",
			})
			expect(
				selected.some(stream => stream.stream_name === "disabled_table"),
			).toBe(false)
			expect(
				selected.some(stream => stream.stream_name === "known_unselected"),
			).toBe(false)
			for (const stream of selected) {
				expect(stream).not.toHaveProperty("drop_removed_columns")
			}
		})

		test(`${format}: drop removed columns defaults off and saves on the edited stream only`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, { format })
			await page.goto("/jobs/42/edit")
			const dropSwitch = page.getByRole("switch", {
				name: "Remove columns dropped from source",
				exact: true,
			})
			await expect(dropSwitch).toHaveAttribute("aria-checked", "false")
			await expect(dropSwitch).toBeDisabled()
			await page
				.getByRole("switch", { name: "Normalization", exact: true })
				.click()
			await dropSwitch.click()
			await expect(dropSwitch).toHaveAttribute("aria-checked", "true")
			await page.getByRole("button", { name: "Finish", exact: true }).click()
			await expect(page).toHaveURL(/\/jobs$/)
			const selected = submittedCatalog(requests.updates[0]).selected_streams
				.public
			expect(
				selected.find(stream => stream.stream_name === "orders"),
			).toMatchObject({ normalization: true, drop_removed_columns: true })
			expect(
				selected.find(stream => stream.stream_name === "previously_enrolled"),
			).not.toHaveProperty("drop_removed_columns")
		})

		test(`${format}: selecting a known unselected table uses new-table defaults and preserves explicit and sparse saved choices`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, {
				format,
				sparseSelection: true,
				routing: "custom",
			})
			await page.goto("/jobs/42/edit")
			await expectRequestedDefaults(page)
			const newTable = page.getByRole("button", {
				name: "known_unselected",
				exact: true,
			})
			await expect(newTable.getByRole("checkbox")).not.toBeChecked()
			await newTable.getByRole("checkbox").check()
			await expect(
				page.getByRole("switch", { name: "Normalization", exact: true }),
			).toHaveAttribute("aria-checked", "true")
			await expect(
				page.getByRole("radio", { name: "Upsert", exact: true }),
			).toBeChecked()
			await expect(
				page.locator(".ant-select").filter({
					has: page.getByRole("combobox", {
						name: "Upsert Type",
						exact: true,
					}),
				}),
			).toContainText("Positional Deletes")
			await page.getByRole("button", { name: "Finish", exact: true }).click()
			await page
				.getByRole("dialog")
				.getByRole("button", { name: "Confirm", exact: true })
				.click()
			await expect(page).toHaveURL(/\/jobs$/)
			const selected = submittedCatalog(requests.updates[0]).selected_streams
			expect(
				selected.public.find(
					stream => stream.stream_name === "known_unselected",
				),
			).toMatchObject({
				normalization: true,
				append_mode: false,
				update_type: "pos",
				selected_columns: { columns: ["id"], sync_new_columns: true },
				destination_database: "custom_warehouse",
			})
			expect(
				selected.public.find(stream => stream.stream_name === "orders"),
			).toMatchObject({
				normalization: false,
				append_mode: true,
				selected_columns: { sync_new_columns: false },
				destination_database: "custom_warehouse",
			})
			expect(
				selected.public.find(
					stream => stream.stream_name === "previously_enrolled",
				),
			).toMatchObject({
				normalization: true,
				append_mode: false,
				update_type: "pos",
			})
			expect(
				selected.public.find(stream => stream.stream_name === "sparse_saved"),
			).toMatchObject({
				normalization: false,
				append_mode: format === "legacy" ? false : true,
				destination_database: "custom_warehouse",
			})
			expect(
				selected.public.some(stream => stream.stream_name === "disabled_table"),
			).toBe(false)
			expect(selected.archive ?? []).toHaveLength(0)
		})
	}

	test("explicit disabled policy and false/append/equality/column choices stay saved", async ({
		page,
	}) => {
		const policy: AutoTableEnrollmentPolicy = {
			enabled: false,
			schemas: savedEnrollmentPolicy.schemas,
			defaults: {
				normalization: false,
				append_mode: true,
				update_type: UpsertType.EQUALITY,
				sync_new_columns: false,
			},
		}
		const requests = await installEnrollmentFixture(page, { policy })
		await page.goto("/jobs/42/settings")
		await expect(enrollmentToggle(page)).toHaveAttribute(
			"aria-checked",
			"false",
		)
		await expect(
			enrollmentPanel(page).getByText("Disabled", { exact: true }),
		).toHaveCount(3)
		await expect(
			enrollmentPanel(page).getByText("Append", { exact: true }),
		).toBeVisible()
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment,
		).toEqual(policy)
		expectSavedConfigurationPreserved(requests, requests.updates[0])
	})

	test("a saved policy without drop_removed_columns loads as off and saves it only when chosen", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			format: "split",
			policy: savedEnrollmentPolicy,
		})
		await page.goto("/jobs/42/settings")
		await expect(
			enrollmentPanel(page)
				.locator(".ant-select")
				.filter({
					has: page.getByRole("combobox", {
						name: "Remove columns dropped from source",
						exact: true,
					}),
				})
				.locator(".ant-select-selection-item"),
		).toHaveText("Disabled")
		await expect(
			page.getByText(/Save or discard unsaved settings/),
		).toHaveCount(0)
		await chooseDefault(page, "Remove columns dropped from source", "Enabled")
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment,
		).toEqual({
			...savedEnrollmentPolicy,
			schemas: inferredSchemas,
			defaults: {
				...savedEnrollmentPolicy.defaults,
				drop_removed_columns: true,
			},
		})
	})

	test("bulk configure sets drop_removed_columns only on the selected streams", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, { format: "split" })
		await page.goto("/jobs/42/edit")
		await page
			.getByRole("button", { name: "Bulk Configure", exact: true })
			.click()
		const dialog = page.getByRole("dialog")
		await dialog.getByRole("checkbox", { name: "orders", exact: true }).check()
		await dialog
			.getByRole("button", { name: "Configure Streams", exact: true })
			.click()
		const dropSwitch = dialog.getByRole("switch", {
			name: "Remove columns dropped from source",
			exact: true,
		})
		await expect(dropSwitch).toHaveAttribute("aria-checked", "false")
		await expect(dropSwitch).toBeDisabled()
		await dialog
			.getByRole("switch", { name: "Normalization", exact: true })
			.click()
		await dropSwitch.click()
		await dialog
			.getByRole("button", { name: "Review Changes", exact: true })
			.click()
		await dialog
			.getByRole("button", { name: "Apply Changes", exact: true })
			.click()
		await expect(dialog).toHaveCount(0, { timeout: 10_000 })
		await page.getByRole("button", { name: "Finish", exact: true }).click()
		await expect(page).toHaveURL(/\/jobs$/)
		const selected = submittedCatalog(requests.updates[0]).selected_streams
			.public
		expect(
			selected.find(stream => stream.stream_name === "orders"),
		).toMatchObject({ normalization: true, drop_removed_columns: true })
		expect(
			selected.find(stream => stream.stream_name === "previously_enrolled"),
		).not.toHaveProperty("drop_removed_columns")
	})

	test("explicit empty disabled policy receives settings defaults but never opts in", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			policy: { enabled: false },
		})
		await page.goto("/jobs/42/settings")
		await expect(enrollmentToggle(page)).toHaveAttribute(
			"aria-checked",
			"false",
		)
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment,
		).toEqual({
			enabled: false,
			schemas: [],
			defaults: savedEnrollmentPolicy.defaults,
		})
	})

	test("unsupported connector pairs never opt in when no policy was saved", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			sourceType: "mysql",
		})
		await page.goto("/jobs/42/settings")
		await expect(enrollmentToggle(page)).toHaveAttribute(
			"aria-checked",
			"false",
		)
		await expect(enrollmentToggle(page)).toBeDisabled()
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(requests.updates[0].advanced_settings).toEqual(
			requests.job.advanced_settings,
		)
		expectSavedConfigurationPreserved(requests, requests.updates[0])
	})

	test("opting out preserves removed schema mappings and unsupported saved delete choices", async ({
		page,
	}) => {
		const policy: AutoTableEnrollmentPolicy = {
			...savedEnrollmentPolicy,
			schemas: [
				{ namespace: "removed_schema", destination_database: "old:removed" },
			],
			defaults: {
				...savedEnrollmentPolicy.defaults,
				update_type: UpsertType.DELETION_VECTOR,
			},
		}
		const requests = await installEnrollmentFixture(page, {
			policy,
			readerSupports: [UpsertType.EQUALITY],
		})
		await page.goto("/jobs/42/settings")
		await enrollmentToggle(page).click()
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment,
		).toEqual({ ...policy, enabled: false })
		expectSavedConfigurationPreserved(requests, requests.updates[0])
	})

	for (const routing of ["dynamic", "custom"] satisfies (
		| "dynamic"
		| "custom"
	)[]) {
		test(`infers ${routing} database routing for selected overrides and discovered unselected namespaces`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, {
				routing,
				format: "split",
			})
			await page.goto("/jobs/42/settings")
			await expectNoObsoleteControls(page)
			await saveSettings(page)
			await expect(page).toHaveURL(/\/jobs$/)
			expect(
				requests.updates[0].advanced_settings?.auto_table_enrollment?.schemas,
			).toEqual(
				routing === "custom"
					? [
							{
								namespace: "archive",
								destination_database: "custom_warehouse",
							},
							{ namespace: "public", destination_database: "custom_warehouse" },
						]
					: inferredSchemas,
			)
			expectSavedConfigurationPreserved(requests, requests.updates[0])
		})
	}

	for (const routing of ["conflicting", "available-conflicting"] satisfies (
		| "conflicting"
		| "available-conflicting"
	)[]) {
		test(`rejects ambiguous ${routing} namespace routing instead of selecting the first database`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, {
				routing,
				...(routing === "available-conflicting" && {
					sparseSelection: true,
					sparseNamespace: "audit",
					sparseDestinationDatabase: "other:audit",
				}),
			})
			await page.goto("/jobs/42/settings")
			await saveSettings(page)
			await expect(
				page.getByText(/Cannot infer one destination database/).last(),
			).toBeVisible()
			expect(requests.updates).toEqual([])
			expect(requests.writes).toEqual([])
		})
	}

	for (const staleUpdateTypes of [null, [UpsertType.EQUALITY]]) {
		test(`current readers allow deletion vectors despite ${staleUpdateTypes === null ? "all-NULL" : "stale equality-only"} saved capabilities`, async ({
			page,
		}) => {
			const policy: AutoTableEnrollmentPolicy = {
				...savedEnrollmentPolicy,
				defaults: {
					...savedEnrollmentPolicy.defaults,
					update_type: UpsertType.DELETION_VECTOR,
				},
			}
			const requests = await installEnrollmentFixture(page, {
				policy,
				staleUpdateTypes,
			})
			await page.goto("/jobs/42/settings")
			await chooseDefault(
				page,
				"Delete format for new tables",
				"Deletion Vector",
			)
			await saveSettings(page)
			await expect(page).toHaveURL(/\/jobs$/)
			expect(
				requests.updates[0].advanced_settings?.auto_table_enrollment?.defaults
					?.update_type,
			).toBe("dv")
			expectSavedConfigurationPreserved(requests, requests.updates[0])
		})
	}

	test("legacy reader names are normalized for capabilities without rewriting saved settings", async ({
		page,
	}) => {
		const targets = ["  TrInO  ", "", "   "]
		const policy: AutoTableEnrollmentPolicy = {
			...savedEnrollmentPolicy,
			defaults: {
				...savedEnrollmentPolicy.defaults,
				update_type: UpsertType.DELETION_VECTOR,
			},
		}
		const requests = await installEnrollmentFixture(page, {
			policy,
			targetQueryEngines: targets,
			staleUpdateTypes: [UpsertType.EQUALITY],
		})
		await page.goto("/jobs/42/settings")
		await chooseDefault(page, "Delete format for new tables", "Deletion Vector")
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(requests.updates[0].advanced_settings?.target_query_engines).toEqual(
			targets,
		)
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment?.defaults
				?.update_type,
		).toBe("dv")
	})

	test("settings defaults use fetched DV-only readers instead of unhydrated configuration-store readers", async ({
		page,
	}) => {
		const targets = ["  DV-ONLY-READER ", ""]
		const requests = await installEnrollmentFixture(page, {
			targetQueryEngines: targets,
		})
		await page.goto("/jobs/42/settings")
		await expect(
			enrollmentPanel(page).getByText("Deletion Vector", { exact: true }),
		).toBeVisible()
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment?.defaults
				?.update_type,
		).toBe("dv")
		expect(requests.updates[0].advanced_settings?.target_query_engines).toEqual(
			targets,
		)
		expect(requests.specRequests).toBe(1)
	})

	for (const limitingSide of ["reader", "writer"]) {
		test(`the ${limitingSide} capability intersection disables and rejects unsupported saved deletes`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, {
				policy: savedEnrollmentPolicy,
				...(limitingSide === "reader"
					? { readerSupports: [UpsertType.EQUALITY] }
					: { writableTypes: [UpsertType.EQUALITY] }),
			})
			await page.goto("/jobs/42/settings")
			await enrollmentPanel(page)
				.locator(".ant-select")
				.filter({
					has: page.getByRole("combobox", {
						name: "Delete format for new tables",
					}),
				})
				.click()
			await expect(
				page
					.locator(".ant-select-dropdown:visible")
					.getByTitle("Positional Deletes", { exact: true }),
			).toHaveClass(/ant-select-item-option-disabled/)
			await page.keyboard.press("Escape")
			await saveSettings(page)
			await expect(
				page.getByText(
					"Choose a delete format supported by the current source version and target query engines.",
					{ exact: true },
				),
			).toBeVisible()
			expect(requests.writes).toEqual([])
			await chooseDefault(
				page,
				"Delete format for new tables",
				"Equality Deletes",
			)
			await saveSettings(page)
			await expect(page).toHaveURL(/\/jobs$/)
			expect(
				requests.updates[0].advanced_settings?.auto_table_enrollment?.defaults
					?.update_type,
			).toBe("eq")
		})
	}

	test("a new policy selects a compatible fallback when positional deletes cannot be read", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			readerSupports: [UpsertType.EQUALITY],
		})
		await page.goto("/jobs/42/settings")
		await expect(
			enrollmentPanel(page).getByText("Equality Deletes", { exact: true }),
		).toBeVisible()
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment?.defaults
				?.update_type,
		).toBe("eq")
	})

	test("creation never overrides source write compatibility with its positional default", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			writableTypes: [UpsertType.EQUALITY],
			format: "split",
		})
		await selectCreationPair(page)
		await expect(
			enrollmentPanel(page).getByText("Equality Deletes", { exact: true }),
		).toBeVisible()
		await advanceCreation(page)
		await createJob(page, false)
		await expect.poll(() => requests.creations.length).toBe(1)
		expect(
			requests.creations[0].advanced_settings?.auto_table_enrollment?.defaults
				?.update_type,
		).toBe("eq")
		expect(
			submittedCatalog(requests.creations[0]).selected_streams.public[0],
		).toMatchObject({
			normalization: true,
			append_mode: false,
			update_type: "eq",
		})
	})

	for (const capabilityState of ["error", "loading"] satisfies (
		| "error"
		| "loading"
	)[]) {
		test(`capability ${capabilityState} blocks enrollment save but never blocks opting out`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, {
				capabilityState,
				policy: savedEnrollmentPolicy,
			})
			await page.goto("/jobs/42/settings")
			await expect(enrollmentToggle(page)).toHaveAttribute(
				"aria-checked",
				"true",
			)
			await expect(
				page
					.getByText(
						capabilityState === "loading"
							? "Wait for delete-format capabilities to load before saving automatic enrollment."
							: "Delete-format capabilities could not be loaded. Retry fetching query engines before saving automatic enrollment.",
						{ exact: true },
					)
					.first(),
			).toBeVisible()
			await saveSettings(page)
			await expect(
				page
					.getByText(
						capabilityState === "loading"
							? "Wait for delete-format capabilities to load before saving automatic enrollment."
							: "Delete-format capabilities could not be loaded. Retry fetching query engines before saving automatic enrollment.",
						{ exact: true },
					)
					.last(),
			).toBeVisible()
			expect(requests.updates).toEqual([])
			await enrollmentToggle(page).click()
			await saveSettings(page)
			await expect(page).toHaveURL(/\/jobs$/)
			expect(
				requests.updates[0].advanced_settings?.auto_table_enrollment,
			).toEqual({ ...savedEnrollmentPolicy, enabled: false })
			requests.releaseCapabilities()
		})
	}

	for (const scenario of [
		{ name: "unreadable catalog", invalidCatalog: true },
		{ name: "unsupported source pair", sourceType: "mysql" },
		{ name: "conflicting mappings", routing: "conflicting" },
	] satisfies {
		name: string
		invalidCatalog?: boolean
		omitRevision?: boolean
		sourceType?: string
		routing?: "conflicting"
	}[]) {
		test(`disabling a populated policy bypasses ${scenario.name} and preserves the saved policy`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, {
				...scenario,
				policy: savedEnrollmentPolicy,
			})
			await page.goto("/jobs/42/settings")
			await enrollmentToggle(page).click()
			await saveSettings(page)
			await expect(page).toHaveURL(/\/jobs$/)
			expect(
				requests.updates[0].advanced_settings?.auto_table_enrollment,
			).toEqual({ ...savedEnrollmentPolicy, enabled: false })
			expectSavedConfigurationPreserved(requests, requests.updates[0])
		})
	}

	test("enabled enrollment requires the original catalog revision", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			omitRevision: true,
		})
		await page.goto("/jobs/42/settings")
		await saveSettings(page)
		await expect(
			page.getByText(
				"Refresh this job to obtain its catalog revision before enabling automatic table enrollment.",
				{ exact: true },
			),
		).toBeVisible()
		expect(requests.writes).toEqual([])
	})

	test("disabling an enabled policy still requires the original catalog revision", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			omitRevision: true,
			policy: savedEnrollmentPolicy,
		})
		await page.goto("/jobs/42/settings")
		await enrollmentToggle(page).click()
		await saveSettings(page)
		await expect(
			page.getByText(
				"Refresh this job to obtain its catalog revision before enabling automatic table enrollment.",
				{ exact: true },
			),
		).toBeVisible()
		expect(requests.writes).toEqual([])
	})

	test("an already-disabled policy remains saveable without a revision", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			omitRevision: true,
			policy: { ...savedEnrollmentPolicy, enabled: false },
		})
		await page.goto("/jobs/42/settings")
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment?.enabled,
		).toBe(false)
	})

	test("a present legacy policy without an enabled flag stays disabled", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			sparsePolicy: true,
			omitRevision: true,
		})
		await page.goto("/jobs/42/settings")
		await expect(enrollmentToggle(page)).toHaveAttribute(
			"aria-checked",
			"false",
		)
		expect(requests.writes).toEqual([])
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment,
		).toEqual({
			enabled: false,
			schemas: [],
			defaults: savedEnrollmentPolicy.defaults,
		})
	})

	test("stale 409 retains defaults and original revision without refetching, resetting, or retrying automatically", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			format: "split",
			policy: savedEnrollmentPolicy,
			rejectStale: true,
		})
		await page.goto("/jobs/42/settings")
		await chooseDefault(page, "Normalization for new tables", "Disabled")
		await chooseDefault(page, "Delete format for new tables", "Deletion Vector")
		await chooseDefault(page, "Automatically sync new columns", "Disabled")
		await saveSettings(page)
		await expect(
			page.getByText(ENROLLMENT_STALE_MESSAGE, { exact: true }),
		).toBeVisible()
		await expect(page).toHaveURL(/\/jobs\/42\/settings$/)
		await expect(
			enrollmentPanel(page).getByText("Disabled", { exact: true }),
		).toHaveCount(3)
		await expect(
			enrollmentPanel(page).getByText("Deletion Vector", { exact: true }),
		).toBeVisible()
		expect(requests.updates).toHaveLength(1)
		expect(requests.jobReads).toBe(1)
		expectSavedConfigurationPreserved(requests, requests.updates[0])
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment,
		).toEqual({
			enabled: true,
			schemas: inferredSchemas,
			defaults: {
				normalization: false,
				append_mode: false,
				update_type: "dv",
				sync_new_columns: false,
			},
		})
		// A second *user* save must still submit the retained edit/revision, not
		// silently adopt a newer catalog or restore the originally loaded policy.
		const secondConflict = page.waitForResponse(
			response =>
				response.request().method() === "PUT" && response.status() === 409,
		)
		await saveSettings(page)
		await secondConflict
		expect(requests.updates[1]).toEqual(requests.updates[0])
		expect(requests.jobReads).toBe(1)
	})

	test("creation policy choices persist through Next, Back, rediscovery and actual create", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, { format: "split" })
		await selectCreationPair(page)
		await enrollmentToggle(page).click()
		await chooseDefault(page, "Normalization for new tables", "Disabled")
		await chooseDefault(page, "Write mode for new tables", "Append")
		await chooseDefault(
			page,
			"Delete format for new tables",
			"Equality Deletes",
		)
		await chooseDefault(page, "Automatically sync new columns", "Disabled")
		await advanceCreation(page)
		await page.getByRole("button", { name: "Back", exact: true }).click()
		await page.getByRole("button", { name: "Yes, Leave", exact: true }).click()
		await expect(enrollmentToggle(page)).toHaveAttribute(
			"aria-checked",
			"false",
		)
		await advanceCreation(page)
		await expect(enrollmentToggle(page)).toHaveAttribute(
			"aria-checked",
			"false",
		)
		await expect(
			page.getByRole("switch", { name: "Normalization", exact: true }),
		).toHaveAttribute("aria-checked", "false")
		await createJob(page, false)
		await expect.poll(() => requests.creations.length).toBe(1)
		expect(
			requests.creations[0].advanced_settings?.auto_table_enrollment,
		).toEqual({
			enabled: false,
			schemas: [],
			defaults: {
				normalization: false,
				append_mode: true,
				update_type: "eq",
				sync_new_columns: false,
			},
		})
		expect(
			submittedCatalog(requests.creations[0]).selected_streams.public[0],
		).toMatchObject({
			normalization: false,
			append_mode: true,
			selected_columns: { sync_new_columns: false },
		})
		expect(requests.discoveries).toHaveLength(2)
	})

	test("stream configuration validates against currently edited target readers, not the saved catalog", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			format: "split",
			policy: savedEnrollmentPolicy,
		})
		await page.goto("/jobs/42/edit")
		await expectRequestedDefaults(page)
		await page.getByRole("button", { name: "Back", exact: true }).click()
		await page.getByRole("button", { name: "Yes, Leave", exact: true }).click()
		await page.getByRole("combobox", { name: "Target Query Engines" }).click()
		await page
			.locator(".ant-select-dropdown:visible")
			.getByTitle("Equality reader", { exact: true })
			.click()
		await page.keyboard.press("Escape")
		await page.getByRole("button", { name: "Next", exact: true }).click()
		await page.getByRole("button", { name: "Proceed", exact: true }).click()
		await expect(
			page.getByRole("button", { name: "Finish", exact: true }),
		).toBeEnabled()
		await page.getByRole("button", { name: "Finish", exact: true }).click()
		await expect(
			page.getByText(
				"Choose a delete format supported by the current source version and target query engines.",
				{ exact: true },
			),
		).toBeVisible()
		expect(requests.updates).toEqual([])
		await chooseDefault(
			page,
			"Delete format for new tables",
			"Equality Deletes",
		)
		await page.getByRole("button", { name: "Finish", exact: true }).click()
		await expect(page).toHaveURL(/\/jobs$/)
		expect(requests.updates[0].advanced_settings?.target_query_engines).toEqual(
			["trino", "equality-reader"],
		)
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment?.defaults
				?.update_type,
		).toBe("eq")
		expect(requests.specRequests).toBe(1)
	})

	for (const pagePath of ["/jobs/42/settings", "/jobs/42/edit"]) {
		test(`${pagePath}: sparse inherited routing conflicts with an explicit mapping instead of being replaced by it`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, {
				format: "split",
				sparseSelection: true,
				sparseDestinationDatabase: "other:public",
			})
			await page.goto(pagePath)
			await expectRequestedDefaults(page)
			await page
				.getByRole("button", {
					name: pagePath.endsWith("edit") ? "Finish" : "Save",
					exact: true,
				})
				.click()
			await expect(
				page
					.getByText(
						/Cannot infer one destination database for source schema public/,
					)
					.last(),
			).toBeVisible()
			expect(requests.updates).toEqual([])
		})

		test(`${pagePath}: equivalent dynamic and custom namespaces save without changing stream routing`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, {
				format: "split",
				sparseSelection: true,
				sparseDestinationDatabase: "warehouse_public_new",
			})
			await page.goto(pagePath)
			await expectRequestedDefaults(page)
			await page
				.getByRole("button", {
					name: pagePath.endsWith("edit") ? "Finish" : "Save",
					exact: true,
				})
				.click()
			await expect(page).toHaveURL(/\/jobs$/)
			expect(
				requests.updates[0].advanced_settings?.auto_table_enrollment?.schemas,
			).toEqual([
				{ namespace: "archive", destination_database: "saved:archive" },
				{ namespace: "public", destination_database: "warehouse:public_new" },
			])
			const savedCatalog = submittedCatalog(requests.job)
			const saved = savedCatalog.selected_streams.public
			const updatedCatalog = submittedCatalog(requests.updates[0])
			const updated = updatedCatalog.selected_streams.public
			for (const selection of saved) {
				expect(
					updated.find(stream => stream.stream_name === selection.stream_name)
						?.destination_database ??
						updatedCatalog.streams.find(
							entry =>
								entry.stream.namespace === "public" &&
								entry.stream.name === selection.stream_name,
						)?.stream.destination_database,
				).toBe(
					selection.destination_database ??
						savedCatalog.streams.find(
							entry =>
								entry.stream.namespace === "public" &&
								entry.stream.name === selection.stream_name,
						)?.stream.destination_database,
				)
			}
		})

		test(`${pagePath}: consistent sparse inherited mappings are accepted and preserved`, async ({
			page,
		}) => {
			const requests = await installEnrollmentFixture(page, {
				format: "split",
				sparseSelection: true,
			})
			await page.goto(pagePath)
			await expectRequestedDefaults(page)
			await page
				.getByRole("button", {
					name: pagePath.endsWith("edit") ? "Finish" : "Save",
					exact: true,
				})
				.click()
			await expect(page).toHaveURL(/\/jobs$/)
			expect(
				requests.updates[0].advanced_settings?.auto_table_enrollment?.schemas,
			).toEqual(inferredSchemas)
		})
	}

	test("unselected namespace routing does not inherit an explicit prefix when another selected namespace inherits a different prefix", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			format: "split",
			sparseSelection: true,
			sparseNamespace: "audit",
			sparseDestinationDatabase: "other:audit",
		})
		await page.goto("/jobs/42/settings")
		await saveSettings(page)
		await expect(page).toHaveURL(/\/jobs$/)
		expect(
			requests.updates[0].advanced_settings?.auto_table_enrollment?.schemas,
		).toEqual([
			{ namespace: "archive", destination_database: "saved:archive" },
			{ namespace: "audit", destination_database: "other:audit" },
			{ namespace: "public", destination_database: "warehouse:public_new" },
		])
	})

	test("default display does not block resync, but unsaved enrollment changes do", async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page)
		await page.goto("/jobs/42/settings")
		await expectRequestedDefaults(page)
		await page
			.getByRole("combobox", { name: "Table to resync" })
			.fill("public.orders")
		await page.getByTitle("public.orders", { exact: true }).click()
		const resync = page.getByRole("button", {
			name: "Resync table",
			exact: true,
		})
		await expect(resync).toBeEnabled()
		await enrollmentToggle(page).click()
		await expect(resync).toBeDisabled()
		await expect(page.getByRole("status")).toContainText(
			"Save or discard unsaved settings",
		)
		await enrollmentToggle(page).click()
		await expect(resync).toBeEnabled()
		await resync.click()
		await page
			.getByRole("dialog", { name: "Confirm table resync" })
			.getByRole("button", { name: "Clear and resync table", exact: true })
			.click()
		await expect(page).toHaveURL(/\/jobs\/42\/history$/)
		expect(requests.resyncs).toEqual([
			{ namespace: "public", stream_name: "orders" },
		])
		expect(requests.updates).toEqual([])
	})
})
