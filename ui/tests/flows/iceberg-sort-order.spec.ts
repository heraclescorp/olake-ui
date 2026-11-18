import { expect, test } from "@playwright/test"
import type { Locator, Page } from "@playwright/test"

import type {
	SelectedStreams,
	SortField,
} from "../../src/modules/ingestion/common/types"
import { installEnrollmentFixture } from "../fixtures/auto-table-enrollment.fixture"
import type {
	EnrollmentCatalogFormat,
	EnrollmentFixtureRequests,
} from "../fixtures/auto-table-enrollment.fixture"

const CREATED_AT_DEFAULT: SortField[] = [
	{ column: "created_at", direction: "asc", null_order: "nulls-last" },
]
const SORT_FORMATS: EnrollmentCatalogFormat[] = ["legacy", "split"]

const chooseSortOption = async ({
	page,
	scope,
	label,
	option,
}: {
	page: Page
	scope: Page | Locator
	label: string
	option: string
}): Promise<void> => {
	const combobox = page.getByRole("combobox", { name: label, exact: true })
	await scope
		.locator(".ant-select")
		.filter({ has: combobox })
		.locator(".ant-select-selector")
		.click()
	await page
		.locator(".ant-select-dropdown:visible")
		.getByTitle(option, { exact: true })
		.click()
}

const savedCatalog = (
	requests: EnrollmentFixtureRequests,
	format: EnrollmentCatalogFormat,
): SelectedStreams => {
	const update = requests.updates.at(-1)
	if (!update) {
		throw new Error("Expected a persisted job update")
	}
	const raw =
		format === "split" ? update.selected_streams_config : update.streams_config
	if (!raw) {
		throw new Error("Expected a saved selected catalog")
	}
	return JSON.parse(raw)
}

const saveSortJob = async (
	page: Page,
	requests: EnrollmentFixtureRequests,
	count: number,
): Promise<void> => {
	await page.getByRole("button", { name: "Finish", exact: true }).click()
	const indexConfirmation = page
		.getByRole("dialog")
		.filter({ hasText: "Indexes might get built" })
		.getByRole("button", { name: "Confirm", exact: true })
	await Promise.race([
		expect.poll(() => requests.updates.length).toBe(count),
		indexConfirmation.waitFor({ state: "visible" }),
	])
	if (await indexConfirmation.isVisible()) {
		await indexConfirmation.click()
	}
	await expect.poll(() => requests.updates.length).toBe(count)
}

const openBulkSort = async (page: Page): Promise<Locator> => {
	await page
		.getByRole("button", { name: "Bulk Configure", exact: true })
		.click()
	const dialog = page.getByRole("dialog")
	for (const name of ["orders", "previously_enrolled"]) {
		await dialog.getByRole("checkbox", { name, exact: true }).check()
	}
	await dialog
		.getByRole("button", { name: "Configure Streams", exact: true })
		.click()
	return dialog
}

for (const format of SORT_FORMATS) {
	test(`${format}: sort default, eligibility, ordered overrides and save/reopen remain distinct`, async ({
		page,
	}) => {
		test.setTimeout(90_000)
		const requests = await installEnrollmentFixture(page, {
			format,
			sortScenario: true,
		})
		await page.goto("/jobs/42/edit")
		const defaultSwitch = page.getByRole("switch", {
			name: "Default created_at sort order",
			exact: true,
		})
		await expect(defaultSwitch).not.toBeChecked()
		await expect(
			page.getByText(
				/Creation only: applies only when a genuinely new native Iceberg table/,
			),
		).toBeVisible()
		await expect(
			page.getByText(/does not sort newly written Parquet batches/),
		).toBeVisible()
		await defaultSwitch.click()
		const editor = page.getByRole("region", {
			name: "Iceberg table sort order",
			exact: true,
		})
		await page.getByRole("button", { name: "orders", exact: true }).click()
		await expect(editor).toContainText(
			"Eligible: inherits created_at ASC NULLS LAST",
		)
		await page
			.getByRole("button", { name: "previously_enrolled", exact: true })
			.click()
		await expect(editor).toContainText("is not a supported temporal primitive")
		await page
			.getByRole("button", { name: "excluded_time", exact: true })
			.click()
		await expect(editor).toContainText("Excluded from selected columns")
		await page
			.getByRole("button", { name: "unselected_history", exact: true })
			.click()
		await expect(editor).toContainText("created_at is missing or ambiguous")

		await page.getByRole("button", { name: "orders", exact: true }).click()
		await chooseSortOption({
			page,
			scope: editor,
			label: "Table sort policy",
			option: "Custom ordered columns",
		})
		await chooseSortOption({
			page,
			scope: editor,
			label: "Sort column 1",
			option: "created_at",
		})
		await editor
			.getByRole("button", { name: "Add sort column", exact: true })
			.click()
		await chooseSortOption({
			page,
			scope: editor,
			label: "Sort column 2",
			option: "priority",
		})
		await chooseSortOption({
			page,
			scope: editor,
			label: "Sort direction 2",
			option: "Descending (DESC)",
		})
		await chooseSortOption({
			page,
			scope: editor,
			label: "Sort null order 2",
			option: "NULLS FIRST",
		})
		await editor
			.getByRole("button", { name: "Move sort key 2 up", exact: true })
			.click()
		await page
			.getByRole("button", { name: "previously_enrolled", exact: true })
			.click()
		await chooseSortOption({
			page,
			scope: editor,
			label: "Table sort policy",
			option: "Explicitly unsorted",
		})
		await page
			.getByRole("button", { name: "known_unselected", exact: true })
			.getByRole("checkbox")
			.check()
		await expect(editor).toContainText(
			"Eligible: inherits created_at ASC NULLS LAST",
		)
		await saveSortJob(page, requests, 1)
		const catalog = savedCatalog(requests, format)
		expect(catalog.default_sort_order).toEqual(CREATED_AT_DEFAULT)
		expect(catalog.selected_extension).toEqual({ kept: true })
		expect(catalog.catalogMetadata).toEqual({ fixture_extension: "kept" })
		expect(
			catalog.selected_streams.public.find(row => row.stream_name === "orders"),
		).toMatchObject({
			sort_order: [
				{ column: "priority", direction: "desc", null_order: "nulls-first" },
				{ column: "created_at", direction: "asc", null_order: "nulls-last" },
			],
			future_extension: { kept: true },
		})
		expect(
			catalog.selected_streams.public.find(
				row => row.stream_name === "previously_enrolled",
			)?.sort_order,
		).toEqual([])
		expect(
			catalog.selected_streams.public.find(
				row => row.stream_name === "known_unselected",
			),
		).not.toHaveProperty("sort_order")
		expect(catalog.selected_streams.unavailable).toEqual([
			{
				stream_name: "missing_from_discovery",
				sort_order: [],
				future_extension: { kept: true },
				...(format === "legacy" ? { append_mode: false } : {}),
			},
			{
				stream_name: "disabled_missing",
				disabled: true,
				sort_order: [],
				future_extension: { kept: true },
				...(format === "legacy" ? { append_mode: false } : {}),
			},
		])
		if (format === "split") {
			const available: Record<string, unknown> = JSON.parse(
				requests.updates[0].available_streams_config ?? "{}",
			)
			expect(available.available_extension).toEqual({ kept: true })
			expect(available).not.toHaveProperty("default_sort_order")
		}
		expect(requests.resyncs).toHaveLength(0)

		await page.goto("/jobs/42/edit")
		await expect(defaultSwitch).toBeChecked()
		await page.getByRole("button", { name: "orders", exact: true }).click()
		await expect(
			editor.locator(".ant-select").filter({
				has: page.getByRole("combobox", {
					name: "Sort column 1",
					exact: true,
				}),
			}),
		).toContainText("priority")
		await defaultSwitch.click()
		await saveSortJob(page, requests, 2)
		const cleared = savedCatalog(requests, format)
		expect(cleared).not.toHaveProperty("default_sort_order")
		expect(
			cleared.selected_streams.public.find(row => row.stream_name === "orders")
				?.sort_order,
		).toHaveLength(2)
		expect(
			cleared.selected_streams.public.find(
				row => row.stream_name === "previously_enrolled",
			)?.sort_order,
		).toEqual([])

		await page.goto("/jobs/42/edit")
		await page.getByRole("button", { name: "orders", exact: true }).click()
		await chooseSortOption({
			page,
			scope: editor,
			label: "Table sort policy",
			option: "Inherit catalog default",
		})
		await saveSortJob(page, requests, 3)
		expect(
			savedCatalog(requests, format).selected_streams.public.find(
				row => row.stream_name === "orders",
			),
		).not.toHaveProperty("sort_order")
		expect(requests.resyncs).toHaveLength(0)
	})

	test(`${format}: bulk unsorted and reset-to-inherit preserve the catalog default`, async ({
		page,
	}) => {
		test.setTimeout(90_000)
		const requests = await installEnrollmentFixture(page, {
			format,
			sortScenario: true,
			defaultSortOrder: CREATED_AT_DEFAULT,
		})
		await page.goto("/jobs/42/edit")
		let dialog = await openBulkSort(page)
		await chooseSortOption({
			page,
			scope: dialog,
			label: "Table sort policy",
			option: "Explicitly unsorted",
		})
		await dialog
			.getByRole("button", { name: "Review Changes", exact: true })
			.click()
		await expect(dialog).toContainText("UNSORTED: ignores the catalog default")
		await dialog
			.getByRole("button", { name: "Apply Changes", exact: true })
			.click()
		await expect(dialog).toHaveCount(0, { timeout: 10_000 })
		await saveSortJob(page, requests, 1)
		for (const name of ["orders", "previously_enrolled"]) {
			expect(
				savedCatalog(requests, format).selected_streams.public.find(
					row => row.stream_name === name,
				)?.sort_order,
			).toEqual([])
		}
		await page.goto("/jobs/42/edit")
		dialog = await openBulkSort(page)
		await dialog
			.getByRole("button", {
				name: "Reset selected tables to inherited sort policy",
				exact: true,
			})
			.click()
		await dialog
			.getByRole("button", { name: "Review Changes", exact: true })
			.click()
		await dialog
			.getByRole("button", { name: "Apply Changes", exact: true })
			.click()
		await expect(dialog).toHaveCount(0, { timeout: 10_000 })
		await saveSortJob(page, requests, 2)
		for (const name of ["orders", "previously_enrolled"]) {
			expect(
				savedCatalog(requests, format).selected_streams.public.find(
					row => row.stream_name === name,
				),
			).not.toHaveProperty("sort_order")
		}
		expect(savedCatalog(requests, format).default_sort_order).toEqual(
			CREATED_AT_DEFAULT,
		)
	})
}

interface InvalidSortCase {
	name: string
	order: SortField[]
	error: string
	excludeCreatedAt?: boolean
	malformedField?: "direction" | "null-order"
}
const INVALID_SORT_CASES: InvalidSortCase[] = [
	{
		name: "absent",
		order: [{ column: "missing", direction: "asc", null_order: "nulls-last" }],
		error: "absent from the discovered schema",
	},
	{
		name: "complex",
		order: [{ column: "payload", direction: "asc", null_order: "nulls-last" }],
		error: "Requires one supported primitive type",
	},
	{
		name: "excluded",
		order: CREATED_AT_DEFAULT,
		excludeCreatedAt: true,
		error: "Excluded from selected columns",
	},
	{
		name: "duplicate",
		order: [
			{ column: "id", direction: "asc", null_order: "nulls-last" },
			{ column: "id", direction: "desc", null_order: "nulls-first" },
		],
		error: "Duplicate sort column",
	},
	{
		name: "generated metadata",
		order: [
			{
				column: "_olake_sort_fixture",
				direction: "asc",
				null_order: "nulls-last",
			},
		],
		error: "OLake metadata column",
	},
	{
		name: "direction",
		order: [],
		malformedField: "direction",
		error: "direction must be asc or desc",
	},
	{
		name: "null ordering",
		order: [],
		malformedField: "null-order",
		error: "null ordering must be nulls-first or nulls-last",
	},
]
for (const invalid of INVALID_SORT_CASES) {
	test(`explicit ${invalid.name} sort order shows validation and cannot save until repaired`, async ({
		page,
	}) => {
		const requests = await installEnrollmentFixture(page, {
			format: "split",
			sortScenario: true,
			sortOrder: invalid.order,
			excludeCreatedAt: invalid.excludeCreatedAt,
			malformedSortField: invalid.malformedField,
		})
		await page.goto("/jobs/42/edit")
		await page.getByRole("button", { name: "orders", exact: true }).click()
		const editor = page.getByRole("region", {
			name: "Iceberg table sort order",
			exact: true,
		})
		await expect(editor).toContainText(invalid.error)
		await page.getByRole("button", { name: "Finish", exact: true }).click()
		await expect(page.locator(".ant-message-notice-content")).toContainText(
			invalid.error,
		)
		expect(requests.updates).toHaveLength(0)
		await chooseSortOption({
			page,
			scope: editor,
			label: "Table sort policy",
			option: "Explicitly unsorted",
		})
		await saveSortJob(page, requests, 1)
		expect(
			savedCatalog(requests, "split").selected_streams.public.find(
				row => row.stream_name === "orders",
			)?.sort_order,
		).toEqual([])
	})
}

test("normalization disables custom controls without erasing the saved order", async ({
	page,
}) => {
	await installEnrollmentFixture(page, {
		format: "split",
		sortScenario: true,
		sortOrder: CREATED_AT_DEFAULT,
	})
	await page.goto("/jobs/42/edit")
	await page.getByRole("button", { name: "orders", exact: true }).click()
	const editor = page.getByRole("region", {
		name: "Iceberg table sort order",
		exact: true,
	})
	await page.getByRole("switch", { name: "Normalization", exact: true }).click()
	await expect(
		editor.getByRole("combobox", { name: "Table sort policy", exact: true }),
	).toBeDisabled()
	await expect(editor).toContainText("Saved settings are preserved")
	await page.getByRole("switch", { name: "Normalization", exact: true }).click()
	await expect(
		editor.getByRole("combobox", { name: "Table sort policy", exact: true }),
	).toBeEnabled()
	await expect(editor).toContainText("created_at")
})

test("missing advertised capability blocks sort edits despite an unchanged connector version", async ({
	page,
}) => {
	const requests = await installEnrollmentFixture(page, {
		format: "split",
		sortScenario: true,
		unsupportedSortConnector: true,
		refreshedSortSupport: true,
		defaultSortOrder: CREATED_AT_DEFAULT,
		sortOrder: CREATED_AT_DEFAULT,
	})
	await page.goto("/jobs/42/edit")
	await expect(
		page.getByRole("switch", {
			name: "Default created_at sort order",
			exact: true,
		}),
	).toBeDisabled()
	await page.getByRole("button", { name: "orders", exact: true }).click()
	const editor = page.getByRole("region", {
		name: "Iceberg table sort order",
		exact: true,
	})
	await expect(
		editor.getByRole("combobox", { name: "Table sort policy", exact: true }),
	).toBeDisabled()
	await expect(editor).toContainText("an older connector may ignore it")
	await page.getByRole("button", { name: "Finish", exact: true }).click()
	await expect(page.locator(".ant-message-notice-content")).toContainText(
		"matching sort-aware source connector build",
	)
	expect(requests.updates).toHaveLength(0)
	await page
		.getByRole("button", { name: "Refresh streams", exact: true })
		.click()
	await page
		.getByRole("dialog")
		.getByRole("button", { name: "Refresh streams", exact: true })
		.click()
	await expect(
		page.getByRole("switch", {
			name: "Default created_at sort order",
			exact: true,
		}),
	).toBeEnabled()
	await expect(
		editor.getByRole("combobox", { name: "Table sort policy", exact: true }),
	).toBeEnabled()
	await saveSortJob(page, requests, 1)
	expect(savedCatalog(requests, "split").default_sort_order).toEqual(
		CREATED_AT_DEFAULT,
	)
	expect(
		savedCatalog(requests, "split").selected_streams.public.find(
			row => row.stream_name === "orders",
		)?.sort_order,
	).toEqual(CREATED_AT_DEFAULT)
})

test("non-Iceberg destinations do not offer sorting and retain saved settings visibly", async ({
	page,
}) => {
	const requests = await installEnrollmentFixture(page, {
		format: "split",
		sortScenario: true,
		destinationType: "parquet",
		defaultSortOrder: CREATED_AT_DEFAULT,
		sortOrder: CREATED_AT_DEFAULT,
	})
	await page.goto("/jobs/42/edit")
	await expect(
		page.getByRole("switch", {
			name: "Default created_at sort order",
			exact: true,
		}),
	).toHaveCount(0)
	await expect(
		page.getByText(
			"Saved Iceberg sort policy is preserved. Sorting controls are unavailable for this destination.",
		),
	).toBeVisible()
	await page.getByRole("button", { name: "orders", exact: true }).click()
	const editor = page.getByRole("region", {
		name: "Iceberg table sort order",
		exact: true,
	})
	await expect(
		editor.getByRole("combobox", { name: "Table sort policy", exact: true }),
	).toBeDisabled()
	await expect(editor).toContainText("created_at")
	await editor
		.getByRole("button", { name: "Clear table sort override", exact: true })
		.click()
	await page
		.getByRole("button", { name: "Clear default sort policy", exact: true })
		.click()
	await saveSortJob(page, requests, 1)
	expect(savedCatalog(requests, "split")).not.toHaveProperty(
		"default_sort_order",
	)
	expect(
		savedCatalog(requests, "split").selected_streams.public.find(
			row => row.stream_name === "orders",
		),
	).not.toHaveProperty("sort_order")
})

test("mobile sort editor keeps ordered controls within its panel", async ({
	page,
}) => {
	await page.setViewportSize({ width: 390, height: 844 })
	await installEnrollmentFixture(page, { format: "split", sortScenario: true })
	await page.goto("/jobs/42/edit")
	await page
		.getByRole("button", { name: "Toggle sidebar", exact: true })
		.click()
	await page
		.getByRole("switch", { name: "Default created_at sort order", exact: true })
		.click()
	await page.getByRole("button", { name: "orders", exact: true }).click()
	const editor = page.getByRole("region", {
		name: "Iceberg table sort order",
		exact: true,
	})
	await chooseSortOption({
		page,
		scope: editor,
		label: "Table sort policy",
		option: "Custom ordered columns",
	})
	await editor
		.getByRole("button", { name: "Add sort column", exact: true })
		.click()
	await expect(
		editor
			.locator(".ant-select")
			.filter({
				has: page.getByRole("combobox", {
					name: "Sort direction 2",
					exact: true,
				}),
			})
			.locator(".ant-select-selector"),
	).toBeVisible()
	const bounds = await editor.boundingBox()
	expect(bounds?.width).toBeLessThanOrEqual(390)
	expect(
		await editor.evaluate(
			(element): boolean => element.scrollWidth <= element.clientWidth,
		),
	).toBe(true)
})

test("normalized source names support default eligibility and saved explicit aliases without rewriting metadata", async ({
	page,
}) => {
	const aliasOrder: SortField[] = [
		{ column: "Created-At", direction: "desc", null_order: "nulls-first" },
	]
	const requests = await installEnrollmentFixture(page, {
		format: "split",
		sortScenario: true,
		normalizedSortNames: true,
		defaultSortOrder: CREATED_AT_DEFAULT,
		sortOrder: aliasOrder,
	})
	await page.goto("/jobs/42/edit")
	await page.getByRole("button", { name: "orders", exact: true }).click()
	const editor = page.getByRole("region", {
		name: "Iceberg table sort order",
		exact: true,
	})
	await expect(editor).toContainText("Created-At (source: Created At)")
	await expect(editor.getByRole("alert")).toHaveCount(0)
	await saveSortJob(page, requests, 1)
	expect(
		savedCatalog(requests, "split").selected_streams.public.find(
			row => row.stream_name === "orders",
		)?.sort_order,
	).toEqual(aliasOrder)
	await page.goto("/jobs/42/edit")
	await page.getByRole("button", { name: "orders", exact: true }).click()
	await chooseSortOption({
		page,
		scope: editor,
		label: "Table sort policy",
		option: "Inherit catalog default",
	})
	await expect(editor).toContainText(
		"Eligible: inherits Created At ASC NULLS LAST",
	)
})

test("source-name preservation disables normalized default fallback", async ({
	page,
}) => {
	await installEnrollmentFixture(page, {
		format: "split",
		sortScenario: true,
		normalizedSortNames: true,
		preserveSortSourceNames: true,
		defaultSortOrder: CREATED_AT_DEFAULT,
	})
	await page.goto("/jobs/42/edit")
	await page.getByRole("button", { name: "orders", exact: true }).click()
	await expect(
		page.getByRole("region", { name: "Iceberg table sort order", exact: true }),
	).toContainText("created_at is missing or ambiguous")
})

test("ambiguous normalized columns are rejected rather than choosing a destination field", async ({
	page,
}) => {
	const requests = await installEnrollmentFixture(page, {
		format: "split",
		sortScenario: true,
		normalizedSortNames: true,
		ambiguousSortNames: true,
		sortOrder: [
			{ column: "Created At", direction: "asc", null_order: "nulls-last" },
		],
	})
	await page.goto("/jobs/42/edit")
	await page.getByRole("button", { name: "orders", exact: true }).click()
	await expect(
		page.getByRole("region", { name: "Iceberg table sort order", exact: true }),
	).toContainText("Ambiguous normalized source column")
	await page.getByRole("button", { name: "Finish", exact: true }).click()
	await expect(page.locator(".ant-message-notice-content")).toContainText(
		"Ambiguous normalized source column",
	)
	expect(requests.updates).toHaveLength(0)
})

test("unsupported connector policies can be explicitly removed without rediscovery", async ({
	page,
}) => {
	const requests = await installEnrollmentFixture(page, {
		format: "split",
		sortScenario: true,
		unsupportedSortConnector: true,
		defaultSortOrder: CREATED_AT_DEFAULT,
		sortOrder: CREATED_AT_DEFAULT,
	})
	await page.goto("/jobs/42/edit")
	await page.getByRole("button", { name: "orders", exact: true }).click()
	await page
		.getByRole("region", { name: "Iceberg table sort order", exact: true })
		.getByRole("button", { name: "Clear table sort override", exact: true })
		.click()
	await page
		.getByRole("button", { name: "Clear default sort policy", exact: true })
		.click()
	await saveSortJob(page, requests, 1)
	const catalog = savedCatalog(requests, "split")
	expect(catalog).not.toHaveProperty("default_sort_order")
	expect(
		catalog.selected_streams.public.find(row => row.stream_name === "orders"),
	).not.toHaveProperty("sort_order")
	expect(requests.resyncs).toHaveLength(0)
})
