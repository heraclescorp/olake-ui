import { test, expect } from "@playwright/test"

import { installEnrollmentFixture } from "../fixtures/auto-table-enrollment.fixture"

for (const format of ["legacy", "split"] as const) {
	for (const allDV of [true, false]) {
		test(`${format}: bulk ${allDV ? "saved DV" : "mixed"} to equality preserves other settings`, async ({
			page,
		}) => {
			const r = await installEnrollmentFixture(page, {
				format,
				bulkAllDV: allDV,
			})
			await page.goto("/jobs/42/edit")
			await page
				.getByRole("button", { name: "Bulk Configure", exact: true })
				.click()
			const d = page.getByRole("dialog")
			for (const name of ["orders", "previously_enrolled"])
				await d.getByRole("checkbox", { name, exact: true }).check()
			await d
				.getByRole("button", { name: "Configure Streams", exact: true })
				.click()
			await d.getByText("Ingestion Settings", { exact: true }).click()
			if (allDV) {
				await expect(
					d.getByRole("radio", { name: "Upsert", exact: true }),
				).toBeChecked()
				await expect(
					d.locator(".ant-select").filter({
						has: page.getByRole("combobox", { name: "Upsert Type" }),
					}),
				).toContainText("Deletion Vector")
			} else {
				await expect(
					d.getByText("Mixed / Keep existing ingestion modes", { exact: true }),
				).toBeVisible()
				await expect(
					d.getByRole("radio", { name: "Upsert", exact: true }),
				).not.toBeChecked()
				await expect(
					d.getByRole("radio", { name: "Append", exact: true }),
				).not.toBeChecked()
			}
			const select = page.getByRole("combobox", { name: "Upsert Type" })
			await d
				.locator(".ant-select")
				.filter({ has: select })
				.locator(".ant-select-selector")
				.click()
			await page
				.locator(".ant-select-dropdown:visible")
				.getByText("Equality Deletes", { exact: true })
				.click()
			await d
				.getByRole("button", {
					name: "Keep existing write settings",
					exact: true,
				})
				.click()
			await expect(
				d.getByRole("button", { name: "Review Changes", exact: true }),
			).toBeDisabled()
			await d
				.locator(".ant-select")
				.filter({ has: page.getByRole("combobox", { name: "Upsert Type" }) })
				.locator(".ant-select-selector")
				.click()
			await page
				.locator(".ant-select-dropdown:visible")
				.getByText("Equality Deletes", { exact: true })
				.click()
			await expect(
				d.getByRole("radio", { name: "Upsert", exact: true }),
			).toBeChecked()
			await d
				.getByRole("button", { name: "Review Changes", exact: true })
				.click()
			await expect(d).toContainText("Equality Deletes")
			await expect(d).toContainText("orders")
			expect(r.updates).toHaveLength(0)
			await d
				.getByRole("button", { name: "Apply Changes", exact: true })
				.click()
			await expect(d).toHaveCount(0, { timeout: 10000 })
			expect(r.updates).toHaveLength(0)
			for (const name of ["orders", "previously_enrolled"]) {
				await page.getByRole("button", { name, exact: true }).click()
				await expect(
					page.getByRole("radio", { name: "Upsert", exact: true }),
				).toBeChecked()
			}
			await page.getByRole("button", { name: "Finish", exact: true }).click()
			// Existing indexed→equality should not require clearing data in the fixture.
			await expect.poll(() => r.updates.length).toBe(1)
			const u = r.updates[0]
			const catalog = JSON.parse(
				(format === "split" ? u.selected_streams_config : u.streams_config)!,
			)
			for (const name of ["orders", "previously_enrolled"]) {
				const row = catalog.selected_streams.public.find(
					(s: { stream_name: string }) => s.stream_name === name,
				)
				expect(row).toMatchObject({
					append_mode: false,
					update_type: "eq",
					sync_mode: "cdc",
					destination_table:
						name === "orders" ? "orders_selected" : "previously_enrolled_saved",
				})
				expect(row.normalization).toBe(name !== "orders")
				expect(row.selected_columns.sync_new_columns).toBe(name !== "orders")
			}
			expect(
				catalog.selected_streams.public.some(
					(s: { stream_name: string }) => s.stream_name === "known_unselected",
				),
			).toBe(false)
			expect(r.resyncs).toHaveLength(0)
		})
	}
}

for (const format of ["legacy", "split"] as const) {
	test(`${format}: reverting ingestion mode to saved Append is not a change`, async ({
		page,
	}) => {
		const r = await installEnrollmentFixture(page, { format })
		await page.goto("/jobs/42/edit")
		await page
			.getByRole("button", { name: "Bulk Configure", exact: true })
			.click()
		const d = page.getByRole("dialog")
		await d.getByRole("checkbox", { name: "orders", exact: true }).check()
		await d
			.getByRole("button", { name: "Configure Streams", exact: true })
			.click()
		await d.getByText("Ingestion Settings", { exact: true }).click()
		await expect(
			d.getByRole("radio", { name: "Append", exact: true }),
		).toBeChecked()
		await d.getByRole("radio", { name: "Upsert", exact: true }).check()
		await expect(
			d.getByRole("button", { name: "Review Changes", exact: true }),
		).toBeEnabled()
		await d.getByRole("radio", { name: "Append", exact: true }).check()
		await expect(
			d.getByRole("button", { name: "Review Changes", exact: true }),
		).toBeDisabled()
		expect(r.updates).toHaveLength(0)
		// A real unrelated change must not resurrect ingestion mode in the summary.
		await d.getByRole("switch", { name: "Normalization", exact: true }).click()
		await d.getByRole("button", { name: "Review Changes", exact: true }).click()
		await expect(d.getByText("Ingestion Mode:", { exact: true })).toHaveCount(0)
		await d.getByRole("button", { name: "Apply Changes", exact: true }).click()
		await expect(d).toHaveCount(0, { timeout: 10000 })
		await page.getByRole("button", { name: "Finish", exact: true }).click()
		await expect.poll(() => r.updates.length).toBe(1)
		const u = r.updates[0]
		const c = JSON.parse(
			(format === "split" ? u.selected_streams_config : u.streams_config)!,
		)
		expect(
			c.selected_streams.public.find(
				(s: { stream_name: string }) => s.stream_name === "orders",
			),
		).toMatchObject({ append_mode: true, normalization: true })
	})
}
