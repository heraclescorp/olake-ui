import { test, expect } from "@playwright/test"

import { installEnrollmentFixture } from "../fixtures/auto-table-enrollment.fixture"

for (const format of ["legacy", "split"] as const) {
	test(`${format}: force LSN requires preview and warnings, sends no stream list`, async ({
		page,
	}) => {
		const r = await installEnrollmentFixture(page, { format })
		r.job.activate = false
		const calls: { method: string; body: Record<string, unknown> }[] = []
		await page.route(/\/jobs\/42\/force-lsn(?:\/preview)?$/, async route => {
			const body = route.request().postDataJSON()
			calls.push({ method: route.request().method(), body })
			const data =
				route.request().method() === "PUT"
					? { backup_id: 17, lsn: "761/F6ADE70" }
					: {
							lsn: "761/F6ADE70",
							current_lsn: "0/123",
							streams: ["public.orders", "public.previously_enrolled"],
							revision: "checkpoint-revision",
							state: {
								type: "GLOBAL",
								version: 8,
								global: {
									state: { lsn: "761/F6ADE70" },
									streams: ["public.orders", "public.previously_enrolled"],
								},
							},
						}
			await route.fulfill({ json: { success: true, data } })
		})
		await page.goto("/jobs")
		await page.getByRole("tab", { name: /Inactive/ }).click()
		await page
			.getByRole("tabpanel", { name: "Inactive jobs", exact: true })
			.getByTestId(`job-${r.job.name}`)
			.click()
		await page
			.getByRole("menuitem", { name: "Force set CDC LSN", exact: true })
			.click()
		const d = page.getByRole("dialog")
		await expect(d).toContainText("permanently skip changes")
		await expect(d).toContainText("cannot rewind a slot")
		const preview = d.getByRole("button", {
			name: "Preview checkpoint",
			exact: true,
		})
		await d.getByLabel("New CDC LSN", { exact: true }).fill("garbage")
		await expect(preview).toBeDisabled()
		await d.getByLabel("New CDC LSN", { exact: true }).fill("0/0")
		await expect(preview).toBeDisabled()
		await d.getByLabel("New CDC LSN", { exact: true }).fill("761/f6ade70")
		await preview.click()
		await expect(d).toContainText("2 selected CDC tables")
		await expect(d).toContainText("0/123")
		await expect(d.locator(".ant-btn-loading")).toHaveCount(0)
		const force = d.getByRole("button", { name: /Force set LSN$/ })
		await expect(force).toBeDisabled()
		await d.getByRole("checkbox").check()
		await d
			.getByLabel("Type FORCE 761/F6ADE70 to confirm", { exact: true })
			.fill("FORCE wrong")
		await expect(force).toBeDisabled()
		await d
			.getByLabel("Type FORCE 761/F6ADE70 to confirm", { exact: true })
			.fill("FORCE 761/F6ADE70")
		await force.click()
		await expect(d).toContainText("database backup #17")
		expect(calls).toEqual([
			{ method: "POST", body: { lsn: "761/f6ade70" } },
			{
				method: "PUT",
				body: {
					lsn: "761/F6ADE70",
					revision: "checkpoint-revision",
					confirm_baseline: true,
					confirmation: "FORCE 761/F6ADE70",
				},
			},
		])
		expect(r.updates).toHaveLength(0)
		expect(r.resyncs).toHaveLength(0)
		expect(r.job.activate).toBe(false)
	})
}

test("force LSN disabled on active and running jobs", async ({ page }) => {
	const r = await installEnrollmentFixture(page)
	await page.goto("/jobs")
	await page.getByTestId(`job-${r.job.name}`).click()
	await expect(
		page.getByRole("menuitem", { name: "Force set CDC LSN", exact: true }),
	).toHaveAttribute("aria-disabled", "true")
})

test("changing LSN invalidates preview; stale commit never shows success", async ({
	page,
}) => {
	const r = await installEnrollmentFixture(page)
	r.job.activate = false
	await page.route(/\/jobs\/42\/force-lsn(?:\/preview)?$/, async route => {
		if (route.request().method() === "PUT") {
			await route.fulfill({
				status: 409,
				json: { message: "Checkpoint changed; preview again" },
			})
			return
		}
		await route.fulfill({
			json: {
				data: {
					lsn: "761/F6ADE70",
					current_lsn: "",
					streams: ["public.orders"],
					revision: "old",
					state: {},
				},
			},
		})
	})
	await page.goto("/jobs")
	await page.getByRole("tab", { name: /Inactive/ }).click()
	await page
		.getByRole("tabpanel", { name: "Inactive jobs", exact: true })
		.getByTestId(`job-${r.job.name}`)
		.click()
	await page
		.getByRole("menuitem", { name: "Force set CDC LSN", exact: true })
		.click()
	const d = page.getByRole("dialog")
	await d.getByLabel("New CDC LSN", { exact: true }).fill("761/F6ADE70")
	await d
		.getByRole("button", { name: "Preview checkpoint", exact: true })
		.click()
	await d.getByLabel("New CDC LSN", { exact: true }).fill("761/F6ADE71")
	await expect(d.getByRole("checkbox")).toHaveCount(0)
	await d.getByLabel("New CDC LSN", { exact: true }).fill("761/F6ADE70")
	await d
		.getByRole("button", { name: "Preview checkpoint", exact: true })
		.click()
	await d.getByRole("checkbox").check()
	await d
		.getByLabel("Type FORCE 761/F6ADE70 to confirm", { exact: true })
		.fill("FORCE 761/F6ADE70")
	await d.getByRole("button", { name: /Force set LSN$/ }).click()
	await expect(d).toContainText("Unable to update checkpoint")
	await expect(
		d.getByRole("button", { name: "Preview checkpoint", exact: true }),
	).toBeVisible()
})
