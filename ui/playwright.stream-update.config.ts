import { defineConfig, devices } from "@playwright/test"

// Runs the real frontend with intercepted API fixtures; no backend, credentials,
// Docker daemon access, or production connections are required.
export default defineConfig({
	testDir: "./tests/flows",
	testMatch: ["paused-stream-update.spec.ts", "bulk-write-update.spec.ts"],
	timeout: 45_000,
	expect: { timeout: 15_000 },
	use: {
		...devices["Desktop Chrome"],
		baseURL: "http://localhost:8000",
		launchOptions: {
			executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
		},
	},
	webServer: {
		command: "pnpm dev --host 127.0.0.1 --port 8000 --strictPort",
		url: "http://localhost:8000",
		reuseExistingServer: !process.env.CI,
	},
})
