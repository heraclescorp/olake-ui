import { strict as assert } from "node:assert"
import { test } from "node:test"

import { UpsertType } from "@/modules/ingestion/common/types"
import type { AutoTableEnrollmentPolicy } from "../types"
import {
	buildEnrollmentPolicy,
	enrollmentCatalogFromStreams,
	enrollmentDraftFromPolicy,
} from "./autoTableEnrollment"

const policy: AutoTableEnrollmentPolicy = {
	enabled: true,
	schemas: [
		{ namespace: "public", destination_database: "warehouse", extension: 42 },
	],
	defaults: {
		normalization: true,
		append_mode: false,
		update_type: UpsertType.POSITIONAL,
		sync_new_columns: true,
		merge_strategy: "copy-on-write",
		extension: { retained: true },
	},
	extension: "retained",
}
const catalog = {
	namespaces: ["public"],
	schemas: [{ namespace: "public", destination_database: "warehouse" }],
	supportsCopyOnWrite: true,
}
const capabilities = {
	availableUpdateTypes: [UpsertType.DELETION_VECTOR],
	error: "MOR unavailable",
}
const sourceConfig = { update_method: { toast_value_source: "source" } }
const build = (draft = enrollmentDraftFromPolicy(policy), overrides = {}) =>
	buildEnrollmentPolicy({
		draft,
		supported: true,
		catalog,
		capabilities,
		sourceConfig,
		...overrides,
	})

test("COW saves and reopens without changing inactive MOR preference or extensions", () => {
	const result = build()
	assert.equal(result.valid, true)
	if (!result.valid) return
	assert.deepEqual(result.policy, policy)
	assert.deepEqual(
		enrollmentDraftFromPolicy(result.policy),
		enrollmentDraftFromPolicy(policy),
	)
	const legacy = enrollmentDraftFromPolicy({
		...policy,
		defaults: { ...policy.defaults, merge_strategy: undefined },
	})
	assert.equal(legacy.defaults.merge_strategy, undefined)
	assert.equal(build(legacy).valid, false) // MOR still checks reader capabilities.
})

test("COW requires capability, CDC, source TOAST, normalization, upsert and valid delete type", () => {
	for (const overrides of [
		{ catalog: { ...catalog, supportsCopyOnWrite: false } },
		{ sourceConfig: {} },
		{ sourceConfig: { update_method: { toast_value_source: "destination" } } },
		{
			draft: {
				...enrollmentDraftFromPolicy(policy),
				defaults: { ...policy.defaults, normalization: false },
			},
		},
		{
			draft: {
				...enrollmentDraftFromPolicy(policy),
				defaults: { ...policy.defaults, append_mode: true },
			},
		},
		{
			draft: {
				...enrollmentDraftFromPolicy(policy),
				defaults: { ...policy.defaults, update_type: "invalid" },
			},
		},
		{
			draft: {
				...enrollmentDraftFromPolicy(policy),
				defaults: { ...policy.defaults, merge_strategy: null },
			},
		},
	]) {
		assert.equal(
			buildEnrollmentPolicy({
				draft: enrollmentDraftFromPolicy(policy),
				supported: true,
				catalog,
				capabilities,
				sourceConfig,
				...overrides,
			} as Parameters<typeof buildEnrollmentPolicy>[0]).valid,
			false,
		)
	}
	const disabled = { ...enrollmentDraftFromPolicy(policy), enabled: false }
	assert.equal(
		build(disabled, {
			catalog: { ...catalog, supportsCopyOnWrite: false },
			sourceConfig: {},
		}).valid,
		true,
	)
})

test("capability evidence does not require existing tables to have a primary key", () => {
	const stream = {
		stream: {
			name: "no_key",
			namespace: "public",
			json_schema: {},
			type_schema: { properties: {} },
			supports_iceberg_copy_on_write: true,
			destination_database: "warehouse",
			source_defined_primary_key: [],
			default_stream_properties: { normalization: true, append_mode: false },
		},
	}
	const found = enrollmentCatalogFromStreams({
		streams: [stream],
		selected_streams: {},
	})
	assert.equal(found.supportsCopyOnWrite, true)
	assert.equal(
		build(enrollmentDraftFromPolicy(policy), { catalog: found }).valid,
		true,
	)
	stream.stream.supports_iceberg_copy_on_write = false
	assert.equal(
		enrollmentCatalogFromStreams({ streams: [stream], selected_streams: {} })
			.supportsCopyOnWrite,
		false,
	)
})
