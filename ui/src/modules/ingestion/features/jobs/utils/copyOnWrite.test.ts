import { strict as assert } from "node:assert"
import { test } from "node:test"

import {
	CatalogFormat,
	SyncMode,
	UpsertType,
	type SelectedStream,
	type StreamData,
	type StreamsDataStructure,
} from "@/modules/ingestion/common/types"

import { useStreamSelectionStore } from "../stores/streamSelectionStore"
import {
	COW_CAPABILITY_WARNING,
	getCOWEligibilityError,
	validateCatalogCOW,
	withDefaultCopyOnWrite,
} from "./copyOnWrite"
import {
	buildCatalogPayload,
	getStreamsDataFromSourceStreamsResponse,
	hasIndexedUpsertStream,
	parseDiscoverResponse,
	withIndexRequired,
	willBuildIndex,
} from "./streams"

const stream: StreamData = {
	stream: {
		name: "orders",
		namespace: "public",
		json_schema: {},
		type_schema: {
			properties: { id: { type: ["integer"] }, body: { type: ["string"] } },
		},
		source_defined_primary_key: ["id"],
		sync_mode: SyncMode.CDC,
		supports_iceberg_copy_on_write: true,
		default_stream_properties: {
			normalization: true,
			append_mode: false,
			update_type: UpsertType.DELETION_VECTOR,
		},
	},
}
const selected: SelectedStream = {
	stream_name: "orders",
	partition_regex: "",
	normalization: true,
	append_mode: false,
	sync_mode: SyncMode.CDC,
	update_type: UpsertType.DELETION_VECTOR,
	merge_strategy: "copy-on-write",
	selected_columns: { columns: ["id", "body"], sync_new_columns: true },
	extension: { keep: "unchanged" },
}
const sourceConfig = {
	update_method: {
		replication_slot: "saved_slot",
		publication: "saved_publication",
		toast_value_source: "source",
	},
}
const destinationConfig = { type: "ICEBERG", writer: { catalog_type: "rest" } }

function catalog(): StreamsDataStructure {
	return {
		streams: [structuredClone(stream)],
		selected_streams: { public: [structuredClone(selected)] },
		default_sort_order: [],
		catalogMetadata: {
			available: { capability_extension: true },
			selected: { root_extension: { keep: true } },
		},
	}
}

test("capability, normalization, source identity and hydration gate active COW", () => {
	assert.equal(
		getCOWEligibilityError({
			stream,
			selected,
			sourceType: "postgres",
			destinationType: "iceberg",
			sourceConfig,
			destinationConfig,
		}),
		null,
	)
	assert.equal(
		getCOWEligibilityError({
			stream,
			selected: {
				...selected,
				selected_columns: { columns: ["body"], sync_new_columns: true },
			},
			sourceType: "postgres",
			destinationType: "iceberg",
		}),
		null,
	)
	for (const connection of [
		{ sourceType: "mysql", destinationType: "iceberg" },
		{ sourceType: "postgres", destinationType: "s3" },
	]) {
		assert.notEqual(
			getCOWEligibilityError({ stream, selected, ...connection }),
			null,
		)
	}
	const withoutPK = structuredClone(stream)
	withoutPK.stream.source_defined_primary_key = []
	assert.match(
		getCOWEligibilityError({
			stream: withoutPK,
			selected,
			sourceType: "postgres",
			destinationType: "iceberg",
		}) ?? "",
		/source primary key/,
	)
	const unsupported = structuredClone(stream)
	delete unsupported.stream.supports_iceberg_copy_on_write
	assert.equal(
		getCOWEligibilityError({
			stream: unsupported,
			selected,
			sourceType: "postgres",
			destinationType: "iceberg",
		}),
		COW_CAPABILITY_WARNING,
	)
	for (const change of [
		{ normalization: false },
		{ sync_mode: SyncMode.FULL_REFRESH },
	]) {
		assert.notEqual(
			getCOWEligibilityError({
				stream,
				selected: { ...selected, ...change },
				sourceType: "postgres",
				destinationType: "iceberg",
			}),
			null,
		)
	}
	assert.match(
		getCOWEligibilityError({
			stream,
			selected,
			sourceType: "postgres",
			destinationType: "iceberg",
			sourceConfig: {
				update_method: {
					...sourceConfig.update_method,
					toast_value_source: "destination",
				},
			},
		}) ?? "",
		/source TOAST hydration/,
	)
	// Identity is the existing string _olake_id, not a second source-PK encoding.
	for (const type of [["integer", "null"], ["number"], ["object"]]) {
		const canonicalIdentity = structuredClone(stream)
		canonicalIdentity.stream.type_schema!.properties.id.type = type
		assert.equal(
			getCOWEligibilityError({
				stream: canonicalIdentity,
				selected,
				sourceType: "postgres",
				destinationType: "iceberg",
			}),
			null,
		)
	}
})

test("COW never requests a row index while mixed MOR still does", () => {
	const data = catalog()
	assert.equal(hasIndexedUpsertStream(data), false)
	assert.equal(willBuildIndex(data), false)
	assert.equal(
		withIndexRequired({ index_required: true }, data).index_required,
		false,
	)
	data.selected_streams.public.push({
		...selected,
		stream_name: "mor",
		merge_strategy: "merge-on-read",
	})
	assert.equal(hasIndexedUpsertStream(data), true)
	assert.equal(withIndexRequired({}, data).index_required, true)
	data.selected_streams.public[1].append_mode = true
	assert.equal(hasIndexedUpsertStream(data), false)
})

test("legacy and split discovery/save preserve COW policy, extensions and empty sort", () => {
	for (const format of [CatalogFormat.LEGACY, CatalogFormat.V2]) {
		const data = catalog()
		const payload = buildCatalogPayload(data, format)
		const saved = JSON.parse(
			"streams_config" in payload
				? payload.streams_config
				: payload.selected_streams_config,
		)
		assert.deepEqual(
			saved.selected_streams.public[0].extension,
			selected.extension,
		)
		assert.equal(
			saved.selected_streams.public[0].merge_strategy,
			"copy-on-write",
		)
		assert.equal(saved.selected_streams.public[0].update_type, "dv")
		assert.deepEqual(saved.default_sort_order, [])
		assert.deepEqual(saved.root_extension, { keep: true })
		const discovered = parseDiscoverResponse(
			"streams_config" in payload
				? {
						streams_config: JSON.parse(payload.streams_config),
						available_streams: null,
						selected_streams: null,
					}
				: {
						streams_config: null,
						available_streams: JSON.parse(payload.available_streams_config),
						selected_streams: saved,
					},
		)
		const reopened = getStreamsDataFromSourceStreamsResponse(
			discovered.response,
			"iceberg",
			"postgres",
			"v1.1.1",
			undefined,
			undefined,
			true,
		)
		assert.deepEqual(
			reopened.selected_streams.public[0].extension,
			selected.extension,
		)
		assert.equal(
			reopened.selected_streams.public[0].merge_strategy,
			"copy-on-write",
		)
		assert.equal(
			reopened.streams[0].stream.supports_iceberg_copy_on_write,
			true,
		)
	}
})

test("single and bulk COW updates preserve MOR preferences and catalog extensions", () => {
	const store = useStreamSelectionStore
	store.getState().reset()
	const data = catalog()
	data.selected_streams.public.push({
		...selected,
		stream_name: "mor",
		merge_strategy: "merge-on-read",
		update_type: UpsertType.POSITIONAL,
	})
	store.getState().initializeFromDiscovery(data, CatalogFormat.V2)
	const id = { namespace: "public", streamName: "orders" }
	store.getState().updateMergeStrategy(id, "merge-on-read")
	assert.equal(
		store.getState().streamsData!.selected_streams.public[0].update_type,
		"dv",
	)
	store.getState().updateMergeStrategy(id, "copy-on-write")
	store.getState().bulkUpdateStreams([id], {
		mergeStrategy: "copy-on-write",
		upsertType: UpsertType.EQUALITY,
	})
	const saved = store.getState().streamsData!.selected_streams.public[0]
	assert.equal(saved.update_type, "dv")
	assert.deepEqual(
		store.getState().streamsData!.catalogMetadata,
		catalog().catalogMetadata,
	)
	assert.deepEqual(saved.extension, selected.extension)
	store
		.getState()
		.bulkUpdateStreams([id, { namespace: "public", streamName: "mor" }], {
			upsertType: UpsertType.EQUALITY,
		})
	const mixed = store.getState().streamsData!.selected_streams.public
	assert.equal(mixed[0].merge_strategy, "copy-on-write")
	assert.equal(mixed[0].update_type, "dv")
	assert.equal(mixed[1].merge_strategy, "merge-on-read")
	assert.equal(mixed[1].update_type, "eq")
	store.getState().updateMergeStrategy(id, "merge-on-read")
	assert.equal(
		store.getState().streamsData!.selected_streams.public[0].update_type,
		"dv",
	)
	store.getState().reset()
})

test("older discovery retains unsupported saved choice until explicit disable; append is inactive", () => {
	const data = catalog()
	delete data.streams[0].stream.supports_iceberg_copy_on_write
	assert.match(
		validateCatalogCOW(data, "postgres", "iceberg") ?? "",
		/supports_iceberg_copy_on_write/,
	)
	data.selected_streams.public[0].append_mode = true
	assert.equal(validateCatalogCOW(data, "postgres", "iceberg"), null)
	data.selected_streams.public[0].append_mode = false
	data.selected_streams.public[0].merge_strategy = "merge-on-read"
	assert.equal(validateCatalogCOW(data, "postgres", "iceberg"), null)
})

test("new eligible jobs default to COW without reinterpreting existing or explicit policies", () => {
	const data = catalog()
	delete data.selected_streams.public[0].merge_strategy
	const options = {
		catalog: data,
		existingJob: false,
		sourceType: "postgres",
		destinationType: "iceberg",
		sourceConfig,
		destinationConfig,
	}
	const created = withDefaultCopyOnWrite(options)
	assert.equal(
		created.selected_streams.public[0].merge_strategy,
		"copy-on-write",
	)
	assert.equal(withIndexRequired({}, created).index_required, false)
	assert.equal(
		withDefaultCopyOnWrite({ ...options, existingJob: true }).selected_streams
			.public[0].merge_strategy,
		undefined,
	)
	data.selected_streams.public[0].merge_strategy = "merge-on-read"
	assert.equal(
		withDefaultCopyOnWrite(options).selected_streams.public[0].merge_strategy,
		"merge-on-read",
	)
	delete data.selected_streams.public[0].merge_strategy
	data.selected_streams.public[0].append_mode = true
	assert.equal(
		withDefaultCopyOnWrite(options).selected_streams.public[0].merge_strategy,
		undefined,
	)
	data.selected_streams.public[0].append_mode = false
	delete data.streams[0].stream.supports_iceberg_copy_on_write
	assert.equal(
		withDefaultCopyOnWrite(options).selected_streams.public[0].merge_strategy,
		undefined,
	)
})
