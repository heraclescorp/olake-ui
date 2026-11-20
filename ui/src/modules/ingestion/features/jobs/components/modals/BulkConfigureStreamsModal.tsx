import {
	CaretDownIcon,
	CaretRightIcon,
	CheckIcon,
	ColumnsPlusRightIcon,
	FadersHorizontalIcon,
	InfoIcon,
	RowsIcon,
	TableIcon,
	WarningIcon,
	XIcon,
} from "@phosphor-icons/react"
import { Button, Modal, Tooltip } from "antd"
import clsx from "clsx"
import { useEffect, useMemo, useRef, useState } from "react"

import { DESTINATION_INTERNAL_TYPES } from "@/modules/ingestion/common/constants"
import {
	FilterConfig,
	StreamIdentifier,
	SyncMode,
	UpsertType,
	SortField,
	MergeStrategy,
} from "@/modules/ingestion/common/types"
import { isKafkaSource } from "@/modules/ingestion/common/utils"
import { BulkConfigureStreamsModalProps } from "@/modules/ingestion/features/jobs/types"

import { CARD_STYLE, STREAM_DEFAULTS } from "../../constants"
import {
	selectAvailableUpdateTypes,
	useStreamSelectionStore,
	useJobConfigurationStore,
} from "../../stores"
import {
	buildBulkStreamsData,
	getDedupKeyOptions,
	getKafkaUpsertNotSupportedMessage,
	isUseSourceColumnNamesSupported,
} from "../../utils/streams"
import BulkStreamSelectorList from "../streams/BulkStreamSelectorList"
import DataFilterSectionBulk from "../streams/DataFilterSectionBulk"
import DedupKeysSelectionBulk from "../streams/DedupKeysSelectionBulk"
import DropRemovedColumnsSectionBulk from "../streams/DropRemovedColumnsSectionBulk"
import IngestionModeSectionBulk from "../streams/IngestionModeSectionBulk"
import NormalizationSectionBulk from "../streams/NormalizationSectionBulk"
import PartitionRegexSectionBulk from "../streams/PartitionRegexSectionBulk"
import SourceNamingConventionSectionBulk from "../streams/SourceNamingConventionSectionBulk"
import SyncModeSectionBulk from "../streams/SyncModeSectionBulk"
import UpsertTypeSectionBulk from "../streams/UpsertTypeSectionBulk"
import SortOrderEditor from "../streams/SortOrderEditor"
import { getBulkSortConfiguration } from "../../utils/sortOrder"
import MergeStrategyEditor from "../streams/MergeStrategyEditor"
import {
	copyOnWriteEnabled,
	getCOWEligibilityError,
} from "../../utils/copyOnWrite"

type BulkConfigureStep =
	| "select-streams"
	| "apply-configurations"
	| "summary"
	| "success"
type BulkConfigurationTab = "config" | "partitioning" | "schema"

type BulkConfig = {
	syncMode: SyncMode | undefined
	cursorField: string | undefined
	appendMode: boolean | undefined
	dedupKeys: string[]
	upsertType?: UpsertType
	mergeStrategy?: MergeStrategy
	normalization: boolean
	dropRemovedColumns: boolean
	filter: string
	filterConfig: FilterConfig | undefined
	partitionRegex: string
	useSourceColumnNames: boolean
	sortOrder: SortField[] | undefined
}

enum BulkDirtyFieldKey {
	SyncMode = "syncMode",
	AppendMode = "appendMode",
	DedupKeys = "dedupKeys",
	UpsertType = "upsertType",
	Normalization = "normalization",
	DropRemovedColumns = "dropRemovedColumns",
	Filter = "filter",
	PartitionRegex = "partitionRegex",
	UseSourceColumnNames = "useSourceColumnNames",
	SortOrder = "sortOrder",
	MergeStrategy = "mergeStrategy",
}

type BulkDirtyFields = Record<BulkDirtyFieldKey, boolean>

const INITIAL_DIRTY_FIELDS: BulkDirtyFields = {
	[BulkDirtyFieldKey.SyncMode]: false,
	[BulkDirtyFieldKey.AppendMode]: false,
	[BulkDirtyFieldKey.DedupKeys]: false,
	[BulkDirtyFieldKey.UpsertType]: false,
	[BulkDirtyFieldKey.Normalization]: false,
	[BulkDirtyFieldKey.DropRemovedColumns]: false,
	[BulkDirtyFieldKey.Filter]: false,
	[BulkDirtyFieldKey.PartitionRegex]: false,
	[BulkDirtyFieldKey.UseSourceColumnNames]: false,
	[BulkDirtyFieldKey.SortOrder]: false,
	[BulkDirtyFieldKey.MergeStrategy]: false,
}

const INITIAL_BULK_CONFIG: BulkConfig = {
	syncMode: SyncMode.FULL_REFRESH,
	cursorField: undefined,
	appendMode: true,
	dedupKeys: [],
	upsertType: undefined,
	mergeStrategy: undefined,
	normalization: false,
	dropRemovedColumns: false,
	filter: "",
	filterConfig: undefined,
	partitionRegex: "",
	useSourceColumnNames: false,
	sortOrder: undefined,
}

const CLOSE_COUNTDOWN = 3
const MAX_VISIBLE_SELECTED_STREAMS = 5

const sortCursorFields = (
	availableCursors: string[],
	primaryKeys: string[],
): string[] =>
	[...availableCursors].sort((a, b) => {
		const aIsPK = primaryKeys.includes(a)
		const bIsPK = primaryKeys.includes(b)
		if (aIsPK && !bIsPK) return -1
		if (!aIsPK && bIsPK) return 1
		return a.localeCompare(b)
	})

const BulkConfigureStreamsModal = ({
	open,
	onClose,
	streamsData,
	sourceType,
	sourceVersion,
	destinationType,
}: BulkConfigureStreamsModalProps) => {
	const isKafka = isKafkaSource(sourceType)
	const source = useJobConfigurationStore(state => state.selectedSource)
	const destination = useJobConfigurationStore(
		state => state.selectedDestination,
	)
	const availableUpdateTypes = useStreamSelectionStore(
		selectAvailableUpdateTypes,
	)
	const kafkaUpsertBlocked = !!getKafkaUpsertNotSupportedMessage(
		sourceType,
		sourceVersion,
		availableUpdateTypes,
	)
	const schemaTabVisible = isUseSourceColumnNamesSupported(sourceVersion)
	const [step, setStep] = useState<BulkConfigureStep>("select-streams")
	const [activeTab, setActiveTab] = useState<BulkConfigurationTab>("config")
	const [closeCountdown, setCloseCountdown] = useState(CLOSE_COUNTDOWN)
	const [isSyncIngestionCollapsed, setIsSyncIngestionCollapsed] =
		useState<boolean>(true)
	const [isSelectedStreamsExpanded, setIsSelectedStreamsExpanded] =
		useState<boolean>(false)
	const [isSummaryStreamsExpanded, setIsSummaryStreamsExpanded] =
		useState<boolean>(false)

	const [bulkSelectedStreams, setBulkSelectedStreams] = useState<
		StreamIdentifier[]
	>([])
	const prepareCOW = useRef(false)
	const [cowSkipped, setCOWSkipped] = useState<
		{ name: string; reason: string }[] | null
	>(null)

	// Local bulk config state
	const [bulkConfig, setBulkConfig] = useState<BulkConfig>(INITIAL_BULK_CONFIG)
	const setBulkConfigField = <K extends keyof BulkConfig>(
		key: K,
		value: BulkConfig[K],
	) => setBulkConfig(prev => ({ ...prev, [key]: value }))

	// Tracks which sections the user has explicitly modified.
	// Only dirty sections are included in the apply payload.
	const [dirtyFields, setDirtyFields] =
		useState<BulkDirtyFields>(INITIAL_DIRTY_FIELDS)
	const bulkSort = getBulkSortConfiguration({
		catalog: streamsData,
		streams: bulkSelectedStreams,
		order: bulkConfig.sortOrder,
		normalization: dirtyFields[BulkDirtyFieldKey.Normalization]
			? bulkConfig.normalization
			: undefined,
		destinationType,
	})
	const sortError = dirtyFields[BulkDirtyFieldKey.SortOrder]
		? bulkSort.error
		: null
	const cowRows = bulkSelectedStreams.flatMap(id => {
		const stream = streamsData?.streams.find(
			entry =>
				entry.stream.name === id.streamName &&
				(entry.stream.namespace ?? "") === id.namespace,
		)
		const selected = streamsData?.selected_streams[id.namespace]?.find(
			entry => entry.stream_name === id.streamName,
		)
		if (!stream || !selected) return []
		return [
			{
				stream,
				selected: {
					...selected,
					disabled: false,
					...(dirtyFields[BulkDirtyFieldKey.AppendMode] && {
						append_mode: bulkConfig.appendMode,
					}),
					...(dirtyFields[BulkDirtyFieldKey.Normalization] && {
						normalization: bulkConfig.normalization,
					}),
					...(dirtyFields[BulkDirtyFieldKey.SyncMode] && {
						sync_mode: bulkConfig.syncMode,
					}),
					...(dirtyFields[BulkDirtyFieldKey.UseSourceColumnNames] && {
						use_source_column_names: bulkConfig.useSourceColumnNames,
					}),
					...(dirtyFields[BulkDirtyFieldKey.MergeStrategy] && {
						merge_strategy: bulkConfig.mergeStrategy,
					}),
				},
			},
		]
	})
	const cowEligibility = cowRows.map(row => ({
		...row,
		error: getCOWEligibilityError({
			...row,
			sourceType,
			destinationType,
			sourceConfig: source?.config,
			destinationConfig: destination?.config,
		}),
	}))
	const cowDisabledReason = cowEligibility.find(row => row.error)?.error
	const bulkCOWError = cowEligibility.find(
		row => copyOnWriteEnabled(row.selected) && row.error,
	)?.error
	const allActiveCOW =
		cowRows.length > 0 && cowRows.every(row => copyOnWriteEnabled(row.selected))

	const markDirty = (key: BulkDirtyFieldKey) =>
		setDirtyFields(prev => ({ ...prev, [key]: true }))

	const selectionKey = useMemo(
		() =>
			[...bulkSelectedStreams]
				.map(s => `${s.namespace}__${s.streamName}`)
				.sort()
				.join(","),
		[bulkSelectedStreams],
	)

	const { stream: bulkStream, defaults: bulkStreamDefaults } = useMemo(
		() =>
			buildBulkStreamsData(
				bulkSelectedStreams,
				streamsData,
				sourceType,
				destinationType,
			),
		[selectionKey, streamsData, sourceType, destinationType],
	)

	// Resolve saved selections against each stream's own defaults, not the first
	// connector default. Undefined represents mixed values, never an instruction.
	const savedWriteSettings = useMemo(() => {
		const rows = bulkSelectedStreams.map(id => {
			const selected = streamsData?.selected_streams[id.namespace]?.find(
				s => s.stream_name === id.streamName,
			)
			const defaults = streamsData?.streams.find(
				s =>
					s.stream.name === id.streamName &&
					(s.stream.namespace || "") === id.namespace,
			)?.stream.default_stream_properties
			return {
				append: selected?.append_mode ?? defaults?.append_mode ?? false,
				type: selected?.update_type ?? defaults?.update_type,
			}
		})
		return {
			append:
				rows.length && rows.every(r => r.append === rows[0].append)
					? rows[0].append
					: undefined,
			type:
				rows.length && rows.every(r => !r.append && r.type === rows[0].type)
					? rows[0].type
					: undefined,
		}
	}, [selectionKey, streamsData])

	useEffect(() => {
		if (open) {
			setStep("select-streams")
			setActiveTab("config")
			setCloseCountdown(CLOSE_COUNTDOWN)
			setBulkSelectedStreams([])
			setBulkConfig(INITIAL_BULK_CONFIG)
			setDirtyFields(INITIAL_DIRTY_FIELDS)
			setIsSyncIngestionCollapsed(true)
			setIsSelectedStreamsExpanded(false)
			prepareCOW.current = false
			setCOWSkipped(null)
		}
	}, [open])

	useEffect(() => {
		if (!open || step !== "success") return

		if (closeCountdown <= 0) {
			setStep("select-streams")
			setCloseCountdown(CLOSE_COUNTDOWN)
			onClose()
			return
		}

		const timeoutId = window.setTimeout(() => {
			setCloseCountdown(prev => prev - 1)
		}, 1000)

		return () => window.clearTimeout(timeoutId)
	}, [open, step, closeCountdown, onClose])

	useEffect(() => {
		// Reset all config state to defaults on selection change.
		const syncMode = bulkStreamDefaults.sync_mode
		const availableCursors = bulkStream.stream.available_cursor_fields ?? []
		const primaryKeys = bulkStream.stream.source_defined_primary_key ?? []
		const sortedCursors = sortCursorFields(availableCursors, primaryKeys)
		setBulkConfig({
			syncMode,
			cursorField:
				syncMode === SyncMode.INCREMENTAL ? sortedCursors[0] : undefined,
			appendMode: savedWriteSettings.append,
			dedupKeys: bulkStreamDefaults.dedup_keys ?? [],
			upsertType: savedWriteSettings.type,
			mergeStrategy: undefined,
			normalization:
				bulkStreamDefaults.normalization ?? STREAM_DEFAULTS.normalization,
			dropRemovedColumns: false,
			filter: "",
			filterConfig: undefined,
			partitionRegex: "",
			useSourceColumnNames: false,
			sortOrder: undefined,
		})
		setDirtyFields(INITIAL_DIRTY_FIELDS)
		setIsSyncIngestionCollapsed(true)
		setIsSelectedStreamsExpanded(false)
		setIsSummaryStreamsExpanded(false)
	}, [selectionKey])

	useEffect(() => {
		if (step !== "apply-configurations" || !prepareCOW.current) return
		prepareCOW.current = false
		setBulkConfig(prev => ({ ...prev, mergeStrategy: "copy-on-write" }))
		setDirtyFields(prev => ({
			...prev,
			[BulkDirtyFieldKey.MergeStrategy]: true,
		}))
	}, [step, selectionKey])

	const getStepTitle = () => {
		if (step === "select-streams") return "Select Streams"
		if (step === "apply-configurations") return "Apply Configurations"
		if (step === "summary") return "Summary of Changes"
		return ""
	}

	const getStepSubtitle = () => {
		if (step === "select-streams")
			return "Select streams you wish to bulk configure"
		if (step === "apply-configurations")
			return "Set the properties to be applied across selected streams"
		if (step === "summary")
			return "Review the configurations that will be applied to selected streams"
		return ""
	}

	const bulkUpdateStreams = useStreamSelectionStore(
		state => state.bulkUpdateStreams,
	)

	const keepWriteSettings = () => {
		setBulkConfig(prev => ({
			...prev,
			appendMode: savedWriteSettings.append,
			upsertType: savedWriteSettings.type,
		}))
		setDirtyFields(prev => ({
			...prev,
			[BulkDirtyFieldKey.AppendMode]: false,
			[BulkDirtyFieldKey.UpsertType]: false,
		}))
	}

	const selectEligibleCOW = (): void => {
		const eligible: StreamIdentifier[] = []
		const skipped: { name: string; reason: string }[] = []
		const streams = new Map(
			streamsData?.streams.map(entry => [
				JSON.stringify([entry.stream.namespace ?? "", entry.stream.name]),
				entry,
			]),
		)
		for (const [namespace, entries] of Object.entries(
			streamsData?.selected_streams ?? {},
		)) {
			for (const selected of entries) {
				if (selected.disabled) continue
				const stream = streams.get(
					JSON.stringify([namespace, selected.stream_name]),
				)
				const reason = !stream
					? "Refresh discovery: source table metadata is unavailable."
					: selected.append_mode
						? "Append mode does not use upserts."
						: getCOWEligibilityError({
								stream,
								selected,
								sourceType,
								destinationType,
								sourceConfig: source?.config,
								destinationConfig: destination?.config,
							})
				if (reason)
					skipped.push({
						name: `${namespace ? `${namespace}.` : ""}${selected.stream_name}`,
						reason,
					})
				else eligible.push({ namespace, streamName: selected.stream_name })
			}
		}
		prepareCOW.current = eligible.length > 0
		setCOWSkipped(skipped)
		setBulkSelectedStreams(eligible)
		if (eligible.length > 0) setStep("apply-configurations")
	}

	const handleApplyChanges = () => {
		bulkUpdateStreams(bulkSelectedStreams, {
			...(dirtyFields[BulkDirtyFieldKey.SyncMode] && {
				syncMode: bulkConfig.syncMode,
				cursorField: bulkConfig.cursorField,
			}),
			...(dirtyFields[BulkDirtyFieldKey.MergeStrategy] && {
				mergeStrategy: bulkConfig.mergeStrategy,
			}),
			...(dirtyFields[BulkDirtyFieldKey.AppendMode] && {
				appendMode: bulkConfig.appendMode,
			}),
			...(dirtyFields[BulkDirtyFieldKey.DedupKeys] &&
				bulkConfig.appendMode !== true && {
					dedupKeys: bulkConfig.dedupKeys,
				}),
			...(dirtyFields[BulkDirtyFieldKey.UpsertType] &&
				!allActiveCOW &&
				bulkConfig.appendMode !== true && {
					upsertType: bulkConfig.upsertType,
				}),
			...(dirtyFields[BulkDirtyFieldKey.Normalization] && {
				normalization: bulkConfig.normalization,
			}),
			...(dirtyFields[BulkDirtyFieldKey.DropRemovedColumns] && {
				dropRemovedColumns: bulkConfig.dropRemovedColumns,
			}),
			...(dirtyFields[BulkDirtyFieldKey.Filter] && {
				filterValue: bulkConfig.filter,
				filterConfig: bulkConfig.filterConfig,
			}),
			...(dirtyFields[BulkDirtyFieldKey.PartitionRegex] && {
				partitionRegex: bulkConfig.partitionRegex,
			}),
			...(dirtyFields[BulkDirtyFieldKey.UseSourceColumnNames] && {
				useSourceColumnNames: bulkConfig.useSourceColumnNames,
			}),
			...(dirtyFields[BulkDirtyFieldKey.SortOrder] && {
				sortOrder: bulkConfig.sortOrder,
			}),
		})

		setCloseCountdown(CLOSE_COUNTDOWN)
		setStep("success")
	}

	const missingUpsertDedupKeys =
		isKafka &&
		!kafkaUpsertBlocked &&
		bulkConfig.appendMode !== true &&
		(dirtyFields[BulkDirtyFieldKey.AppendMode] ||
			dirtyFields[BulkDirtyFieldKey.DedupKeys]) &&
		bulkConfig.dedupKeys.length === 0

	const handleRemoveSelectedStream = (streamToRemove: StreamIdentifier) => {
		setBulkSelectedStreams(prev =>
			prev.filter(
				stream =>
					!(
						stream.namespace === streamToRemove.namespace &&
						stream.streamName === streamToRemove.streamName
					),
			),
		)
	}

	const visibleSelectedStreams = isSelectedStreamsExpanded
		? bulkSelectedStreams
		: bulkSelectedStreams.slice(0, MAX_VISIBLE_SELECTED_STREAMS)
	const hiddenSelectedStreamsCount = Math.max(
		0,
		bulkSelectedStreams.length - MAX_VISIBLE_SELECTED_STREAMS,
	)

	const getFooter = () => {
		if (step === "success") return null

		if (step === "select-streams") {
			return (
				<div className="flex h-20 items-center justify-end gap-3 border-t border-olake-border px-8">
					<Button onClick={onClose}>Cancel</Button>
					<Button
						type="primary"
						onClick={() => setStep("apply-configurations")}
						disabled={bulkSelectedStreams.length === 0}
					>
						Configure Streams
					</Button>
				</div>
			)
		}

		if (step === "summary") {
			return (
				<div className="flex h-20 items-center justify-between border-t border-olake-border px-8">
					<Button onClick={() => setStep("apply-configurations")}>Back</Button>
					<div className="flex items-center gap-3">
						<Button onClick={onClose}>Cancel</Button>
						<Tooltip
							title={
								missingUpsertDedupKeys
									? "Select at least one dedup key for Upsert"
									: undefined
							}
						>
							<span className="inline-block">
								<Button
									type="primary"
									onClick={handleApplyChanges}
									disabled={
										missingUpsertDedupKeys || !!sortError || !!bulkCOWError
									}
								>
									Apply Changes
								</Button>
							</span>
						</Tooltip>
					</div>
				</div>
			)
		}

		return (
			<div className="flex h-20 items-center justify-between border-t border-olake-border px-8">
				<Button onClick={() => setStep("select-streams")}>Back</Button>
				<div className="flex items-center gap-3">
					<Button onClick={onClose}>Cancel</Button>
					<Button
						type="primary"
						onClick={() => setStep("summary")}
						disabled={
							bulkSelectedStreams.length === 0 ||
							!Object.values(dirtyFields).some(Boolean) ||
							missingUpsertDedupKeys ||
							!!sortError ||
							!!bulkCOWError
						}
					>
						Review Changes
					</Button>
				</div>
			</div>
		)
	}

	return (
		<Modal
			open={open}
			onCancel={onClose}
			destroyOnHidden
			footer={getFooter()}
			closable={false}
			centered
			width={983}
			classNames={{
				content: "!overflow-hidden !rounded-[20px] !p-0",
				body: "!p-0",
				footer: "!m-0 !p-0",
			}}
		>
			{step === "success" ? (
				<div className="relative h-[808px] bg-background-primary">
					<div className="absolute left-1/2 top-1/2 flex w-[374px] -translate-x-1/2 -translate-y-1/2 flex-col items-center gap-5 text-center">
						<div className="rounded-xl bg-primary-100 p-3">
							<CheckIcon
								weight="bold"
								className="size-8 text-primary"
							/>
						</div>
						<div className="w-full">
							<div className="text-xl font-medium leading-7 text-olake-text">
								{`${bulkSelectedStreams.length} streams configured successfully`}
							</div>
							<div className="mt-1 text-base leading-6 text-olake-text">
								You are free to edit the stream separately if you wish
							</div>
						</div>
					</div>
					<div className="absolute left-1/2 top-[683px] -translate-x-1/2">
						<Button onClick={onClose}>Closing in {closeCountdown}...</Button>
					</div>
				</div>
			) : (
				<div className="flex h-[728px] flex-col">
					<div className="border-b border-olake-border px-8 pb-5 pt-8">
						<h2 className="text-xl font-medium leading-7 text-olake-text">
							Bulk Streams Configure
						</h2>
						<p className="mt-2 text-sm leading-5 text-olake-text">
							{getStepSubtitle()}
						</p>
					</div>

					<div className="border-b border-olake-border px-8 py-4 text-base font-medium leading-7 text-olake-text">
						{getStepTitle()}
					</div>

					{step !== "select-streams" && cowSkipped && (
						<div
							className="border-b border-olake-border px-8 py-2"
							role="status"
						>
							{bulkSelectedStreams.length} eligible; {cowSkipped.length}{" "}
							skipped. Destination delete files are checked when the job runs.
							{cowSkipped.length > 0 && (
								<details>
									<summary>Skipped tables and reasons</summary>
									<ul>
										{cowSkipped.map(row => (
											<li key={row.name}>
												{row.name}: {row.reason}
											</li>
										))}
									</ul>
								</details>
							)}
						</div>
					)}
					<div className="min-h-0 flex-1">
						{step === "select-streams" ? (
							<div className="flex h-full flex-col">
								{sourceType?.toLowerCase() === "postgres" &&
									destinationType?.toLowerCase() === "iceberg" && (
										<div className="border-b border-neutral-border px-8 py-3">
											<Button onClick={selectEligibleCOW}>
												Configure COW for eligible job tables
											</Button>
											<p className="mt-2 text-xs text-olake-text-secondary">
												Only enabled upsert tables are considered. Review before
												applying; existing live delete files require separate
												cleanup.
											</p>
										</div>
									)}
								{cowSkipped && bulkSelectedStreams.length === 0 && (
									<div
										className="px-8 py-2"
										role="status"
									>
										No eligible tables. {cowSkipped.length} skipped; see reasons
										below.
									</div>
								)}
								{cowSkipped && (
									<details className="px-8">
										<summary>Skipped tables</summary>
										<ul>
											{cowSkipped.map(row => (
												<li key={row.name}>
													{row.name}: {row.reason}
												</li>
											))}
										</ul>
									</details>
								)}
								<div className="min-h-0 flex-1">
									<BulkStreamSelectorList
										streamsData={streamsData}
										bulkSelectedStreams={bulkSelectedStreams}
										onChange={value => {
											prepareCOW.current = false
											setCOWSkipped(null)
											setBulkSelectedStreams(value)
										}}
									/>
								</div>
							</div>
						) : step === "summary" ? (
							<div className="h-full overflow-y-auto px-8 py-5">
								<div className="flex flex-col gap-4">
									{/* Selected streams */}
									<div className={CARD_STYLE}>
										<div className="mb-3 flex items-center gap-2">
											<TableIcon className="size-4 text-olake-text-secondary" />
											<span className="text-sm font-medium text-olake-text">
												Streams to Configure ({bulkSelectedStreams.length})
											</span>
										</div>
										<div className="flex flex-wrap gap-2">
											{(isSummaryStreamsExpanded
												? bulkSelectedStreams
												: bulkSelectedStreams.slice(
														0,
														MAX_VISIBLE_SELECTED_STREAMS,
													)
											).map(stream => {
												const streamId = `${stream.namespace}__${stream.streamName}`
												return (
													<div
														key={streamId}
														className="flex h-7 items-center gap-1.5 rounded bg-olake-surface-muted px-3 py-0.5"
													>
														<TableIcon className="size-3.5 shrink-0 text-olake-text-secondary" />
														<span className="text-xs text-olake-text">
															{stream.namespace ? `${stream.namespace} / ` : ""}
															{stream.streamName}
														</span>
													</div>
												)
											})}
											{bulkSelectedStreams.length >
												MAX_VISIBLE_SELECTED_STREAMS &&
												!isSummaryStreamsExpanded && (
													<button
														type="button"
														onClick={() => setIsSummaryStreamsExpanded(true)}
														className="flex h-7 items-center gap-2 rounded bg-olake-surface-muted px-3 py-0.5 text-olake-text"
													>
														<span className="text-xs font-medium">
															+
															{bulkSelectedStreams.length -
																MAX_VISIBLE_SELECTED_STREAMS}{" "}
															more
														</span>
													</button>
												)}
											{isSummaryStreamsExpanded &&
												bulkSelectedStreams.length >
													MAX_VISIBLE_SELECTED_STREAMS && (
													<button
														type="button"
														onClick={() => setIsSummaryStreamsExpanded(false)}
														className="flex h-7 items-center gap-2 rounded bg-olake-surface-muted px-3 py-0.5 text-olake-text"
													>
														<span className="text-xs font-medium">
															View less
														</span>
													</button>
												)}
										</div>
									</div>

									{/* Changes to apply — same components as configure step, read-only. */}
									<div className="relative">
										<div className="absolute inset-0 z-10 cursor-not-allowed" />
										<div className="pointer-events-none flex flex-col gap-4 opacity-60">
											{(dirtyFields[BulkDirtyFieldKey.SyncMode] ||
												dirtyFields[BulkDirtyFieldKey.AppendMode] ||
												dirtyFields[BulkDirtyFieldKey.DedupKeys] ||
												(dirtyFields[BulkDirtyFieldKey.UpsertType] &&
													bulkConfig.appendMode !== true)) && (
												<div className={CARD_STYLE}>
													<span className="mb-2 block text-sm font-medium text-olake-text">
														Ingestion Settings
													</span>
													{dirtyFields[BulkDirtyFieldKey.SyncMode] && (
														<SyncModeSectionBulk
															bulkStream={bulkStream}
															bulkSyncMode={bulkConfig.syncMode}
															bulkCursorField={bulkConfig.cursorField}
														/>
													)}
													{dirtyFields[BulkDirtyFieldKey.AppendMode] && (
														<IngestionModeSectionBulk
															sourceType={sourceType}
															sourceVersion={sourceVersion}
															destinationType={destinationType}
															bulkAppendMode={bulkConfig.appendMode}
															dedupKeyCount={
																isKafka
																	? getDedupKeyOptions(bulkStream).length
																	: undefined
															}
														/>
													)}
													{isKafka &&
														!kafkaUpsertBlocked &&
														dirtyFields[BulkDirtyFieldKey.DedupKeys] &&
														bulkConfig.appendMode !== true && (
															<DedupKeysSelectionBulk
																options={getDedupKeyOptions(bulkStream)}
																value={bulkConfig.dedupKeys}
																isDirty
																onChange={() => {}}
															/>
														)}
													{dirtyFields[BulkDirtyFieldKey.UpsertType] &&
														!allActiveCOW && (
															<UpsertTypeSectionBulk
																sourceType={sourceType}
																sourceVersion={sourceVersion}
																destinationType={destinationType}
																bulkAppendMode={bulkConfig.appendMode}
																bulkUpsertType={bulkConfig.upsertType}
															/>
														)}
												</div>
											)}
											{dirtyFields[BulkDirtyFieldKey.MergeStrategy] && (
												<MergeStrategyEditor
													strategy={bulkConfig.mergeStrategy}
													appendMode={bulkConfig.appendMode}
													keepExisting
												/>
											)}
											{dirtyFields[BulkDirtyFieldKey.Normalization] && (
												<NormalizationSectionBulk
													normalization={bulkConfig.normalization}
												/>
											)}
											{dirtyFields[BulkDirtyFieldKey.DropRemovedColumns] && (
												<DropRemovedColumnsSectionBulk
													destinationType={destinationType}
													normalization={bulkConfig.normalization}
													dropRemovedColumns={bulkConfig.dropRemovedColumns}
												/>
											)}
											{dirtyFields[BulkDirtyFieldKey.SortOrder] && (
												<SortOrderEditor
													order={bulkConfig.sortOrder}
													columns={bulkSort.columns}
													inheritanceDescription="Inherit the catalog default, including future changes."
												/>
											)}
											{dirtyFields[BulkDirtyFieldKey.Filter] && (
												<DataFilterSectionBulk
													bulkStream={bulkStream}
													bulkFilter={bulkConfig.filter}
													bulkFilterConfig={bulkConfig.filterConfig}
												/>
											)}
											{dirtyFields[BulkDirtyFieldKey.PartitionRegex] && (
												<PartitionRegexSectionBulk
													destinationType={destinationType}
													bulkPartitionRegex={bulkConfig.partitionRegex}
												/>
											)}
											{dirtyFields[BulkDirtyFieldKey.UseSourceColumnNames] && (
												<SourceNamingConventionSectionBulk
													useSourceColumnNames={bulkConfig.useSourceColumnNames}
												/>
											)}
										</div>
									</div>
								</div>
							</div>
						) : (
							<div className={clsx("h-full", "overflow-y-auto px-8 py-5")}>
								<div className="text-base leading-6 text-olake-text">
									Streams Selected ({bulkSelectedStreams.length})
								</div>
								{bulkSelectedStreams.length === 0 ? (
									<div className="flex h-full items-center justify-center">
										<div className="max-w-[420px] text-center">
											<div className="text-base font-medium text-olake-text">
												No Streams Selected
											</div>
											<div className="mt-2 text-sm leading-5 text-olake-text-secondary">
												No streams are selected for bulk configuration. Please
												Go back and select one or more streams to continue.
											</div>
										</div>
									</div>
								) : (
									<>
										<div className="mt-2 flex flex-wrap gap-2">
											{visibleSelectedStreams.map(stream => {
												const streamId = `${stream.namespace}__${stream.streamName}`
												return (
													<div
														key={streamId}
														className="flex h-7 items-center gap-2 rounded bg-olake-surface-muted px-3.5 py-0.5"
													>
														<TableIcon className="size-4 text-olake-text" />
														<span className="text-sm text-olake-text">
															{stream.namespace ? `${stream.namespace} / ` : ""}
															{stream.streamName}
														</span>
														<button
															type="button"
															onClick={() => handleRemoveSelectedStream(stream)}
															aria-label={`Remove ${stream.streamName}`}
															className="inline-flex items-center text-olake-text-tertiary hover:text-olake-text"
														>
															<XIcon className="size-4" />
														</button>
													</div>
												)
											})}
											{hiddenSelectedStreamsCount > 0 &&
												!isSelectedStreamsExpanded && (
													<button
														type="button"
														onClick={() => setIsSelectedStreamsExpanded(true)}
														className="flex h-7 items-center gap-2 rounded bg-olake-surface-muted px-3.5 py-0.5 text-olake-text"
													>
														<span className="text-sm font-medium">
															+{hiddenSelectedStreamsCount} more
														</span>
													</button>
												)}
											{isSelectedStreamsExpanded &&
												bulkSelectedStreams.length >
													MAX_VISIBLE_SELECTED_STREAMS && (
													<button
														type="button"
														onClick={() => setIsSelectedStreamsExpanded(false)}
														className="flex h-7 items-center gap-2 rounded bg-olake-surface-muted px-3.5 py-0.5 text-olake-text"
													>
														<span className="text-sm font-medium">
															View less
														</span>
													</button>
												)}
										</div>
										<div className="mt-4 rounded-md border border-l-4 border-olake-border border-l-primary bg-primary-100 px-3 py-2 text-sm text-olake-text-secondary">
											<div className="mb-1 flex items-center gap-1.5">
												<InfoIcon className="size-4 text-primary" />
												<span className="text-xs font-semibold uppercase tracking-wide text-olake-text">
													Info
												</span>
											</div>
											<p className="text-xs font-normal leading-5">
												Changes will be applied only to properties marked with{" "}
												<span className="inline-block align-baseline">
													<WarningIcon className="relative top-[4px] size-5 text-amber-600" />
												</span>
												<br />
												Any existing configuration for those properties will be
												overridden
											</p>
										</div>

										<div className="mt-8 rounded-md bg-olake-surface-muted p-0.5">
											<div
												className={clsx(
													"grid gap-1",
													schemaTabVisible ? "grid-cols-3" : "grid-cols-2",
												)}
											>
												<button
													type="button"
													onClick={() => setActiveTab("config")}
													className={clsx(
														"flex h-7 items-center justify-center gap-2 rounded-md border px-3 text-sm leading-5",
														activeTab === "config"
															? "border-primary bg-white text-primary shadow-sm"
															: "border-transparent text-olake-text",
													)}
												>
													<FadersHorizontalIcon className="size-4" />
													Config
												</button>
												{schemaTabVisible && (
													<button
														type="button"
														onClick={() => setActiveTab("schema")}
														className={clsx(
															"flex h-7 items-center justify-center gap-2 rounded-md border px-3 text-sm leading-5",
															activeTab === "schema"
																? "border-primary bg-white text-primary shadow-sm"
																: "border-transparent text-olake-text",
														)}
													>
														<ColumnsPlusRightIcon className="size-4" />
														Schema
													</button>
												)}
												<button
													type="button"
													onClick={() => setActiveTab("partitioning")}
													className={clsx(
														"flex h-7 items-center justify-center gap-2 rounded-md border px-3 text-sm leading-5",
														activeTab === "partitioning"
															? "border-primary bg-white text-primary shadow-sm"
															: "border-transparent text-olake-text",
													)}
												>
													<RowsIcon className="size-4" />
													Partitioning
												</button>
											</div>
										</div>
										<div className="mt-4">
											{activeTab === "schema" ? (
												<div
													key={selectionKey}
													className="flex flex-col gap-4"
												>
													<SourceNamingConventionSectionBulk
														isDirty={
															dirtyFields[
																BulkDirtyFieldKey.UseSourceColumnNames
															]
														}
														useSourceColumnNames={
															bulkConfig.useSourceColumnNames
														}
														onChange={value => {
															setBulkConfigField("useSourceColumnNames", value)
															markDirty(BulkDirtyFieldKey.UseSourceColumnNames)
														}}
													/>
												</div>
											) : activeTab === "config" ? (
												<div
													key={selectionKey}
													className="flex flex-col gap-4"
												>
													{/* Sync Mode and Ingestion Mode Sections */}
													<div className={CARD_STYLE}>
														<button
															type="button"
															onClick={() =>
																setIsSyncIngestionCollapsed(prev => !prev)
															}
															className={clsx(
																`flex w-full items-center justify-between text-left text-sm font-medium text-olake-text`,
																!isSyncIngestionCollapsed && "mb-2",
															)}
															aria-expanded={!isSyncIngestionCollapsed}
															aria-label="Toggle sync and ingestion settings"
														>
															<span>Ingestion Settings</span>
															{isSyncIngestionCollapsed ? (
																<CaretRightIcon className="size-4 text-olake-text-tertiary" />
															) : (
																<CaretDownIcon className="size-4 text-olake-text-tertiary" />
															)}
														</button>
														{!isSyncIngestionCollapsed && (
															<>
																<Button onClick={keepWriteSettings}>
																	Keep existing write settings
																</Button>
																<p>
																	Only changed fields apply. Selecting a delete
																	format also sets Upsert for the listed
																	streams. Apply Changes updates this editor;
																	Finish saves the job.
																</p>
																<SyncModeSectionBulk
																	isDirty={
																		dirtyFields[BulkDirtyFieldKey.SyncMode]
																	}
																	bulkStream={bulkStream}
																	bulkSyncMode={bulkConfig.syncMode}
																	bulkCursorField={bulkConfig.cursorField}
																	onBulkSyncModeChange={(mode, cursor) => {
																		setBulkConfigField("syncMode", mode)
																		setBulkConfigField("cursorField", cursor)
																		markDirty(BulkDirtyFieldKey.SyncMode)
																	}}
																/>
																<IngestionModeSectionBulk
																	isDirty={
																		dirtyFields[BulkDirtyFieldKey.AppendMode]
																	}
																	sourceType={sourceType}
																	sourceVersion={sourceVersion}
																	destinationType={destinationType}
																	bulkAppendMode={bulkConfig.appendMode}
																	dedupKeyCount={
																		isKafka
																			? getDedupKeyOptions(bulkStream).length
																			: undefined
																	}
																	onBulkIngestionModeChange={value => {
																		setBulkConfigField("appendMode", value)
																		setDirtyFields(prev => ({
																			...prev,
																			[BulkDirtyFieldKey.AppendMode]:
																				value !== savedWriteSettings.append,
																		}))
																	}}
																/>
																{isKafka &&
																	!kafkaUpsertBlocked &&
																	bulkConfig.appendMode !== true && (
																		<DedupKeysSelectionBulk
																			isDirty={
																				dirtyFields[BulkDirtyFieldKey.DedupKeys]
																			}
																			options={getDedupKeyOptions(bulkStream)}
																			value={bulkConfig.dedupKeys}
																			onChange={keys => {
																				setBulkConfigField("dedupKeys", keys)
																				markDirty(BulkDirtyFieldKey.DedupKeys)
																			}}
																		/>
																	)}
																{!allActiveCOW &&
																	cowRows.some(row =>
																		copyOnWriteEnabled(row.selected),
																	) && (
																		<p className="text-xs text-olake-text-secondary">
																			Delete-format changes apply only to
																			merge-on-read tables. COW tables retain
																			their saved MOR preference.
																		</p>
																	)}
																{!allActiveCOW && (
																	<UpsertTypeSectionBulk
																		isDirty={
																			dirtyFields[BulkDirtyFieldKey.UpsertType]
																		}
																		sourceType={sourceType}
																		sourceVersion={sourceVersion}
																		destinationType={destinationType}
																		bulkAppendMode={bulkConfig.appendMode}
																		bulkUpsertType={bulkConfig.upsertType}
																		onBulkUpsertTypeChange={value => {
																			setBulkConfigField("upsertType", value)
																			setDirtyFields(prev => ({
																				...prev,
																				[BulkDirtyFieldKey.UpsertType]:
																					value !== undefined &&
																					value !== savedWriteSettings.type,
																				...(value !== undefined && {
																					[BulkDirtyFieldKey.AppendMode]:
																						savedWriteSettings.append !== false,
																				}),
																			}))
																			if (value !== undefined)
																				setBulkConfigField("appendMode", false)
																		}}
																	/>
																)}
															</>
														)}
													</div>

													{/* Normalization Section */}
													<NormalizationSectionBulk
														isDirty={
															dirtyFields[BulkDirtyFieldKey.Normalization]
														}
														normalization={bulkConfig.normalization}
														onChange={(value: boolean) => {
															setBulkConfigField("normalization", value)
															markDirty(BulkDirtyFieldKey.Normalization)
														}}
													/>
													{((sourceType?.toLowerCase() === "postgres" &&
														destinationType?.toLowerCase() === "iceberg") ||
														cowRows.some(
															row =>
																row.selected.merge_strategy === "copy-on-write",
														)) && (
														<MergeStrategyEditor
															strategy={bulkConfig.mergeStrategy}
															appendMode={bulkConfig.appendMode}
															keepExisting
															dirty={
																dirtyFields[BulkDirtyFieldKey.MergeStrategy]
															}
															disabledReason={cowDisabledReason}
															error={bulkCOWError}
															onStrategyChange={strategy => {
																setBulkConfigField("mergeStrategy", strategy)
																setDirtyFields(prev => ({
																	...prev,
																	[BulkDirtyFieldKey.MergeStrategy]:
																		strategy !== undefined,
																}))
															}}
														/>
													)}

													<DropRemovedColumnsSectionBulk
														isDirty={
															dirtyFields[BulkDirtyFieldKey.DropRemovedColumns]
														}
														destinationType={destinationType}
														normalization={bulkConfig.normalization}
														dropRemovedColumns={bulkConfig.dropRemovedColumns}
														onChange={(value: boolean) => {
															setBulkConfigField("dropRemovedColumns", value)
															markDirty(BulkDirtyFieldKey.DropRemovedColumns)
														}}
													/>

													{destinationType ===
														DESTINATION_INTERNAL_TYPES.ICEBERG && (
														<div className="flex flex-col gap-2">
															<p className="text-xs text-olake-text-secondary">
																{dirtyFields[BulkDirtyFieldKey.SortOrder]
																	? "Sort policy will be applied to all selected tables."
																	: "Keep existing sort policies unless you change this section."}{" "}
																Custom columns must be eligible in every
																selected table.
															</p>
															<SortOrderEditor
																order={bulkConfig.sortOrder}
																columns={bulkSort.columns}
																disabled={bulkSort.disabled}
																disabledReason={bulkSort.disabledReason}
																inheritanceDescription="Inherited tables follow the catalog default; explicit overrides remain unchanged until applied."
																error={sortError}
																onChange={(order): void => {
																	setBulkConfigField("sortOrder", order)
																	markDirty(BulkDirtyFieldKey.SortOrder)
																}}
															/>
															{dirtyFields[BulkDirtyFieldKey.SortOrder] && (
																<Button
																	onClick={(): void =>
																		setDirtyFields(prev => ({
																			...prev,
																			[BulkDirtyFieldKey.SortOrder]: false,
																		}))
																	}
																>
																	Keep existing sort settings
																</Button>
															)}
															{!dirtyFields[BulkDirtyFieldKey.SortOrder] &&
																!bulkSort.disabled && (
																	<Button
																		onClick={(): void => {
																			setBulkConfigField("sortOrder", undefined)
																			markDirty(BulkDirtyFieldKey.SortOrder)
																		}}
																	>
																		Reset selected tables to inherited sort
																		policy
																	</Button>
																)}
														</div>
													)}

													{/* Data Filter Section */}
													<DataFilterSectionBulk
														isDirty={dirtyFields[BulkDirtyFieldKey.Filter]}
														bulkStream={bulkStream}
														bulkFilter={bulkConfig.filter}
														onBulkFilterChange={value => {
															setBulkConfigField("filter", value)
															markDirty(BulkDirtyFieldKey.Filter)
														}}
														bulkFilterConfig={bulkConfig.filterConfig}
														onBulkFilterConfigChange={value => {
															setBulkConfigField("filterConfig", value)
															markDirty(BulkDirtyFieldKey.Filter)
														}}
													/>
												</div>
											) : (
												<div
													key={selectionKey}
													className="flex flex-col gap-4"
												>
													<PartitionRegexSectionBulk
														isDirty={
															dirtyFields[BulkDirtyFieldKey.PartitionRegex]
														}
														destinationType={destinationType}
														bulkPartitionRegex={bulkConfig.partitionRegex}
														onBulkPartitionRegexChange={value => {
															setBulkConfigField("partitionRegex", value)
															markDirty(BulkDirtyFieldKey.PartitionRegex)
														}}
													/>
												</div>
											)}
										</div>
									</>
								)}
							</div>
						)}
					</div>
				</div>
			)}
		</Modal>
	)
}

export default BulkConfigureStreamsModal
