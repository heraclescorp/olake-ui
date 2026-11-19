import { Alert, Checkbox, InputNumber, Select } from "antd"
import { useId, type ReactElement } from "react"

import type {
	COWOptions,
	MergeStrategy,
} from "@/modules/ingestion/common/types"

import { CARD_STYLE } from "../../constants"
import {
	COW_DEFAULTS,
	COW_MAX_FLUSH_INTERVAL_SECONDS,
	validateCOWOptions,
} from "../../utils/copyOnWrite"

interface MergeStrategyEditorProps {
	strategy?: MergeStrategy
	options?: COWOptions
	appendMode?: boolean
	disabled?: boolean
	disabledReason?: string | null
	keepExisting?: boolean
	dirty?: boolean
	error?: string | null
	onStrategyChange?: (strategy: MergeStrategy | undefined) => void
	onOptionsChange?: (options: COWOptions) => void
}

export default function MergeStrategyEditor({
	strategy,
	options,
	appendMode,
	disabled = false,
	disabledReason,
	keepExisting = false,
	dirty = false,
	error,
	onStrategyChange,
	onOptionsChange,
}: MergeStrategyEditorProps): ReactElement {
	const id = useId()
	const cow = strategy === "copy-on-write"
	const optionError = validateCOWOptions(options)
	const controlsDisabled = disabled || !onStrategyChange
	return (
		<section
			className={`${CARD_STYLE} flex min-w-0 flex-col gap-3`}
			aria-label="Upsert materialization"
		>
			<h3 className="text-sm font-medium text-olake-text">
				Upsert materialization{dirty ? " — modified" : ""}
			</h3>
			<Select<MergeStrategy | "keep">
				aria-label="Upsert materialization strategy"
				value={strategy ?? (keepExisting ? "keep" : "merge-on-read")}
				disabled={controlsDisabled}
				options={[
					...(keepExisting
						? [
								{
									value: "keep" as const,
									label: "Keep each table’s existing strategy",
								},
							]
						: []),
					{ value: "merge-on-read", label: "Merge-on-read" },
					{
						value: "copy-on-write",
						label: "Copy-on-write (index-free)",
						disabled: !!disabledReason || appendMode === true,
					},
				]}
				onChange={value =>
					onStrategyChange?.(value === "keep" ? undefined : value)
				}
			/>
			<p className="text-xs text-olake-text-secondary">
				Copy-on-write is the default for new eligible upserts. Existing jobs
				keep their saved strategy. Merge-on-read is available for write-heavy
				workloads when readers support its delete files.
			</p>
			{appendMode === true && (
				<p className="text-xs text-olake-text-secondary">
					Append remains append. The saved merge policy and COW options are
					inactive and preserved for Upsert.
				</p>
			)}
			{disabledReason && (
				<Alert
					type="warning"
					showIcon
					message={disabledReason}
				/>
			)}
			{cow && (
				<>
					<Alert
						type="warning"
						showIcon
						message="Copy-on-write scans and rewrites matching data files; it builds no persistent row index. Clean current snapshots use data files, not equality/position deletes or delete vectors. Saved MOR delete-format preferences are retained for switching back."
					/>
					<p className="text-xs text-olake-text-secondary">
						{keepExisting
							? "Existing options are kept unless explicitly changed. "
							: ""}
						New tables use Iceberg v2; existing v3 tables are not downgraded.
						Existing sort orders, schema, partition layout and table identity
						are unchanged. Sorting by actual primary keys may reduce scans, but
						sort configuration remains creation-only.
					</p>
					<Checkbox
						indeterminate={
							keepExisting &&
							options?.materialize_existing_deletes === undefined
						}
						checked={options?.materialize_existing_deletes ?? false}
						disabled={
							controlsDisabled ||
							!!disabledReason ||
							appendMode === true ||
							!onOptionsChange
						}
						onChange={event =>
							onOptionsChange?.({
								materialize_existing_deletes: event.target.checked,
							})
						}
					>
						I explicitly permit one-time materialization of existing deletes
					</Checkbox>
					<Alert
						type="warning"
						showIcon
						message="Existing equality/position deletes or Puffin vectors fail safely without this permission. Opting in permits a full current-view rewrite honoring all existing deletes, not just the next changed rows. It preserves the source checkpoint and replication slot: no source backfill, checkpoint reset or slot reset. Old history/files may remain under retention; switching policy alone does not instantly clean historical snapshots."
					/>
					<details>
						<summary className="cursor-pointer text-sm text-olake-text">
							Advanced copy-on-write batching
						</summary>
						<p className="my-2 text-xs text-olake-text-secondary">
							Soft limits flush only after complete source transactions. A long
							transaction may exceed a limit; no partial transaction is
							acknowledged.
						</p>
						<div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
							{(
								[
									["flush_interval_seconds", "Flush interval (seconds)"],
									["max_batch_rows", "Maximum incoming batch rows"],
									["max_batch_bytes", "Maximum incoming batch bytes"],
									["rewrite_concurrency", "Rewrite concurrency"],
								] as const
							).map(([key, label]) => (
								<div
									key={key}
									className="flex flex-col gap-1"
								>
									<label
										htmlFor={`${id}-${key}`}
										className="text-xs text-olake-text"
									>
										{label}
									</label>
									<InputNumber
										id={`${id}-${key}`}
										aria-label={label}
										aria-invalid={!!optionError}
										aria-describedby={optionError ? `${id}-error` : undefined}
										min={1}
										max={
											key === "flush_interval_seconds"
												? COW_MAX_FLUSH_INTERVAL_SECONDS
												: Number.MAX_SAFE_INTEGER
										}
										step={1}
										value={
											keepExisting
												? options?.[key]
												: (options?.[key] ?? COW_DEFAULTS[key])
										}
										placeholder={
											keepExisting
												? `Keep existing (default ${COW_DEFAULTS[key]})`
												: undefined
										}
										disabled={
											controlsDisabled ||
											!!disabledReason ||
											appendMode === true ||
											!onOptionsChange
										}
										onChange={value => onOptionsChange?.({ [key]: value ?? 0 })}
									/>
								</div>
							))}
						</div>
					</details>
				</>
			)}
			{(optionError || error) && (
				<p
					id={`${id}-error`}
					role="alert"
					className="text-sm text-red-600"
				>
					{optionError || error}
				</p>
			)}
		</section>
	)
}
