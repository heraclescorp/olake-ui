import { Alert, Select } from "antd"
import type { ReactElement } from "react"

import type { MergeStrategy } from "@/modules/ingestion/common/types"

import { CARD_STYLE } from "../../constants"

interface MergeStrategyEditorProps {
	strategy?: MergeStrategy
	appendMode?: boolean
	disabled?: boolean
	disabledReason?: string | null
	keepExisting?: boolean
	dirty?: boolean
	error?: string | null
	onStrategyChange?: (strategy: MergeStrategy | undefined) => void
}

export default function MergeStrategyEditor({
	strategy,
	appendMode,
	disabled = false,
	disabledReason,
	keepExisting = false,
	dirty = false,
	error,
	onStrategyChange,
}: MergeStrategyEditorProps): ReactElement {
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
				disabled={disabled || !onStrategyChange}
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
				New eligible jobs default to copy-on-write. Existing jobs keep their
				saved strategy and MOR delete-format preference. Append ignores this
				choice.
			</p>
			{disabledReason && (
				<Alert
					type="warning"
					showIcon
					message={disabledReason}
				/>
			)}
			{strategy === "copy-on-write" && (
				<Alert
					type="warning"
					showIcon
					message="Copy-on-write rewrites matching data files without a persistent row index. Existing MOR tables with live delete files require a separate maintenance cleanup before switching; this job does not clean them up."
				/>
			)}
			{error && (
				<p
					role="alert"
					className="text-sm text-red-600"
				>
					{error}
				</p>
			)}
		</section>
	)
}
