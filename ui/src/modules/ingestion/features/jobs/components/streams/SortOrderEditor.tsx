import { ArrowDownIcon, ArrowUpIcon, TrashIcon } from "@phosphor-icons/react"
import { Alert, Button, Select } from "antd"
import type { ReactElement } from "react"
import { useId } from "react"

import type {
	SortDirection,
	SortField,
	SortNullOrder,
} from "@/modules/ingestion/common/types"

import { CARD_STYLE } from "../../constants"
import type { SortColumnOption } from "../../utils/sortOrder"

export interface SortOrderEditorProps {
	order: SortField[] | undefined
	columns: SortColumnOption[]
	disabled?: boolean
	disabledReason?: string
	inheritanceDescription?: string
	error?: string | null
	onChange?: (order: SortField[] | undefined) => void
}

type SortMode = "inherit" | "unsorted" | "custom"

export default function SortOrderEditor({
	order,
	columns,
	disabled = false,
	disabledReason,
	inheritanceDescription,
	error,
	onChange,
}: SortOrderEditorProps): ReactElement {
	const errorId = `${useId()}-sort-error`
	const mode: SortMode =
		order === undefined ? "inherit" : order.length === 0 ? "unsorted" : "custom"
	const usedColumns =
		order?.map(
			field =>
				columns.find(column => column.value === field.column)
					?.destinationColumn ?? field.column,
		) ?? []
	const nextColumn = columns.find(
		column =>
			!column.disabled && !usedColumns.includes(column.destinationColumn),
	)
	const controlsDisabled = disabled || !onChange
	const updateField = (index: number, patch: Partial<SortField>): void => {
		onChange?.(
			order?.map(
				(field, position): SortField =>
					position === index ? { ...field, ...patch } : field,
			),
		)
	}
	const moveField = (index: number, offset: number): void => {
		if (!order) {
			return
		}
		const next = [...order]
		const target = index + offset
		;[next[index], next[target]] = [next[target], next[index]]
		onChange?.(next)
	}

	return (
		<section
			className={`${CARD_STYLE} flex min-w-0 flex-col gap-3 break-words`}
			aria-label="Iceberg table sort order"
		>
			<h3 className="text-sm font-medium text-olake-text">
				Iceberg sort order
			</h3>
			<p className="text-xs text-olake-text-secondary">
				Creation-time table metadata only. Existing tables are never changed.
			</p>
			<Select<SortMode>
				aria-label="Table sort policy"
				aria-invalid={!!error}
				aria-describedby={error ? errorId : undefined}
				value={mode}
				disabled={controlsDisabled}
				options={[
					{ value: "inherit", label: "Inherit catalog default" },
					{ value: "unsorted", label: "Explicitly unsorted" },
					{
						value: "custom",
						label: "Custom ordered columns",
						disabled: !nextColumn && !order?.length,
					},
				]}
				onChange={(value): void => {
					switch (value) {
						case "inherit":
							onChange?.(undefined)
							break
						case "unsorted":
							onChange?.([])
							break
						case "custom":
							if (nextColumn) {
								onChange?.([
									{
										column: nextColumn.value,
										direction: "asc",
										null_order: "nulls-last",
									},
								])
							}
							break
						default: {
							const unsupported: never = value
							throw new Error(`Unsupported sort policy: ${unsupported}`)
						}
					}
				}}
			/>
			{disabledReason && (
				<p className="text-xs text-olake-text-secondary">
					{disabledReason} Saved settings are preserved.
				</p>
			)}
			{controlsDisabled && order !== undefined && onChange && (
				<Button onClick={(): void => onChange(undefined)}>
					Clear table sort override
				</Button>
			)}
			{mode === "inherit" && inheritanceDescription && (
				<p
					className="text-xs text-olake-text-secondary"
					role="status"
				>
					{inheritanceDescription}
				</p>
			)}
			{mode === "unsorted" && (
				<p className="text-xs text-olake-text-secondary">
					UNSORTED: ignores the catalog default for this table.
				</p>
			)}
			{mode === "custom" && (
				<>
					<p className="text-xs text-olake-text-secondary">
						Keys are applied in the order shown. Only selected primitive source
						columns are supported.
					</p>
					<ol className="flex flex-col gap-3">
						{order?.map(
							(field, index): ReactElement => (
								<li
									key={`${index}-${field.column}`}
									className="flex min-w-0 flex-col gap-2 rounded-lg border border-olake-border p-3"
								>
									<div className="flex flex-wrap items-center justify-between gap-2">
										<span className="text-xs font-medium text-olake-text">
											Sort key {index + 1}
										</span>
										<div className="flex gap-1">
											<Button
												size="small"
												aria-label={`Move sort key ${index + 1} up`}
												icon={<ArrowUpIcon />}
												disabled={controlsDisabled || index === 0}
												onClick={(): void => moveField(index, -1)}
											/>
											<Button
												size="small"
												aria-label={`Move sort key ${index + 1} down`}
												icon={<ArrowDownIcon />}
												disabled={
													controlsDisabled || index === (order?.length ?? 0) - 1
												}
												onClick={(): void => moveField(index, 1)}
											/>
											<Button
												size="small"
												aria-label={`Remove sort key ${index + 1}`}
												icon={<TrashIcon />}
												disabled={controlsDisabled}
												onClick={(): void =>
													onChange?.(
														order?.filter(
															(_, position): boolean => position !== index,
														),
													)
												}
											/>
										</div>
									</div>
									<Select<string>
										aria-label={`Sort column ${index + 1}`}
										value={field.column}
										disabled={controlsDisabled}
										options={[
											...(!columns.some(column => column.value === field.column)
												? [
														{
															value: field.column,
															label: `${field.column} — absent from schema`,
														},
													]
												: []),
											...columns.map(column => ({
												...column,
												disabled:
													column.disabled ||
													usedColumns.some(
														(identity, position) =>
															position !== index &&
															identity === column.destinationColumn,
													),
											})),
										]}
										onChange={(column): void => updateField(index, { column })}
									/>
									<div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
										<Select<SortDirection>
											aria-label={`Sort direction ${index + 1}`}
											value={field.direction}
											disabled={controlsDisabled}
											options={[
												{ value: "asc", label: "Ascending (ASC)" },
												{ value: "desc", label: "Descending (DESC)" },
											]}
											onChange={(direction): void =>
												updateField(index, { direction })
											}
										/>
										<Select<SortNullOrder>
											aria-label={`Sort null order ${index + 1}`}
											value={field.null_order}
											disabled={controlsDisabled}
											options={[
												{ value: "nulls-first", label: "NULLS FIRST" },
												{ value: "nulls-last", label: "NULLS LAST" },
											]}
											onChange={(null_order): void =>
												updateField(index, { null_order })
											}
										/>
									</div>
								</li>
							),
						)}
					</ol>
					<Button
						disabled={controlsDisabled || !nextColumn}
						onClick={(): void => {
							if (nextColumn) {
								onChange?.([
									...(order ?? []),
									{
										column: nextColumn.value,
										direction: "asc",
										null_order: "nulls-last",
									},
								])
							}
						}}
					>
						Add sort column
					</Button>
				</>
			)}
			{!columns.some(column => !column.disabled) && (
				<p className="text-xs text-olake-text-secondary">
					No eligible selected primitive columns. Review the schema and column
					selection.
				</p>
			)}
			{error && (
				<Alert
					id={errorId}
					type="error"
					showIcon
					message={error}
				/>
			)}
		</section>
	)
}
