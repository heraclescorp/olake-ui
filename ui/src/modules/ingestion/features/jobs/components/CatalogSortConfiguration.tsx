import { Alert, Button, Switch } from "antd"
import type { ReactElement } from "react"

import { DESTINATION_INTERNAL_TYPES } from "@/modules/ingestion/common/constants"

import { CARD_STYLE } from "../constants"
import { selectStreamsData, useStreamSelectionStore } from "../stores"
import {
	CREATED_AT_SORT_ORDER,
	SORT_COMPACTION_WARNING,
	SORT_CREATION_WARNING,
	getInheritedSortEligibility,
	SORT_CAPABILITY_WARNING,
} from "../utils/sortOrder"

interface CatalogSortConfigurationProps {
	destinationType?: string
	disabled?: boolean
}

export default function CatalogSortConfiguration({
	destinationType,
	disabled,
}: CatalogSortConfigurationProps): ReactElement | null {
	const catalog = useStreamSelectionStore(selectStreamsData)
	const updateDefaultSortOrder = useStreamSelectionStore(
		state => state.updateDefaultSortOrder,
	)
	if (!catalog) {
		return null
	}
	if (destinationType !== DESTINATION_INTERNAL_TYPES.ICEBERG) {
		return catalog.default_sort_order?.length ? (
			<div className="mb-4 flex flex-col gap-2">
				<Alert
					type="info"
					showIcon
					message="Saved Iceberg sort policy is preserved. Sorting controls are unavailable for this destination."
				/>
				<Button
					disabled={disabled}
					onClick={(): void => updateDefaultSortOrder(undefined)}
				>
					Clear default sort policy
				</Button>
			</div>
		) : null
	}
	const order = catalog.default_sort_order
	const supported =
		catalog.streams.length > 0 &&
		catalog.streams.every(
			entry => entry.stream.supports_iceberg_sort_order === true,
		)
	const enabled = !!order?.length
	const isPreset =
		order?.length === 1 &&
		order[0].column === "created_at" &&
		order[0].direction === "asc" &&
		order[0].null_order === "nulls-last"
	let eligible = 0
	let skipped = 0
	for (const stream of catalog.streams) {
		const selected = catalog.selected_streams[
			stream.stream.namespace ?? ""
		]?.find(entry => entry.stream_name === stream.stream.name)
		if (!selected || selected.disabled || selected.sort_order !== undefined) {
			continue
		}
		const eligibility = getInheritedSortEligibility({
			stream,
			selected,
			defaultOrder: CREATED_AT_SORT_ORDER,
		})
		if (eligibility.eligible) {
			eligible += 1
		} else {
			skipped += 1
		}
	}
	return (
		<section
			className={`${CARD_STYLE} mb-4 flex flex-col gap-3`}
			aria-label="Catalog Iceberg sort default"
		>
			<div className="flex items-center justify-between gap-4">
				<label
					htmlFor="catalog-created-at-sort"
					className="text-sm font-medium text-olake-text"
				>
					Default sort order: created_at ASC NULLS LAST
				</label>
				<Switch
					id="catalog-created-at-sort"
					aria-label="Default created_at sort order"
					checked={enabled}
					disabled={disabled || !supported}
					onChange={(checked): void =>
						updateDefaultSortOrder(
							checked
								? CREATED_AT_SORT_ORDER.map(field => ({ ...field }))
								: undefined,
						)
					}
				/>
			</div>
			<p className="text-sm text-olake-text-secondary">
				Opt-in for normalized Iceberg tables with a selected, supported temporal
				created_at column. Missing, excluded or incompatible columns stay
				UNSORTED without failing the job. Per-table overrides take precedence.
			</p>
			<p
				className="text-xs text-olake-text-secondary"
				role="status"
			>
				{enabled
					? "Default enabled."
					: "Default off: existing jobs remain UNSORTED unless a table has a custom override."}{" "}
				{eligible} selected inheriting tables eligible; {skipped} would skip the
				preset. Future selected and auto-discovered tables inherit this policy,
				without copying it into table overrides.
			</p>
			{!supported && (
				<Alert
					type="warning"
					showIcon
					message={SORT_CAPABILITY_WARNING}
				/>
			)}
			{!supported && enabled && (
				<Button
					disabled={disabled}
					onClick={(): void => updateDefaultSortOrder(undefined)}
				>
					Clear default sort policy
				</Button>
			)}
			{enabled && !isPreset && (
				<Alert
					type="warning"
					showIcon
					message="A different catalog default was loaded and is preserved. Turn the default off and back on to replace it with the created_at preset."
				/>
			)}
			<Alert
				type="warning"
				showIcon
				message={SORT_CREATION_WARNING}
				description={SORT_COMPACTION_WARNING}
			/>
		</section>
	)
}
