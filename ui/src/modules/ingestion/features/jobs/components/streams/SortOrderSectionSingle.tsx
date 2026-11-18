import type { ReactElement } from "react"

import { DESTINATION_INTERNAL_TYPES } from "@/modules/ingestion/common/constants"

import {
	selectActiveSelectedStream,
	selectActiveStreamData,
	selectStreamsData,
	useStreamSelectionStore,
} from "../../stores"
import {
	getSortColumnOptions,
	getInheritedSortEligibility,
	SORT_CAPABILITY_WARNING,
	isNormalizedSortStream,
	validateExplicitSortOrder,
} from "../../utils/sortOrder"
import SortOrderEditor from "./SortOrderEditor"

interface SortOrderSectionSingleProps {
	destinationType?: string
}

export default function SortOrderSectionSingle({
	destinationType,
}: SortOrderSectionSingleProps): ReactElement | null {
	const stream = useStreamSelectionStore(selectActiveStreamData)
	const selected = useStreamSelectionStore(selectActiveSelectedStream)
	const catalog = useStreamSelectionStore(selectStreamsData)
	const updateSortOrder = useStreamSelectionStore(
		state => state.updateSortOrder,
	)
	if (!stream || !selected) {
		return null
	}
	const iceberg = destinationType === DESTINATION_INTERNAL_TYPES.ICEBERG
	if (!iceberg && selected.sort_order === undefined) {
		return null
	}
	const normalized = isNormalizedSortStream({ stream, selected })
	let disabledReason: string | undefined
	if (!iceberg) {
		disabledReason =
			"Sort configuration is only available for Iceberg destinations."
	} else if (stream.stream.supports_iceberg_sort_order !== true) {
		disabledReason = SORT_CAPABILITY_WARNING
	} else if (!normalized) {
		disabledReason = "Enable normalization to configure source-column sorting."
	} else if (selected.disabled) {
		disabledReason = "Select the stream to configure its sort order."
	}
	return (
		<SortOrderEditor
			order={selected.sort_order}
			columns={getSortColumnOptions({ stream, selected })}
			disabled={
				!iceberg ||
				stream.stream.supports_iceberg_sort_order !== true ||
				!normalized ||
				!!selected.disabled
			}
			disabledReason={disabledReason}
			inheritanceDescription={
				getInheritedSortEligibility({
					stream,
					selected,
					defaultOrder: catalog?.default_sort_order,
				}).description
			}
			error={validateExplicitSortOrder({ stream, selected, destinationType })}
			onChange={(order): void =>
				updateSortOrder({
					stream: {
						streamName: stream.stream.name,
						namespace: stream.stream.namespace ?? "",
					},
					order,
				})
			}
		/>
	)
}
