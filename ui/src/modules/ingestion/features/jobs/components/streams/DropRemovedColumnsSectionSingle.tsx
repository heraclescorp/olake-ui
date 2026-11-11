import { Switch } from "antd"
import clsx from "clsx"

import { DESTINATION_INTERNAL_TYPES } from "@/modules/ingestion/common/constants/constants"

import {
	CARD_STYLE,
	DROP_REMOVED_COLUMNS_HELP_TEXT,
	STREAM_DEFAULTS,
} from "../../constants"
import {
	selectActiveSelectedStream,
	selectActiveStreamData,
	selectIsStreamEnabled,
	useStreamSelectionStore,
} from "../../stores"

const DropRemovedColumnsSectionSingle = ({
	destinationType,
}: {
	destinationType?: string
}) => {
	const storeStream = useStreamSelectionStore(selectActiveStreamData)
	const storeSelectedStream = useStreamSelectionStore(
		selectActiveSelectedStream,
	)
	const isSelected = useStreamSelectionStore(state =>
		selectIsStreamEnabled(state, storeStream),
	)
	const updateDropRemovedColumns = useStreamSelectionStore(
		state => state.updateDropRemovedColumns,
	)

	if (
		!storeStream ||
		!storeSelectedStream ||
		destinationType !== DESTINATION_INTERNAL_TYPES.ICEBERG
	)
		return null

	const normalization =
		storeSelectedStream.normalization ??
		storeStream.stream.default_stream_properties?.normalization ??
		STREAM_DEFAULTS.normalization
	const enabled = isSelected && normalization

	return (
		<div
			className={clsx(
				!enabled ? "font-normal text-text-disabled" : "font-medium",
				CARD_STYLE,
			)}
		>
			<div className="flex items-center justify-between">
				<label>Drop columns removed from source</label>
				<Switch
					aria-label="Drop columns removed from source"
					checked={!!storeSelectedStream.drop_removed_columns}
					onChange={checked =>
						updateDropRemovedColumns(
							{
								streamName: storeStream.stream.name,
								namespace: storeStream.stream.namespace || "",
							},
							checked,
						)
					}
					disabled={!enabled}
				/>
			</div>
			<p className="mt-1 text-xs text-gray-500">
				{DROP_REMOVED_COLUMNS_HELP_TEXT} Requires Normalization.
			</p>
		</div>
	)
}

export default DropRemovedColumnsSectionSingle
