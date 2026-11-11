import { DESTINATION_INTERNAL_TYPES } from "@/modules/ingestion/common/constants/constants"

import DropRemovedColumnsSectionView from "./DropRemovedColumnsSectionView"
import { STREAM_DEFAULTS } from "../../constants"
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

	return (
		<DropRemovedColumnsSectionView
			dropRemovedColumns={!!storeSelectedStream.drop_removed_columns}
			enabled={isSelected && normalization}
			onChange={checked =>
				updateDropRemovedColumns(
					{
						streamName: storeStream.stream.name,
						namespace: storeStream.stream.namespace || "",
					},
					checked,
				)
			}
		/>
	)
}

export default DropRemovedColumnsSectionSingle
