import { DESTINATION_INTERNAL_TYPES } from "@/modules/ingestion/common/constants/constants"

import DropRemovedColumnsSectionView from "./DropRemovedColumnsSectionView"

interface DropRemovedColumnsSectionBulkProps {
	destinationType?: string
	isDirty?: boolean
	normalization: boolean
	dropRemovedColumns: boolean
	onChange?: (value: boolean) => void
}

const DropRemovedColumnsSectionBulk = ({
	destinationType,
	isDirty,
	normalization,
	dropRemovedColumns,
	onChange,
}: DropRemovedColumnsSectionBulkProps) => {
	if (destinationType !== DESTINATION_INTERNAL_TYPES.ICEBERG) return null

	return (
		<DropRemovedColumnsSectionView
			dropRemovedColumns={dropRemovedColumns}
			enabled={normalization}
			isDirty={isDirty}
			onChange={onChange}
		/>
	)
}

export default DropRemovedColumnsSectionBulk
