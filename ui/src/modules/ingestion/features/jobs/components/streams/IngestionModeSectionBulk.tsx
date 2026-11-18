import IngestionModeSectionView from "./IngestionModeSectionView"
import { IngestionMode } from "../../enums"

interface IngestionModeSectionBulkProps {
	sourceType?: string
	sourceVersion?: string
	destinationType?: string
	isDirty?: boolean
	bulkAppendMode?: boolean
	dedupKeyCount?: number
	onBulkIngestionModeChange?: (appendMode: boolean) => void
}

const IngestionModeSectionBulk = ({
	sourceType,
	sourceVersion,
	destinationType,
	isDirty,
	bulkAppendMode,
	dedupKeyCount,
	onBulkIngestionModeChange,
}: IngestionModeSectionBulkProps) => {
	return (
		<>
			{bulkAppendMode === undefined && (
				<p>Mixed / Keep existing ingestion modes</p>
			)}
			<IngestionModeSectionView
				sourceType={sourceType}
				sourceVersion={sourceVersion}
				destinationType={destinationType}
				isSelected={true}
				isDirty={isDirty}
				appendMode={bulkAppendMode}
				dedupKeyCount={dedupKeyCount}
				onChange={ingestionMode =>
					onBulkIngestionModeChange?.(ingestionMode === IngestionMode.APPEND)
				}
			/>
		</>
	)
}

export default IngestionModeSectionBulk
