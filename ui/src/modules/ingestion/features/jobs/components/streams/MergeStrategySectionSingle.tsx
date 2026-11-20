import type { ReactElement } from "react"

import {
	selectActiveSelectedStream,
	selectActiveStreamData,
	useJobConfigurationStore,
	useStreamSelectionStore,
} from "../../stores"
import { getCOWEligibilityError } from "../../utils/copyOnWrite"
import MergeStrategyEditor from "./MergeStrategyEditor"

interface MergeStrategySectionSingleProps {
	sourceType?: string
	destinationType?: string
}

export default function MergeStrategySectionSingle({
	sourceType,
	destinationType,
}: MergeStrategySectionSingleProps): ReactElement | null {
	const stream = useStreamSelectionStore(selectActiveStreamData)
	const selected = useStreamSelectionStore(selectActiveSelectedStream)
	const updateMergeStrategy = useStreamSelectionStore(
		state => state.updateMergeStrategy,
	)
	const source = useJobConfigurationStore(state => state.selectedSource)
	const destination = useJobConfigurationStore(
		state => state.selectedDestination,
	)
	if (!stream || !selected) return null
	if (
		(sourceType?.toLowerCase() !== "postgres" ||
			destinationType?.toLowerCase() !== "iceberg") &&
		selected.merge_strategy !== "copy-on-write"
	)
		return null
	const identifier = {
		streamName: stream.stream.name,
		namespace: stream.stream.namespace ?? "",
	}
	return (
		<MergeStrategyEditor
			strategy={selected.merge_strategy}
			appendMode={selected.append_mode}
			disabled={!!selected.disabled}
			disabledReason={getCOWEligibilityError({
				stream,
				selected,
				sourceType,
				destinationType,
				sourceConfig: source?.config,
				destinationConfig: destination?.config,
			})}
			onStrategyChange={strategy => {
				if (strategy) updateMergeStrategy(identifier, strategy)
			}}
		/>
	)
}
