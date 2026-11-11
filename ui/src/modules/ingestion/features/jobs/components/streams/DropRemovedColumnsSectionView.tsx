import { WarningIcon } from "@phosphor-icons/react"
import { Switch } from "antd"
import clsx from "clsx"

import { CARD_STYLE, DROP_REMOVED_COLUMNS_HELP_TEXT } from "../../constants"

export interface DropRemovedColumnsSectionViewProps {
	dropRemovedColumns: boolean
	enabled: boolean
	isDirty?: boolean
	onChange?: (value: boolean) => void
}

const DropRemovedColumnsSectionView = ({
	dropRemovedColumns,
	enabled,
	isDirty,
	onChange,
}: DropRemovedColumnsSectionViewProps) => (
	<div
		className={clsx(
			!enabled ? "font-normal text-text-disabled" : "font-medium",
			CARD_STYLE,
		)}
	>
		<div className="flex items-center justify-between">
			<div className="flex items-center gap-1">
				{isDirty && <WarningIcon className="size-4 text-orange-500" />}
				<label>Drop columns removed from source</label>
			</div>
			<Switch
				aria-label="Drop columns removed from source"
				checked={dropRemovedColumns}
				onChange={onChange ?? (() => {})}
				disabled={!enabled}
			/>
		</div>
		<p className="mt-1 text-xs text-gray-500">
			{DROP_REMOVED_COLUMNS_HELP_TEXT} Requires Normalization.
		</p>
	</div>
)

export default DropRemovedColumnsSectionView
