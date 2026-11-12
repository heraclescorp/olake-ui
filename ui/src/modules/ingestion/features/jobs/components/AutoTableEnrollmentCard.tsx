import { Alert, Select, Switch } from "antd"

import { UpsertType } from "@/modules/ingestion/common/types"

import {
	DROP_REMOVED_COLUMNS_HELP_TEXT,
	UPSERT_TYPE_OPTIONS,
} from "../constants"
import {
	AutoTableEnrollmentDraft,
	EnrollmentCatalog,
	EnrollmentCapabilities,
} from "../utils/autoTableEnrollment"

type BooleanChoice = "enabled" | "disabled"

const BOOLEAN_CHOICE_VALUES: Record<BooleanChoice, boolean> = {
	enabled: true,
	disabled: false,
}

interface BooleanSettingProps {
	label: string
	description: string
	value?: boolean
	trueLabel: string
	falseLabel: string
	disabled: boolean
	onChange: (value: boolean) => void
}

const BooleanSetting = ({
	label,
	value,
	description,
	trueLabel,
	falseLabel,
	disabled,
	onChange,
}: BooleanSettingProps): JSX.Element => (
	<label className="flex flex-col gap-2 text-sm text-gray-700">
		{label}
		<Select<BooleanChoice>
			aria-label={label}
			value={value === undefined ? undefined : value ? "enabled" : "disabled"}
			placeholder="Select an option"
			disabled={disabled}
			onChange={(choice): void => onChange(BOOLEAN_CHOICE_VALUES[choice])}
			options={[
				{ value: "enabled", label: trueLabel },
				{ value: "disabled", label: falseLabel },
			]}
		/>
		<span className="text-xs text-gray-600">{description}</span>
	</label>
)

interface AutoTableEnrollmentCardProps {
	supported: boolean
	catalog?: EnrollmentCatalog
	capabilities: EnrollmentCapabilities
	draft: AutoTableEnrollmentDraft
	disabled: boolean
	onChange: (draft: AutoTableEnrollmentDraft) => void
}

const AutoTableEnrollmentCard = ({
	supported,
	catalog,
	capabilities,
	draft,
	disabled,
	onChange,
}: AutoTableEnrollmentCardProps): JSX.Element => {
	const controlsDisabled = disabled || !supported
	const deleteOptions = UPSERT_TYPE_OPTIONS.map(option => ({
		value: option.value,
		label: option.label,
		disabled:
			draft.defaults.append_mode !== true &&
			!capabilities.availableUpdateTypes.includes(option.value),
	}))

	return (
		<section
			aria-labelledby="auto-enrollment-title"
			className="mt-5 rounded-xl border border-olake-border p-6"
		>
			<div className="mb-4 flex items-center justify-between gap-4">
				<div>
					<h3
						id="auto-enrollment-title"
						className="text-base font-medium text-gray-900"
					>
						Automatically add new tables
					</h3>
					<p className="mt-1 text-sm text-gray-600">
						Before each run, OLake checks existing source schemas for new
						tables. It uses this job's stream routing, copies their existing
						rows, and keeps them updated on later runs.
					</p>
				</div>
				<Switch
					aria-label="Automatically add new tables"
					checked={draft.enabled}
					disabled={disabled || (!supported && !draft.enabled)}
					onChange={(enabled): void => onChange({ ...draft, enabled })}
				/>
			</div>
			{!supported && (
				<Alert
					type="info"
					showIcon
					message="Available only for PostgreSQL to Iceberg jobs."
				/>
			)}
			{draft.enabled && (
				<div className="flex flex-col gap-5">
					<Alert
						type="warning"
						showIcon
						message="What happens when this is on"
						description={
							<ul className="list-disc space-y-1 pl-4">
								<li>
									OLake only adds tables it has not seen before. Tables already
									listed in this job but not selected stay unselected.
								</li>
								<li>
									A schema groups tables in a source database. Existing catalog
									schemas are included automatically; brand-new schemas are not.
								</li>
								<li>
									Copying a large new table can delay updates to other tables in
									this job. PostgreSQL may need to keep more change logs until
									the copy finishes.
								</li>
								<li>
									Tables that cannot be copied safely are skipped, for example
									because of missing access, a required primary key, or a
									destination name already in use. The job logs explain why.
								</li>
								<li>
									OLake copies the current data in the table. It does not
									recover a complete history of changes made before the table
									was added.
								</li>
							</ul>
						}
					/>
					{catalog?.error && (
						<Alert
							type="error"
							showIcon
							message={catalog.error}
						/>
					)}
				</div>
			)}
			{draft.enabled && capabilities.error && (
				<Alert
					type="warning"
					showIcon
					message={capabilities.error}
				/>
			)}
			<h4 className="mb-2 mt-5 text-sm font-medium">Settings for new tables</h4>
			<p className="mb-4 text-sm text-gray-600">
				Choose how OLake should copy newly added tables. The switch above
				controls automatic enrollment; these defaults do not change tables
				already in the job. All columns that exist when a table is added are
				copied.
			</p>
			<div className="grid grid-cols-1 gap-4 md:grid-cols-2">
				<BooleanSetting
					label="Normalization for new tables"
					description="Enabled stores source fields as separate columns. Disabled stores each source row as JSON."
					value={draft.defaults.normalization}
					trueLabel="Enabled"
					falseLabel="Disabled"
					disabled={controlsDisabled}
					onChange={(normalization): void =>
						onChange({
							...draft,
							defaults: { ...draft.defaults, normalization },
						})
					}
				/>
				<BooleanSetting
					label="Write mode for new tables"
					description="Upsert keeps the latest version of each row and applies deletes. Append keeps incoming changes as separate rows."
					value={draft.defaults.append_mode}
					trueLabel="Append"
					falseLabel="Upsert"
					disabled={controlsDisabled}
					onChange={(append_mode): void =>
						onChange({
							...draft,
							defaults: { ...draft.defaults, append_mode },
						})
					}
				/>
				<label className="flex flex-col gap-2 text-sm text-gray-700">
					Delete format for new tables
					<Select<UpsertType>
						aria-label="Delete format for new tables"
						value={draft.defaults.update_type}
						placeholder="Select a delete format"
						disabled={controlsDisabled}
						options={deleteOptions}
						onChange={(update_type): void =>
							onChange({
								...draft,
								defaults: { ...draft.defaults, update_type },
							})
						}
					/>
					<span className="text-xs text-gray-600">
						Used by Upsert to apply updates and deletes in Iceberg. Choose a
						format that your query engines support.
					</span>
				</label>
				<BooleanSetting
					label="Automatically sync new columns"
					description="Enabled includes columns added to these tables later. Disabled keeps the original column selection until you change it."
					value={draft.defaults.sync_new_columns}
					trueLabel="Enabled"
					falseLabel="Disabled"
					disabled={controlsDisabled}
					onChange={(sync_new_columns): void =>
						onChange({
							...draft,
							defaults: { ...draft.defaults, sync_new_columns },
						})
					}
				/>
				<BooleanSetting
					label="Remove columns dropped from source"
					description={`${DROP_REMOVED_COLUMNS_HELP_TEXT} Requires Normalization.`}
					value={draft.defaults.drop_removed_columns ?? false}
					trueLabel="Enabled"
					falseLabel="Disabled"
					disabled={controlsDisabled || !draft.defaults.normalization}
					onChange={(drop_removed_columns): void =>
						onChange({
							...draft,
							defaults: { ...draft.defaults, drop_removed_columns },
						})
					}
				/>
			</div>
		</section>
	)
}

export default AutoTableEnrollmentCard
