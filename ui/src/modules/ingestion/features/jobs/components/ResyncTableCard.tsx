import { Alert, Button, Modal, Select, message } from "antd"
import { useMemo, useState } from "react"
import { useNavigate } from "react-router"

import {
	SelectedStreams,
	StreamsDataStructure,
	SyncMode,
} from "@/modules/ingestion/common/types"

import { useResyncTable } from "../hooks"
import { Job, ResyncTableRequest } from "../types"
import { formatDestinationPath, isStreamsV2Job } from "../utils"

interface ResyncTableCardProps {
	job: Job
	disabledReason?: string
}

interface ResyncTableOption extends ResyncTableRequest {
	value: string
	label: string
	destinationLabel: string
	disabled: boolean
}

interface SavedTableOptions {
	options: ResyncTableOption[]
	error?: string
}

const BACKFILL_SUPPORTED: Record<SyncMode, boolean> = {
	[SyncMode.CDC]: true,
	[SyncMode.FULL_REFRESH]: true,
	[SyncMode.INCREMENTAL]: true,
	[SyncMode.STRICT_CDC]: false,
}

const getSavedTableOptions = (job: Job): SavedTableOptions => {
	try {
		const split = isStreamsV2Job(job)
		const selected: SelectedStreams = JSON.parse(
			(split ? job.selected_streams_config : job.streams_config) || "{}",
		)
		const available: StreamsDataStructure = JSON.parse(
			(split ? job.available_streams_config : job.streams_config) || "{}",
		)
		if (!selected.selected_streams || !Array.isArray(available.streams)) {
			return { options: [], error: "Saved table configuration is unavailable." }
		}
		const options = Object.entries(selected.selected_streams).flatMap(
			([namespace, streams]): ResyncTableOption[] =>
				streams
					.filter(stream => !stream.disabled)
					.map(stream => {
						const catalogStream = available.streams.find(
							entry =>
								(entry.stream.namespace || "") === namespace &&
								entry.stream.name === stream.stream_name,
						)
						const syncMode = stream.sync_mode ?? catalogStream?.stream.sync_mode
						const canBackfill =
							syncMode !== undefined && BACKFILL_SUPPORTED[syncMode] === true
						const name = namespace
							? `${namespace}.${stream.stream_name}`
							: stream.stream_name
						return {
							namespace,
							stream_name: stream.stream_name,
							value: JSON.stringify([namespace, stream.stream_name]),
							label: canBackfill ? name : `${name} (backfill unavailable)`,
							destinationLabel:
								formatDestinationPath(
									stream.destination_database ??
										catalogStream?.stream.destination_database,
									stream.destination_table ??
										catalogStream?.stream.destination_table,
								) ??
								"Destination table path unavailable in saved configuration",
							disabled: !canBackfill,
						}
					}),
		)
		return { options }
	} catch {
		return {
			options: [],
			error: "Saved table configuration could not be read.",
		}
	}
}

const ResyncTableCard = ({
	job,
	disabledReason,
}: ResyncTableCardProps): JSX.Element => {
	const navigate = useNavigate()
	const { mutate: resyncTable, isPending } = useResyncTable()
	const [selectedValue, setSelectedValue] = useState<string>()
	const [confirmOpen, setConfirmOpen] = useState(false)
	const { options, error } = useMemo(() => getSavedTableOptions(job), [job])
	const selected = options.find(option => option.value === selectedValue)
	const unavailableReason =
		disabledReason ||
		(job.source.type !== "postgres" || job.destination.type !== "iceberg"
			? "Uninterrupted table resync supports PostgreSQL sources with Iceberg destinations only. Clear Destination is a separate destructive operation."
			: undefined) ||
		error ||
		(!options.length
			? "No tables are selected in the saved job configuration."
			: undefined)
	const disabled =
		!!unavailableReason || !selected || selected.disabled || isPending

	const handleConfirm = (): void => {
		if (disabled || !selected) {
			message.error(
				unavailableReason || "Select a table that supports backfill.",
			)
			return
		}
		resyncTable(
			{
				jobId: job.id.toString(),
				request: {
					namespace: selected.namespace,
					stream_name: selected.stream_name,
				},
			},
			{
				onSuccess: (): void => {
					setConfirmOpen(false)
					navigate(`/jobs/${job.id}/history`)
				},
			},
		)
	}

	return (
		<section
			aria-labelledby="resync-table-heading"
			className="mb-6 rounded-xl border border-gray-200 bg-white p-6"
		>
			<h3
				id="resync-table-heading"
				className="mb-2 font-medium"
			>
				Resync table
			</h3>
			<p className="mb-4 text-sm text-text-tertiary">
				For PostgreSQL sources with Iceberg destinations, rebuild one selected
				table using the saved job configuration while its current committed data
				remains readable. On success, publish one atomic replacement in the same
				destination table. Other source and destination types are not supported.
			</p>
			<div className="flex flex-wrap items-center gap-4">
				<Select
					aria-label="Table to resync"
					className="w-full max-w-md"
					showSearch
					optionFilterProp="label"
					placeholder="Select a saved table"
					options={options}
					value={selectedValue}
					onChange={setSelectedValue}
					disabled={!!unavailableReason || isPending}
				/>
				<Button
					disabled={disabled}
					loading={isPending}
					onClick={(): void => setConfirmOpen(true)}
				>
					Resync table
				</Button>
			</div>
			{unavailableReason && (
				<p
					role="status"
					className="mt-3 text-sm text-text-tertiary"
				>
					{unavailableReason}
				</p>
			)}
			<Modal
				title="Confirm table resync"
				open={confirmOpen}
				onCancel={(): void => setConfirmOpen(false)}
				onOk={handleConfirm}
				okText="Start table rebuild"
				okButtonProps={{ disabled }}
				confirmLoading={isPending}
				cancelButtonProps={{ disabled: isPending }}
				closable={!isPending}
				maskClosable={!isPending}
			>
				<p className="mb-3">
					Resync <strong>{selected?.label}</strong> from source{" "}
					<strong>{job.source.name}</strong> to destination{" "}
					<strong>{job.destination.name}</strong>, table{" "}
					<strong>{selected?.destinationLabel}</strong>?
				</p>
				<Alert
					type="info"
					showIcon
					message="Existing data remains readable until atomic publication."
				/>
				<ul className="my-4 list-disc space-y-2 pl-5">
					<li>
						The source is not changed. The existing table identity and committed
						data are retained while this table is rebuilt and caught up with CDC.
					</li>
					<li>
						A rebuild failure before publication leaves the old data intact.
						After publication, completion and checkpoint persistence may need
						reconciliation. The job stays paused if completion cannot be confirmed.
					</li>
					<li>
						The full saved catalog and state are used, preserving other streams’
						checkpoints and ordinary CDC behavior. Scheduled syncs wait until the
						operation completes successfully.
					</li>
					<li>
						Readers such as Snowflake see the replacement after their usual refresh.
						Unsupported runtimes, destinations, or schema transitions fail safely;
						this does not resolve existing timestamp-default compatibility limits.
					</li>
					<li>
						Each staging attempt is limited to 12 hours. Longer rebuilds stop
						safely rather than risking expiration of unpublished staging files.
					</li>
					<li>
						Column-only historical backfill is unsupported. Save column
						selection changes before resyncing the full table.
					</li>
				</ul>
				{unavailableReason && (
					<Alert
						type="error"
						message={unavailableReason}
					/>
				)}
			</Modal>
		</section>
	)
}

export default ResyncTableCard
