import { Alert, Button, Checkbox, Input, Modal, message } from "antd"
import { useState } from "react"

import { API_CONFIG } from "@/config/apiConfig"
import { api } from "@/core/api"

import { Job } from "../types"

type Preview = {
	lsn: string
	current_lsn: string
	streams: string[]
	revision: string
	state: unknown
}

// Server derives targets from saved configuration and rechecks execution fences
// at preview and commit. The browser never constructs a checkpoint or stream list.
export default function ForceLSNModal({
	job,
	onClose,
}: {
	job: Job
	onClose: () => void
}) {
	const [lsn, setLSN] = useState("")
	const [preview, setPreview] = useState<Preview>()
	const [baseline, setBaseline] = useState(false)
	const [confirmation, setConfirmation] = useState("")
	const [busy, setBusy] = useState(false)
	const [error, setError] = useState("")
	const [saved, setSaved] = useState<{ backup_id: number; lsn: string }>()
	const url = `${API_CONFIG.ENDPOINTS.ETL.JOBS(API_CONFIG.PROJECT_ID)}/${job.id}/force-lsn`
	const submit = async (apply: boolean) => {
		setBusy(true)
		setError("")
		try {
			if (apply && preview) {
				const r = await api.put<{ backup_id: number; lsn: string }>(url, {
					lsn: preview.lsn,
					revision: preview.revision,
					confirm_baseline: baseline,
					confirmation,
				})
				setSaved(r.data)
				message.success(
					"Checkpoint overwritten. Job remains paused; no sync was started.",
				)
			} else {
				const r = await api.post<Preview>(`${url}/preview`, { lsn })
				setPreview(r.data)
			}
		} catch {
			setError(
				"Unable to update checkpoint. The job must be paused and idle. If state or selections changed, close this dialog and preview again. See the API error notification for details.",
			)
			setPreview(undefined)
			setBaseline(false)
			setConfirmation("")
		} finally {
			setBusy(false)
		}
	}
	return (
		<Modal
			open
			title={`Force set CDC LSN — ${job.name}`}
			onCancel={() => !busy && onClose()}
			closable={!busy}
			maskClosable={false}
			width={760}
			footer={
				saved ? (
					<Button onClick={onClose}>Close</Button>
				) : (
					<>
						<Button
							disabled={busy}
							onClick={onClose}
						>
							Cancel
						</Button>
						{!preview ? (
							<Button
								loading={busy}
								disabled={
									!/^[0-9a-fA-F]{1,8}\/[0-9a-fA-F]{1,8}$/.test(lsn.trim()) ||
									/^0+\/0+$/.test(lsn.trim())
								}
								onClick={() => submit(false)}
							>
								Preview checkpoint
							</Button>
						) : (
							<Button
								danger
								type="primary"
								loading={busy}
								disabled={!baseline || confirmation !== `FORCE ${preview.lsn}`}
								onClick={() => submit(true)}
							>
								Force set LSN
							</Button>
						)}
					</>
				)
			}
		>
			<div className="flex flex-col gap-4">
				{saved ? (
					<Alert
						type="success"
						showIcon
						message={`Checkpoint set to ${saved.lsn}`}
						description={`Previous state saved in database backup #${saved.backup_id}. The job is still paused. Resume separately only after checking the slot and WAL retention.`}
					/>
				) : (
					<>
						<Alert
							type="error"
							showIcon
							message="Danger: this overwrites the OLake recovery checkpoint"
							description="Moving forward can permanently skip changes. Moving backward can replay changes and cause duplicates, especially in Append mode. All currently selected CDC tables will be marked as backfill complete, so missing baseline rows will NOT be fetched."
						/>
						<Alert
							type="warning"
							showIcon
							message="This does not change the PostgreSQL replication slot"
							description="This action does not validate slot position, WAL availability, destination contents, or Iceberg metadata. It cannot rewind a slot or bypass connector LSN mismatch checks. No tables are cleared, no source data is copied, and no sync is started. The previous state is backed up atomically in the UI database."
						/>
						<label htmlFor="force-lsn">New CDC LSN</label>
						<Input
							id="force-lsn"
							value={lsn}
							disabled={busy}
							placeholder="761/F6ADE70"
							onChange={e => {
								setLSN(e.target.value)
								setPreview(undefined)
								setBaseline(false)
								setConfirmation("")
							}}
						/>
						{preview && (
							<>
								<p>
									Current saved LSN:{" "}
									<code>{preview.current_lsn || "Missing"}</code> → New LSN:{" "}
									<code>{preview.lsn}</code>
								</p>
								<p>
									<strong>{preview.streams.length} selected CDC tables</strong>{" "}
									will be marked backfill complete. Disabled, unselected and
									non-CDC tables are not added to this list. No manual stream
									list is needed.
								</p>
								<details>
									<summary>View affected tables</summary>
									<ul className="max-h-40 overflow-auto">
										{preview.streams.map(s => (
											<li key={s}>{s}</li>
										))}
									</ul>
								</details>
								<details>
									<summary>View proposed state</summary>
									<pre className="max-h-40 overflow-auto">
										{JSON.stringify(preview.state, null, 2)}
									</pre>
								</details>
								<Checkbox
									checked={baseline}
									disabled={busy}
									onChange={e => setBaseline(e.target.checked)}
								>
									I confirm every listed table has a complete baseline at this
									LSN, and I have independently checked the slot and retained
									WAL.
								</Checkbox>
								<label htmlFor="force-confirmation">
									Type FORCE {preview.lsn} to confirm
								</label>
								<Input
									id="force-confirmation"
									value={confirmation}
									disabled={busy}
									onChange={e => setConfirmation(e.target.value)}
								/>
							</>
						)}
						{error && (
							<Alert
								type="error"
								message={error}
							/>
						)}
					</>
				)}
			</div>
		</Modal>
	)
}
