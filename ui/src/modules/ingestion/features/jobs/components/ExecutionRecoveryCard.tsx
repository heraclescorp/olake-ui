import { useMutation, useQueryClient } from "@tanstack/react-query"
import { Alert, Button } from "antd"
import { Link } from "react-router"

import { jobsKeys } from "../constants"
import { executionRecoveryKey } from "../hooks/useJobExecutionRecovery"
import { jobService } from "../services"
import { JobExecutionRecovery } from "../types"

interface ExecutionRecoveryCardProps {
	jobId: string
	recovery?: JobExecutionRecovery
	loading: boolean
	error: Error | null
	onRefresh: () => void
}

const ExecutionRecoveryCard = ({
	jobId,
	recovery,
	loading,
	error,
	onRefresh,
}: ExecutionRecoveryCardProps): JSX.Element | null => {
	const queryClient = useQueryClient()
	const reconcile = useMutation({
		mutationFn: (): Promise<JobExecutionRecovery> =>
			jobService.reconcileExecution(jobId),
		onSuccess: (result: JobExecutionRecovery): void => {
			queryClient.setQueryData(executionRecoveryKey(jobId), result)
		},
	})
	const resume = useMutation({
		mutationKey: jobsKeys.all(),
		mutationFn: async (): Promise<void> => {
			await jobService.activateJob(jobId, true)
		},
		onSuccess: (): void => onRefresh(),
	})
	if (
		!error &&
		recovery?.status === "idle" &&
		!recovery.schedule_paused &&
		!reconcile.isPending
	) {
		return null
	}
	const pending = reconcile.isPending || recovery?.status === "reconciling"
	return (
		<section
			aria-labelledby="execution-recovery-heading"
			className="mb-6 rounded-xl border border-gray-200 bg-white p-6"
		>
			<h3
				id="execution-recovery-heading"
				className="mb-2 font-medium"
			>
				Execution recovery
			</h3>
			<div
				role="status"
				aria-live="polite"
			>
				<Alert
					type={error ? "error" : "warning"}
					showIcon
					message={
						pending
							? "Reconciliation in progress — not yet released"
							: recovery?.status === "idle"
								? "Execution retired — schedule remains paused"
								: "Execution ownership is protected"
					}
					description={
						error
							? `Unable to check execution ownership. ${error.message} Refresh status before starting another sync or table resync.`
							: recovery?.message || "Checking durable execution ownership…"
					}
				/>
			</div>
			{recovery?.workflow_id && (
				<p className="my-3 break-all text-sm text-text-tertiary">
					Operation: {recovery.workflow_id}. Phase:{" "}
					{recovery.phase || "unknown"}.
					{recovery.outcome && <> Outcome: {recovery.outcome}.</>}
					{recovery.temporal_status && (
						<> Workflow: {recovery.temporal_status}.</>
					)}
				</p>
			)}
			{(recovery?.last_error || reconcile.error || resume.error) && (
				<Alert
					className="my-3"
					type="error"
					message="Outcome remains unconfirmed"
					description={
						resume.error?.message ||
						reconcile.error?.message ||
						recovery?.last_error
					}
				/>
			)}
			<div className="mt-4 flex flex-wrap items-center gap-4">
				{recovery?.status === "idle" && recovery.schedule_paused && (
					<Button
						disabled={loading || resume.isPending}
						loading={resume.isPending}
						onClick={(): void => resume.mutate()}
					>
						Resume schedule
					</Button>
				)}
				<Button
					disabled={!recovery?.can_reconcile || loading || pending}
					loading={pending}
					onClick={(): void => reconcile.mutate()}
				>
					Reconcile execution
				</Button>
				<Button
					loading={loading}
					onClick={onRefresh}
				>
					Refresh status
				</Button>
				<Link to={`/jobs/${jobId}/history`}>
					View execution history and logs
				</Link>
			</div>
			<p className="mt-3 text-sm text-text-tertiary">
				Cancellation alone does not prove remote writes stopped. Reconcile
				reuses the original operation; it does not start a new table rebuild or
				override unknown outcomes. After proven retirement, explicitly request
				the next sync or table resync. Published operations must complete their
				bound handoff.
			</p>
		</section>
	)
}

export default ExecutionRecoveryCard
