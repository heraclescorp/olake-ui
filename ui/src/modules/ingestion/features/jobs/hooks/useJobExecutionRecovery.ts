import { useQuery, UseQueryResult } from "@tanstack/react-query"

import { jobsKeys } from "../constants"
import { jobService } from "../services"
import { JobExecutionRecovery } from "../types"

export const executionRecoveryKey = (jobId: string): readonly string[] => [
	...jobsKeys.detail(jobId),
	"execution-recovery",
]

export const useJobExecutionRecovery = (
	jobId: string,
): UseQueryResult<JobExecutionRecovery, Error> =>
	useQuery({
		queryKey: executionRecoveryKey(jobId),
		queryFn: (): Promise<JobExecutionRecovery> =>
			jobService.getExecutionRecovery(jobId),
		enabled: !!jobId,
		refetchInterval: (query): number | false => {
			const status = query.state.data?.status
			return status === "active" || status === "reconciling" ? 2000 : false
		},
	})
