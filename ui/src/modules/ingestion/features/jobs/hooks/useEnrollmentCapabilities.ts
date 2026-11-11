import { useAvailableQueryEngines } from "@/modules/ingestion/features/sources/hooks"
import { getConnectorInLowerCase } from "@/modules/ingestion/common/utils"

import {
	EnrollmentCapabilities,
	getEnrollmentCapabilities,
} from "../utils/autoTableEnrollment"

export const useEnrollmentCapabilities = (
	sourceType: string,
	sourceVersion: string,
	targetQueryEngines: string[],
): EnrollmentCapabilities => {
	const query = useAvailableQueryEngines(
		getConnectorInLowerCase(sourceType),
		sourceVersion,
	)
	if (query.isPending || query.isFetching) {
		return {
			availableUpdateTypes: [],
			error:
				"Wait for delete-format capabilities to load before saving automatic enrollment.",
		}
	}
	if (query.isError) {
		return {
			availableUpdateTypes: [],
			error:
				"Delete-format capabilities could not be loaded. Retry fetching query engines before saving automatic enrollment.",
		}
	}
	return getEnrollmentCapabilities(query.data, targetQueryEngines)
}
