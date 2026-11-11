import { useMemo } from "react"

import { useJobConfigurationStore, useStreamSelectionStore } from "../stores"
import {
	AutoTableEnrollmentDraft,
	EnrollmentCapabilities,
	EnrollmentCatalog,
	enrollmentCatalogFromStreams,
	enrollmentDraftFromPolicy,
	isAutoTableEnrollmentSupported,
} from "../utils/autoTableEnrollment"
import { useEnrollmentCapabilities } from "./useEnrollmentCapabilities"

interface EnrollmentConfiguration {
	draft: AutoTableEnrollmentDraft
	catalog: EnrollmentCatalog
	capabilities: EnrollmentCapabilities
	supported: boolean
	setDraft: (draft: AutoTableEnrollmentDraft) => void
}

export const useEnrollmentConfiguration = (): EnrollmentConfiguration => {
	const {
		selectedSource,
		selectedDestination,
		advancedSettings,
		enrollmentDraft,
		enrollmentCatalog,
		setEnrollmentDraft,
	} = useJobConfigurationStore()
	const streamsData = useStreamSelectionStore(state => state.streamsData)
	const supported = isAutoTableEnrollmentSupported(
		selectedSource?.type ?? "",
		selectedDestination?.type ?? "",
	)
	const capabilities = useEnrollmentCapabilities(
		selectedSource?.type ?? "",
		selectedSource?.version ?? "",
		advancedSettings?.target_query_engines ?? [],
	)
	const catalog = useMemo(
		() =>
			streamsData
				? enrollmentCatalogFromStreams(streamsData)
				: (enrollmentCatalog ?? enrollmentCatalogFromStreams(null)),
		[streamsData, enrollmentCatalog],
	)
	return {
		supported,
		capabilities,
		draft:
			enrollmentDraft ??
			enrollmentDraftFromPolicy(
				advancedSettings?.auto_table_enrollment,
				supported,
				capabilities.error ? undefined : capabilities.availableUpdateTypes,
			),
		catalog,
		setDraft: setEnrollmentDraft,
	}
}
