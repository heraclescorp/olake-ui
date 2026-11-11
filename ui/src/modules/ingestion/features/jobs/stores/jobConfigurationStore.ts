import { create } from "zustand"

import { Entity } from "@/modules/ingestion/common/types"

import { AdvancedSettings } from "../types"
import {
	AutoTableEnrollmentDraft,
	EnrollmentCatalog,
} from "../utils/autoTableEnrollment"

interface JobConfigurationState {
	jobName: string
	cronExpression: string
	selectedSource: Entity | null
	selectedDestination: Entity | null
	advancedSettings: AdvancedSettings | null
	savedAdvancedSettings: AdvancedSettings | null
	isEditMode: boolean
	enrollmentDraft: AutoTableEnrollmentDraft | null
	enrollmentCatalog: EnrollmentCatalog | null
}

interface JobConfigurationActions {
	setJobName: (name: string) => void
	setCronExpression: (expr: string) => void
	setSelectedSource: (source: Entity | null) => void
	setSelectedDestination: (destination: Entity | null) => void
	setAdvancedSettings: (settings: AdvancedSettings | null) => void
	setSavedAdvancedSettings: (settings: AdvancedSettings | null) => void
	setIsEditMode: (isEdit: boolean) => void
	setEnrollmentDraft: (draft: AutoTableEnrollmentDraft | null) => void
	setEnrollmentCatalog: (catalog: EnrollmentCatalog | null) => void
	reset: () => void
}

type JobConfigurationStore = JobConfigurationState & JobConfigurationActions

const initialState: JobConfigurationState = {
	jobName: "",
	cronExpression: "* * * * *",
	selectedSource: null,
	selectedDestination: null,
	advancedSettings: null,
	savedAdvancedSettings: null,
	isEditMode: false,
	enrollmentDraft: null,
	enrollmentCatalog: null,
}

export const useJobConfigurationStore = create<JobConfigurationStore>(set => ({
	...initialState,
	setJobName: jobName => set({ jobName }),
	setCronExpression: cronExpression => set({ cronExpression }),
	setSelectedSource: selectedSource => set({ selectedSource }),
	setSelectedDestination: selectedDestination => set({ selectedDestination }),
	setAdvancedSettings: advancedSettings => set({ advancedSettings }),
	setSavedAdvancedSettings: savedAdvancedSettings =>
		set({ savedAdvancedSettings }),
	setIsEditMode: isEditMode => set({ isEditMode }),
	setEnrollmentDraft: enrollmentDraft => set({ enrollmentDraft }),
	setEnrollmentCatalog: enrollmentCatalog => set({ enrollmentCatalog }),
	reset: () => set(initialState),
}))
