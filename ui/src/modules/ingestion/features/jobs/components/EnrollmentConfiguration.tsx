import { useEnrollmentConfiguration } from "../hooks/useEnrollmentConfiguration"
import AutoTableEnrollmentCard from "./AutoTableEnrollmentCard"

const EnrollmentConfiguration = (): JSX.Element => {
	const { draft, catalog, capabilities, supported, setDraft } =
		useEnrollmentConfiguration()
	return (
		<AutoTableEnrollmentCard
			draft={draft}
			catalog={catalog.namespaces.length > 0 ? catalog : undefined}
			capabilities={capabilities}
			supported={supported}
			disabled={false}
			onChange={setDraft}
		/>
	)
}

export default EnrollmentConfiguration
