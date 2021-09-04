import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.Constants;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Step3_Queries {

	private static final Logger ourLog = LoggerFactory.getLogger(Step3_Queries.class);
	private static FhirContext ourCtx = FhirContext.forR4();
	private static IGenericClient ourClient;
	private static List<Pair<String, String>> ourNamePairs;
	private static ArrayList<Encounter> ourEncounters;

	public static void main(String[] args) {
		ourCtx.getRestfulClientFactory().setSocketTimeout(10000000);
		ourClient = ourCtx.newRestfulGenericClient(PlaygroundConstants.FHIR_ENDPOINT_BASE_URL);
		ourClient.registerInterceptor(new BasicAuthInterceptor(PlaygroundConstants.FHIR_ENDPOINT_CREDENTIALS));

		preLoadNames();


		ourLog.info("Searching for encounters to determine some search options");
		ourEncounters = new ArrayList<>();
		Bundle outcome = ourClient
			.search()
			.forResource("Encounter")
			.returnBundle(Bundle.class)
			.count(500)
			.execute();

		do {
			outcome
				.getEntry()
				.stream()
				.map(t -> (Encounter) t.getResource())
				.filter(t -> t.getMeta().getProfile().size() > 0)
				.forEach(t -> ourEncounters.add(t));
			if (outcome.getLink(Constants.LINK_NEXT) != null) {
				ourLog.info("Loading next page of Encounters");
				outcome = ourClient.loadPage().next(outcome).execute();
			} else {
				outcome = null;
			}
		} while (outcome != null && ourEncounters.size() < 1000);
	}

	private static void preLoadNames() {
		ourLog.info("Searching for patients to determine some searchable names");
		Bundle outcome = ourClient
			.search()
			.forResource("Patient")
			.returnBundle(Bundle.class)
			.count(500)
			.execute();
		ourNamePairs = new ArrayList<>();

		do {
			outcome.getEntry()
				.stream()
				.map(t -> ((Patient) t.getResource()).getNameFirstRep())
				.map(t -> Pair.of(t.getFamily(), t.getGivenAsSingleString()))
				.forEach(t -> ourNamePairs.add(t));
			if (outcome.getLink(Constants.LINK_NEXT) != null) {
				ourLog.info("Loading next page of Patients");
				outcome = ourClient.loadPage().next(outcome).execute();
			} else {
				outcome = null;
			}
		} while (outcome != null && ourNamePairs.size() < 1000);

		ourLog.info("Found {} name pairs", ourNamePairs.size());
		Validate.isTrue(ourNamePairs.size() >= 10);
	}

}
