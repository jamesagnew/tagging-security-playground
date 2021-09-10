import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.TemporalPrecisionEnum;
import ca.uhn.fhir.rest.api.Constants;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.gclient.QuantityClientParam;
import ca.uhn.fhir.rest.gclient.ReferenceClientParam;
import ca.uhn.fhir.rest.gclient.StringClientParam;
import ca.uhn.fhir.rest.gclient.TokenClientParam;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Snapshot;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Step3_Queries {

	private static final Logger ourLog = LoggerFactory.getLogger(Step3_Queries.class);
	private static final MetricRegistry ourMetricRegistry = new MetricRegistry();
	private static FhirContext ourCtx = FhirContext.forR4();
	private static IGenericClient ourClient;
	private static List<Pair<String, String>> ourNamePairs;
	private static ArrayList<Encounter> ourEncounters;
	private static List<BaseTest> ourTasks = new ArrayList<>();
	private static DecimalFormat ourDecimalFormat = new DecimalFormat("#.##");

	abstract static class BaseTest {

		private final Histogram myElapsedMillisHistogram;
		private final Histogram myResultsHistogram;
		protected int myExecuteCount = 0;
		BaseTest() {
			myElapsedMillisHistogram = ourMetricRegistry.histogram(getName() + "-elapsed", () -> new Histogram(new SlidingWindowReservoir(100)));
			myResultsHistogram = ourMetricRegistry.histogram(getName() + "-results", () -> new Histogram(new SlidingWindowReservoir(100)));
		}

		public Histogram getResultsHistogram() {
			return myResultsHistogram;
		}

		abstract String getName();

		void run() {
			for (int i = 0; i < 10; i++) {
				long startTime = System.nanoTime();
				Bundle outcome = doExecuteQuery();
				long elapsedNanos = System.nanoTime() - startTime;
				long elapsedMillis = elapsedNanos / 1000000;
				myElapsedMillisHistogram.update(elapsedMillis);
				myResultsHistogram.update(outcome.getEntry().size());
				myExecuteCount++;

			}
		}

		protected abstract Bundle doExecuteQuery();

		public Histogram getElapsedMillisHistogram() {
			return myElapsedMillisHistogram;
		}
	}

	static class FindAllPatientsWithSpecificNameTest extends BaseTest {

		@Override
		String getName() {
			return "PTS_WITH_NAME";
		}

		@Override
		protected Bundle doExecuteQuery() {
			int nameIndex = (int) ((double) ourNamePairs.size() * Math.random());
			Pair<String, String> namePair = ourNamePairs.get(nameIndex);
			return ourClient
				.search()
				.forResource("Patient")
				.where(new StringClientParam("given").contains().value(namePair.getLeft()))
				.and(new StringClientParam("family").contains().value(namePair.getRight()))
				.returnBundle(Bundle.class)
				.execute();
		}
	}

	static class FindAllPatientsWithTagTest extends BaseTest {

		@Override
		String getName() {
			return "PTS_WITH_TAG";
		}

		@Override
		protected Bundle doExecuteQuery() {
			String tag = PlaygroundConstants.randomTag();
			return ourClient
				.search()
				.forResource("Patient")
				.where(new StringClientParam("_profile").contains().value(tag))
				.returnBundle(Bundle.class)
				.execute();
		}
	}

	static class FindObservationsAboveThreasholdWithTagTest extends BaseTest {

		@Override
		String getName() {
			return "OBS_ABOVE_THRSHOLD_WITH_TAG";
		}

		@Override
		protected Bundle doExecuteQuery() {
			String tag = PlaygroundConstants.randomTag();
			return ourClient
				.search()
				.forResource("Observation")
				.where(new TokenClientParam("code").exactly().systemAndCode("http://loinc.org", "29463-7"))
				.and(new QuantityClientParam("value-quantity").greaterThan().number(90).andUnits("http://unitsofmeasure.org", "kg"))
				.and(new StringClientParam("_profile").contains().value(tag))
				.returnBundle(Bundle.class)
				.execute();
		}
	}

	static class FindEncountersForProviderWithPatientTag extends BaseTest {

		@Override
		String getName() {
			return "ENCS_FOR_PROVIDER_WITH_PT_TAG";
		}

		@Override
		protected Bundle doExecuteQuery() {
			String tag = PlaygroundConstants.randomTag();
			Encounter encounter = ourEncounters.get((int) ((double) ourEncounters.size() * Math.random()));
			String practitioner = encounter.getParticipantFirstRep().getIndividual().getReference();

			return ourClient
				.search()
				.forResource("Encounter")
				.where(new ReferenceClientParam("practitioner").hasId(practitioner))
				.and(new StringClientParam("_profile").contains().value(tag))
				.returnBundle(Bundle.class)
				.execute();
		}
	}

	static class FindEncountersOnDateWithPatientTag extends BaseTest {

		@Override
		String getName() {
			return "ENCS_ON_DATE_WITH_PT_TAG";
		}

		@Override
		protected Bundle doExecuteQuery() {
			String tag = PlaygroundConstants.randomTag();
			Encounter encounter = ourEncounters.get((int) ((double) ourEncounters.size() * Math.random()));
			DateTimeType encounterStart = new DateTimeType(encounter.getPeriod().getStartElement().asStringValue());
			encounterStart.setPrecision(TemporalPrecisionEnum.DAY);

			return ourClient
				.search()
				.forResource("Encounter")
				.where(new ReferenceClientParam("date").hasId(encounterStart.getValueAsString()))
				.and(new StringClientParam("_profile").contains().value(tag))
				.returnBundle(Bundle.class)
				.execute();
		}
	}

	public static void main(String[] args) {
		ourCtx.getRestfulClientFactory().setSocketTimeout(10000000);
		ourClient = ourCtx.newRestfulGenericClient(PlaygroundConstants.FHIR_ENDPOINT_BASE_URL);
		ourClient.registerInterceptor(new BasicAuthInterceptor(PlaygroundConstants.FHIR_ENDPOINT_CREDENTIALS));

		preLoadNames();
		preLoadEncounters();

		ourTasks.add(new FindAllPatientsWithTagTest());
		ourTasks.add(new FindAllPatientsWithSpecificNameTest());
		ourTasks.add(new FindObservationsAboveThreasholdWithTagTest());
		ourTasks.add(new FindEncountersForProviderWithPatientTag());
		ourTasks.add(new FindEncountersOnDateWithPatientTag());

		StringBuilder headerRow = new StringBuilder();
		for (var nextTask : ourTasks) {
			headerRow.append(nextTask.getName()).append(",");
//			headerRow.append(nextTask.getName() + "_RES_MEDIAN").append(",");
		}
		writeCsvLine(headerRow);

		while (true) {
			StringBuilder csvRow = new StringBuilder();
			for (var nextTask : ourTasks) {
				nextTask.run();
				Snapshot elapsedSnapshot = nextTask.getElapsedMillisHistogram().getSnapshot();
				Snapshot resultsSnapshot = nextTask.getResultsHistogram().getSnapshot();
				csvRow.append(formatNumber(elapsedSnapshot.getMedian())).append(",");
//				csvRow.append(formatNumber(resultsSnapshot.getMedian())).append(",");
			}
			writeCsvLine(csvRow);
		}

	}

	private static String formatNumber(double theInput) {
		return ourDecimalFormat.format(theInput);
	}

	private static void writeCsvLine(StringBuilder theHeaderRow) {
		ourLog.info(theHeaderRow.toString());
	}

	private static void preLoadEncounters() {
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
				.map(t -> Pair.of(t.getGivenAsSingleString(), t.getFamily()))
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
