import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.Constants;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.gclient.StringClientParam;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Snapshot;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Bundle;
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

		private final Histogram myHistogram;

		BaseTest() {
			myHistogram = ourMetricRegistry.histogram(getName(), () -> new Histogram(new SlidingWindowReservoir(100)));
		}

		abstract String getName();

		void run() {
			for (int i = 0; i < 10; i++) {
				long startTime = System.nanoTime();
				doExecuteQuery();
				long elapsedNanos = System.nanoTime() - startTime;
				long elapsedMillis = elapsedNanos / 1000000;
				myHistogram.update(elapsedMillis);
			}
		}

		protected abstract void doExecuteQuery();

		public Histogram getHistogram() {
			return myHistogram;
		}
	}

	static class FindAllPatientsWithTagTest extends BaseTest {

		@Override
		String getName() {
			return "FINDALLPTS";
		}

		@Override
		protected void doExecuteQuery() {
			String tag = PlaygroundConstants.randomTag();
			Bundle outcome = ourClient
				.search()
				.forResource("Patient")
				.where(new StringClientParam("_profile").contains().value(tag))
				.returnBundle(Bundle.class)
				.execute();
			ourLog.debug("{} returned {} results", getName(), outcome.getEntry().size());
		}
	}

	public static void main(String[] args) {
		ourCtx.getRestfulClientFactory().setSocketTimeout(10000000);
		ourClient = ourCtx.newRestfulGenericClient(PlaygroundConstants.FHIR_ENDPOINT_BASE_URL);
		ourClient.registerInterceptor(new BasicAuthInterceptor(PlaygroundConstants.FHIR_ENDPOINT_CREDENTIALS));

		preLoadNames();
		preLoadEncounters();

		ourTasks.add(new FindAllPatientsWithTagTest());

		StringBuilder headerRow = new StringBuilder();
		for (var nextTask : ourTasks) {
			headerRow.append(nextTask.getName()+"-mdn").append(",");
			headerRow.append(nextTask.getName()+"-99pct").append(",");
			headerRow.append(nextTask.getName()+"-75pct").append(",");
		}
		writeCsvLine(headerRow);

		while(true) {
			StringBuilder csvRow = new StringBuilder();
			for (var nextTask : ourTasks) {
				nextTask.run();
				Snapshot snapshot = nextTask.getHistogram().getSnapshot();
				csvRow.append(formatNumber(snapshot.getMedian())).append(",");
				csvRow.append(formatNumber(snapshot.get99thPercentile())).append(",");
				csvRow.append(formatNumber(snapshot.get75thPercentile())).append(",");
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
