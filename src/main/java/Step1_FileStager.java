import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.util.StopWatch;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.glassfish.jaxb.core.api.impl.NameConverter;
import org.hibernate.search.engine.environment.thread.impl.ThreadPoolProviderImpl;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

public class Step1_FileStager {
	private static final Logger ourLog = LoggerFactory.getLogger(Step1_FileStager.class);

	private static final FhirContext ourCtx = FhirContext.forR4Cached();

	public static void main(String[] args) throws Exception {
		ThreadFactory threadFactory = new BasicThreadFactory.Builder()
				.namingPattern("worker-%d")
				.daemon(true)
				.priority(Thread.MIN_PRIORITY)
				.build();
		RejectedExecutionHandler rejectionHandler = new ThreadPoolProviderImpl.BlockPolicy();
		ExecutorService executor = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(500), threadFactory, rejectionHandler);

		Map<String, AtomicInteger> resourceTypeToCount = Collections.synchronizedMap(new HashMap<>());

		Collection<File> inputFiles = FileUtils.listFiles(new File("src/main/data/new_synthea_files"), new String[]{"json"}, false);
		int fileIndex = 0;
		List<Future<?>> futures = new ArrayList<>();
		int totalFiles = inputFiles.size();

		StopWatch sw = new StopWatch();
		for (var next : inputFiles) {
			int finalFileIndex = fileIndex;

			Callable<Void> task = () -> {
				ourLog.info("Processing file {}/{}: {} - {}/sec ETA: {}", finalFileIndex, totalFiles, next.getName(), sw.formatThroughput(finalFileIndex, TimeUnit.SECONDS), sw.getEstimatedTimeRemaining(finalFileIndex, totalFiles));
				Bundle bundle;
				try (Reader reader = new BufferedReader(new FileReader(next))) {
					bundle = ourCtx.newJsonParser().parseResource(Bundle.class, reader);
				}

				List<Resource> resources = new ArrayList<>();
				for (Iterator<Bundle.BundleEntryComponent> iter = bundle.getEntry().iterator(); iter.hasNext(); ) {
					Bundle.BundleEntryComponent bundleEntryComponent = iter.next();
					Resource resource = bundleEntryComponent.getResource();
					if (resource != null) {
						if (next.getName().startsWith("practitionerInformation") || next.getName().startsWith("hospitalInformation")) {
							resources.add(resource);
							continue;
						}

						var resourceType = ourCtx.getResourceType(resource);
						switch (resourceType) {
							case "Patient": {
								Patient p = (Patient) resource;
								resources.add(p);
								break;
							}
							case "Observation": {
								Observation o = (Observation) resource;
								o.setEncounter(null);
								resources.add(o);
								break;
							}
							default:
								iter.remove();
						}

					}
				}

				for (var nextResource : resources) {
					AtomicInteger count = resourceTypeToCount.computeIfAbsent(ourCtx.getResourceType(nextResource), t -> new AtomicInteger());
					count.incrementAndGet();
				}

				File targetFile = new File("src/main/data/staged_synthea_files", next.getName() + ".gz");
				try (FileOutputStream fos = new FileOutputStream(targetFile, false)) {
					try (BufferedOutputStream bos = new BufferedOutputStream(fos)) {
						try (GZIPOutputStream gos = new GZIPOutputStream(bos)) {
							try (OutputStreamWriter w = new OutputStreamWriter(gos, StandardCharsets.UTF_8)) {
								ourCtx.newJsonParser().encodeResourceToWriter(bundle, w);
							}
						}
					}
				}

				next.delete();

				return null;
			};

			futures.add(executor.submit(task));

			fileIndex++;

		}

		for (var next : futures) {
			next.get();
		}

		resourceTypeToCount.keySet().stream().sorted().forEach(t -> ourLog.info("Count {} -> {}", t, resourceTypeToCount.get(t).get()));

		executor.shutdown();
	}
}
