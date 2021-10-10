import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.search.reindex.ResourceReindexingSvcImpl;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.util.StopWatch;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang3.Validate;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
import org.hl7.fhir.r4.model.Bundle;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

@SuppressWarnings("BusyWait")
public class Step2_DataUploader {
	private static final Logger ourLog = LoggerFactory.getLogger(Step2_DataUploader.class);

	private static final FhirContext ourCtx = FhirContext.forR4Cached();
	private static final AtomicInteger ourUploadedCount = new AtomicInteger(0);
	private static final AtomicInteger ourActiveUploadsCount = new AtomicInteger(0);
	private static LinkedBlockingQueue<Runnable> ourWorkQueue;

	private static class UploadTask implements Callable<Void> {
		private final IGenericClient myClient;
		private final Bundle myInputBundle;
		private final long myBytesRead;
		private final long myTotalBytes;
		private final StopWatch mySw;
		private final int myFinalFileIndex;
		private final Queue<Future<?>> myFutures;
		private final ExecutorService myExecutor;
		private int myRetryCount = 0;

		public UploadTask(IGenericClient theClient, Bundle theInputBundle, long theBytesRead, long theTotalBytes, StopWatch theSw, int theFinalFileIndex, Queue<Future<?>> theFutures, ExecutorService theExecutor) {
			myClient = theClient;
			myInputBundle = theInputBundle;
			myBytesRead = theBytesRead;
			myTotalBytes = theTotalBytes;
			mySw = theSw;
			myFinalFileIndex = theFinalFileIndex;
			myFutures = theFutures;
			myExecutor = theExecutor;
		}

		@Override
		public Void call() throws Exception {
			try {

				int active = 0;
				try {
					active = ourActiveUploadsCount.incrementAndGet();
					myClient.transaction().withBundle(myInputBundle).execute();
				} finally {
					ourActiveUploadsCount.decrementAndGet();
				}

				int uploaded = ourUploadedCount.incrementAndGet();
				if (uploaded % 10 == 0) {
					ourLog.info("Uploaded {} - Have read {} of {} - {}/sec - {} active - ETA: {}", uploaded, FileUtils.byteCountToDisplaySize(myBytesRead), FileUtils.byteCountToDisplaySize(myTotalBytes), mySw.formatThroughput(uploaded, TimeUnit.SECONDS), active, mySw.getEstimatedTimeRemaining(myBytesRead, myTotalBytes));
				}
				return null;
			} catch (Exception e) {
				myRetryCount++;
				String msg = "Failure " + myRetryCount + " during upload of file at index " + myFinalFileIndex + ": " + e;
				if (myRetryCount <= 10) {
					//msg += " - Resubmitting";
					ourLog.warn(msg);
					//myFutures.add(myExecutor.submit(this));
					return null;
				}

				// FIXME: remove
				File tmpFile = File.createTempFile("failed-upload", ".json");
				try (FileWriter w = new FileWriter(tmpFile, StandardCharsets.UTF_8, false)) {
					ourCtx.newJsonParser().encodeResourceToWriter(myInputBundle, w);
				}
				msg += " - Failing input saved to " + tmpFile;
				ourLog.error(msg);
				return null;
			}
		}

	}

	public static void main(String[] args) throws Exception {
		ourWorkQueue = new LinkedBlockingQueue<>(5000);
		ExecutorService executor = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, ourWorkQueue, new ResourceReindexingSvcImpl.BlockPolicy());

		ourCtx.getRestfulClientFactory().setSocketTimeout(10000000);
		IGenericClient client = ourCtx.newRestfulGenericClient(PlaygroundConstants.FHIR_ENDPOINT_BASE_URL);
		client.registerInterceptor(new BasicAuthInterceptor(PlaygroundConstants.FHIR_ENDPOINT_CREDENTIALS));
//		client.registerInterceptor(new LoggingInterceptor(false));

		//uploadFile(Step1_FileStager.META_FILES_NDJSON_GZ, executor, client);
		uploadFile(Step1_FileStager.PATIENT_FILES_NDJSON_GZ, executor, client);

		executor.shutdown();
	}

	private static void uploadFile(String theFilename, ExecutorService executor, IGenericClient client) throws IOException, ExecutionException, InterruptedException {
		File inputFile = new File("src/main/data/staged_synthea_files/" + theFilename);
		long totalBytes = FileUtils.sizeOf(inputFile);
		Validate.isTrue(inputFile.exists());

		ourLog.info("Scanning file for line count: {}", theFilename);

		ourLog.info("Beginning upload for file: {}", theFilename);

		StopWatch sw = new StopWatch();
		try (FileInputStream fis = new FileInputStream(inputFile)) {
			try (CountingInputStream countingInputStream = new CountingInputStream(fis)) {
				try (GZIPInputStream gis = new GZIPInputStream(countingInputStream)) {
					try (InputStreamReader inputStreamReader = new InputStreamReader(gis)) {
						try (BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
							int fileIndex = 0;
							Queue<Future<?>> futures = new ArrayBlockingQueue<>(10000);

							while (true) {

								while (ourWorkQueue.size() > 500) {
									ourLog.debug("Work queue has {} entries - Waiting for space", ourWorkQueue.size());
									Thread.sleep(1000);
								}

								String nextLine = bufferedReader.readLine();
								if (nextLine == null) {
									ourLog.info("No more lines to read, waiting for queued uploads to finish");
									for (var next : futures) {
										next.get();
									}
									break;
								}
								if (isNotBlank(nextLine)) {
									int finalFileIndex = fileIndex;
									long bytesRead = countingInputStream.getByteCount();

									if (finalFileIndex % 10 == 0) {
										ourLog.debug("Reading resource {} ({}) - Reading {}/sec", finalFileIndex, theFilename, sw.formatThroughput(finalFileIndex, TimeUnit.SECONDS));
									}

									Bundle inputBundle = ourCtx.newJsonParser().parseResource(Bundle.class, nextLine);

									if (theFilename.equals(Step1_FileStager.META_FILES_NDJSON_GZ)) {
										try {
											List<List<Bundle.BundleEntryComponent>> entryPartitions = Lists.partition(inputBundle.getEntry(), 100);
											TreeMap<String, AtomicInteger> responseToCount = new TreeMap<>();
											for (int i = 0; i < entryPartitions.size(); i++) {
												Bundle partitionBundle = new Bundle();
												partitionBundle.setType(Bundle.BundleType.TRANSACTION);
												partitionBundle.setEntry(entryPartitions.get(i));
												ourLog.info("Uploading meta file {} partition {}/{} with {} entries: {}", finalFileIndex, i, entryPartitions.size(), partitionBundle.getEntry().size(), theFilename);
												Bundle outcome = client.transaction().withBundle(partitionBundle).execute();
												outcome
													.getEntry()
													.stream()
													.map(t -> t.getResponse().getStatus())
													.forEach(t -> responseToCount.computeIfAbsent(t, o -> new AtomicInteger(0)).incrementAndGet());

											}
											ourLog.info("Meta upload outcomes: {}", responseToCount);
											ourUploadedCount.incrementAndGet();
										} catch (BaseServerResponseException e) {
											IBaseOperationOutcome operationOutcome = e.getOperationOutcome();
											if (operationOutcome != null) {
												ourLog.error("Failure response: {}", ourCtx.newJsonParser().setPrettyPrint(true).encodeResourceToString(operationOutcome));
											}
											throw e;
										}
										continue;
									}

									Callable<Void> task = new UploadTask(client, inputBundle, bytesRead, totalBytes, sw, finalFileIndex, futures, executor);
									futures.add(executor.submit(task));

									while (futures.size() > 1000) {
										futures.poll().get();
									}

									fileIndex++;
								}
							}
						}
					}
				}
			}
		}

	}

	public static boolean isMetaFile(@NotNull String theFile) {
		return theFile.startsWith("practitionerInformation") || theFile.startsWith("hospitalInformation");
	}

}
