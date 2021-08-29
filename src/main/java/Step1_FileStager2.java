import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.util.StopWatch;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

@SuppressWarnings("BusyWait")
public class Step1_FileStager2 {
    public static final File NEW_SYNTHEA_FILES = new File("src/main/data/new_synthea_files");
    public static final File STAGED_SYNTHEA_FILES = new File("src/main/data/staged_synthea_files");

    private static final Logger ourLog = LoggerFactory.getLogger(Step1_FileStager2.class);
    private static final FhirContext ourCtx = FhirContext.forR4Cached();
    private static final BlockingQueue<FileAndName> ourInputFilesQueue = new ArrayBlockingQueue<>(1000);
    private static final BlockingQueue<FileAndName> ourOutputFilesQueue = new ArrayBlockingQueue<>(1000);
    private static final Map<String, AtomicInteger> resourceTypeToCount = Collections.synchronizedMap(new HashMap<>());
    private static final AtomicBoolean ourFinishedReading = new AtomicBoolean(false);
    private static final AtomicBoolean ourFinishedWriting = new AtomicBoolean(false);
    private static final AtomicInteger ourTotalFileCount = new AtomicInteger(0);
    private static final AtomicInteger ourTotalProcessedFileCount = new AtomicInteger(0);
    private static final AtomicInteger ourTotalWrittenFileCount = new AtomicInteger(0);
    private static Exception ourException;

    public static void main(String[] args) throws Exception {
        new ReaderThread().start();

        for (int i = 0; i < 10; i++) {
            new ProcessorThread().start();
        }

        new WriterThread().start();

        while (ourException == null && !ourFinishedWriting.get()) {
            Thread.sleep(1000);
        }

        resourceTypeToCount.keySet().stream().sorted().forEach(t -> ourLog.info("Count {} -> {}", t, resourceTypeToCount.get(t).get()));
    }

    private static class FileAndName {
        private final String myFilename;
        private final String myContents;

        private FileAndName(String myFilename, String myContents) {
            this.myFilename = myFilename;
            this.myContents = myContents;
        }

        public String getFilename() {
            return myFilename;
        }

        public String getContents() {
            return myContents;
        }
    }

    private static class WriterThread extends Thread {
        @Override
        public void run() {
            setName("writer");

            StopWatch sw = new StopWatch();
            int count = 0;
            while (true) {
                count++;
                FileAndName nextFile = null;
                try {
                    nextFile = ourOutputFilesQueue.poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    // ignore
                }

                if (nextFile == null) {
                    if (ourFinishedReading.get() && ourTotalFileCount.get() == ourTotalWrittenFileCount.get()) {
                        ourLog.info("Finished writing - Have written {} files", ourTotalWrittenFileCount.get());
                        return;
                    }
                    if (ourException != null) {
                        return;
                    }

                    continue;
                }

                if (count % 10 == 0) {
                    int total = ourTotalFileCount.get();
                    int writeQueue = ourOutputFilesQueue.size();
                    ourLog.info("Processing file {}/{}: {}/sec ETA {} - ProcessQueue[{}] WriteQueue[{}]", count, total, sw.formatThroughput(count, TimeUnit.SECONDS), sw.getEstimatedTimeRemaining(count, total), ourInputFilesQueue.size(), writeQueue);
                }

                File targetFile = new File(STAGED_SYNTHEA_FILES, nextFile.getFilename() + ".gz");
                try (FileOutputStream fos = new FileOutputStream(targetFile, false)) {
                    try (BufferedOutputStream bos = new BufferedOutputStream(fos)) {
                        try (GZIPOutputStream gos = new GZIPOutputStream(bos)) {
                            try (OutputStreamWriter w = new OutputStreamWriter(gos, StandardCharsets.UTF_8)) {
                                w.write(nextFile.getContents());
                            }
                        }
                    }
                } catch (Exception e) {
                    ourLog.error("Failure during write", e);
                    ourException = e;
                    return;
                }

                try {
                    File sourceFile = new File(NEW_SYNTHEA_FILES, nextFile.getFilename());
                    Validate.isTrue(sourceFile.exists());
                    sourceFile.delete();
                } catch (Exception e) {
                    ourLog.error("Failure during write", e);
                    ourException = e;
                    return;
                }


            }
        }
    }

    private static class ReaderThread extends Thread {


        @Override
        public void run() {
            setName("reader");

            ourLog.info("Listing files in {}", NEW_SYNTHEA_FILES);
            Collection<File> inputFiles = FileUtils.listFiles(NEW_SYNTHEA_FILES, new String[]{"json"}, false);
            ourTotalFileCount.set(inputFiles.size());
            int count = 0;
            for (var nextFile : inputFiles) {

                try (FileReader reader = new FileReader(nextFile)) {
                    String contents = IOUtils.toString(reader);
                    ourInputFilesQueue.add(new FileAndName(nextFile.getName(), contents));
                } catch (Exception e) {
                    ourLog.error("Failed during read", e);
                    ourException = e;
                    return;
                }


                if (count % 10 == 0) {
                    ourLog.info("Have read {} files", count);
                }
                count++;

            }

            ourLog.info("Reading is complete - Have read {} files", ourTotalFileCount.get());
            ourFinishedReading.set(true);
        }
    }

    private static class ProcessorThread extends Thread {
        private static int ourThreadCount = 0;

        @Override
        public void run() {
            setName("worker-" + ourThreadCount++);

            int count = 0;
            while (true) {
                FileAndName nextFile = null;
                try {
                    nextFile = ourInputFilesQueue.poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    // ignore
                }

                if (nextFile == null) {
                    if (ourFinishedReading.get() && ourTotalFileCount.get() == ourTotalProcessedFileCount.get()) {
                        ourLog.info("Finished processing - Have processed {} files", ourTotalProcessedFileCount.get());
                        return;
                    }
                    if (ourException != null) {
                        return;
                    }
                    continue;
                }

                Bundle bundle = ourCtx.newJsonParser().parseResource(Bundle.class, nextFile.getContents());

                List<Resource> resources = new ArrayList<>();
                for (Iterator<Bundle.BundleEntryComponent> iter = bundle.getEntry().iterator(); iter.hasNext(); ) {
                    Bundle.BundleEntryComponent bundleEntryComponent = iter.next();
                    Resource resource = bundleEntryComponent.getResource();
                    if (resource != null) {
                        if (nextFile.getFilename().startsWith("practitionerInformation") || nextFile.getFilename().startsWith("hospitalInformation")) {
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
                    AtomicInteger typeCount = resourceTypeToCount.computeIfAbsent(ourCtx.getResourceType(nextResource), t -> new AtomicInteger());
                    typeCount.incrementAndGet();
                }

                String newBundle = ourCtx.newJsonParser().encodeResourceToString(bundle);
                ourOutputFilesQueue.add(new FileAndName(nextFile.getFilename(), newBundle));
                ourTotalProcessedFileCount.incrementAndGet();

                count++;
                if (count % 10 == 0) {
                    ourLog.info("Have processed {} files", count);
                }
            }
        }
    }
}