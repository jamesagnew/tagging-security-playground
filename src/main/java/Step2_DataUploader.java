import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor;
import org.apache.commons.io.FileUtils;
import org.hl7.fhir.r4.model.Bundle;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class Step2_DataUploader {
    private static final Logger ourLog = LoggerFactory.getLogger(Step2_DataUploader.class);

    private static final FhirContext ourCtx = FhirContext.forR4Cached();

    public static void main(String[] args) throws Exception {
        ExecutorService executor = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(500));

        Collection<File> inputFiles =
                FileUtils
                        .listFiles(new File("src/main/data/staged_synthea_files"), new String[]{"gz"}, false)
                        .stream()
                        .sorted(new SyntheaMetaFilesFirstComparator())
                        .collect(Collectors.toList());

        int fileIndex = 0;
        List<Future<?>> futures = new ArrayList<>();

        ourCtx.getRestfulClientFactory().setSocketTimeout(10000000);
        IGenericClient client = ourCtx.newRestfulGenericClient("http://localhost:8000");
        client.registerInterceptor(new BasicAuthInterceptor("admin:password"));
        client.registerInterceptor(new LoggingInterceptor(false));

        for (var next : inputFiles) {
            int finalFileIndex = fileIndex;

            ourLog.info("Processing file {}: {}", finalFileIndex, next.getName());
            Bundle inputBundle;
            try (FileInputStream fis = new FileInputStream(next)) {
                try (GZIPInputStream gis = new GZIPInputStream(fis)) {
                    try (InputStreamReader reader = new InputStreamReader(gis)) {
                        inputBundle = ourCtx.newJsonParser().parseResource(Bundle.class, reader);
                    }
                }
            }

            if (isMetaFile(next)) {
                client.transaction().withBundle(inputBundle).execute();
                continue;
            }

            Callable<Void> task = () -> {
                client.transaction().withBundle(inputBundle).execute();
                return null;
            };

            futures.add(executor.submit(task));

            fileIndex++;

        }

        for (var next : futures) {
            next.get();
        }

        executor.shutdown();
    }

    private static class SyntheaMetaFilesFirstComparator implements Comparator<File> {
        @Override
        public int compare(@NotNull File theFile1, @NotNull File theFile2) {
            boolean f1comesFirst = isMetaFile(theFile1);
            boolean f2comesFirst = isMetaFile(theFile2);
            if (f1comesFirst == f2comesFirst) {
                return 0;
            }
            return f1comesFirst ? -1 : 1;
        }
    }

    private static boolean isMetaFile(@NotNull File theFile) {
        return theFile.getName().startsWith("practitionerInformation") || theFile.getName().startsWith("hospitalInformation");
    }
}