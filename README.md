# Running This Test

This page describes the instructions for executing this test. Note that this test assumes a Smile CDR instance with the following settings:

* Audit log and transaction log have been disabled
* FHIR Storage module is using MongoDB

Note: These instructions assume that you have checked out `synthea` and `tagging-security-playground` into the same parent directory.

# Generate Synthea Files to Upload

* Navigate to the synthea directory

```
cd synthea
```

* Run Synthea

```
./run_synthea -p 1000000
```

* Move files into staging area

```
find output/fhir -name "*.json" -exec mv -v {}  ../tagging-security-playground/src/main/data/new_synthea_files \;
```

# Stage the Synthea Files

In this step we pre-process the Synthea files to reduce their size and filter their contents to only Patient, Encounter, and Observation resources. We also assign random tags.

* Navigate to the playground directory

```
cd ../tagging-security-playground
```

* Run the stager application

```
mvn clean compile exec:java -Dexec.mainClass=Step1_FileStager
```

* Monitor Progress - Run this from the parent directory

```
echo -n "New: "; find src/main/data/new_synthea_files | wc -l; echo -n "Staged: "; find src/main/data/staged_synthea_files | wc -l; du -h src/main/data/new_synthea_files src/main/data/staged_synthea_files 2>/dev/null
```

# Upload the Synthea Files

```
mvn clean compile exec:java -Dexec.mainClass=Step2_DataUploader
```

# Run the tests

```
mvn clean compile exec:java -Dexec.mainClass=Step3_Queries
```

