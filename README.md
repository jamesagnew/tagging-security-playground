
### From Synthea Directory

Run Synthea

> ./run_synthea -p 400000

Move files into staging area

find output/fhir -name "*.json" -exec mv -v {}  ../tagging-security-playground/src/main/data/new_synthea_files \;
