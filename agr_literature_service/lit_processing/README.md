# Take dqm data to load into ABC or process for updating ABC

- To do a live run on MOD data, skip the generation of sample data.  Run ./sample_reference_populate_update.sh using dqm_data/ instead of dqm_update_sample/ and setting appropriate MOD.

## Generate a test set of reference data for base load, generate a test set of reference data for update, populate test database, process update sample data into database.

### Optionally generate a new set of test data if there are model changes to database or dqm data.  Save data in tests/ and commit changes to github so future tests can be done on fixed data that works.

- Make sure API_PORT is pointing to correct database and XML_PATH are pointing to your processing directory
  - export API_PORT=4005
  - export XML_PATH=<your_processing_path>/tests/
  - cd <your_processing_path>
- Get dqm data into dqm_data/
  - pipenv run python3 get_dqm_data.py
- Optional: edit  tests/inputs/sample_dqm_load.json  and  tests/inputs/sample_dqm_update.json  for different data
- Generate sample data for base load into dqm_load_sample/
  - pipenv run python generate_dqm_json_test_set.py -i inputs/sample_dqm_load.json -d dqm_load_sample/ > log_generate_dqm_json_test_set_load
- Generate sample data for updates into dqm_load_sample/
  - pipenv run python generate_dqm_json_test_set.py -i inputs/sample_dqm_update.json -d dqm_update_sample/ > log_generate_dqm_json_test_set_update
- Generate base input list of pmids in tests/inputs/alliance_pmids and pmids_by_mods in tests/pmids_by_mods
  - pipenv run python3 parse_dqm_json_reference.py -p -d ./ -f dqm_load_sample/ > log_parse_dqm_json_reference_load_pmid_list
- Generate new pubmed_xml/ (to commit)
  - pipenv run python3 get_pubmed_xml.py -f inputs/alliance_pmids > log_get_pubmed_xml
- Generate pubmed_json/ (not to commit)
  - pipenv run python3 xml_to_json.py -f inputs/alliance_pmids > log_xml_to_json
- Generate comments-corrections pubmed_xml/ (to commit) and pubmed_json/ (not to commit)
  - pipenv run python3 process_many_pmids_to_json.py -f inputs/alliance_pmids > log_process_many_pmids_to_json_load

### Connect to docker and load database with base load, run tests, update database with changes, run tests.

- Connect to docker
  - docker run --rm --network=agr_literature_service_agr-literature -p 5432:5432 -v ${PWD}:/workdir -t -i 100225593120.dkr.ecr.us-east-1.amazonaws.com/agr_literature_dev:latest /bin/bash
  - cd /workdir/src/xml_processing/tests/
  - export API_PORT=4005
  - export XML_PATH=<your_processing_path>/tests/
- TODO Stuff after here hasn't been tested in docker because we're not sure that's messing with the live database.  2021 11 15
- Load base data into database (generate inputs/ lists, pubmed_json/ , post to api/db)
  - ./sample_reference_populate_load.sh
- Process updates into database (generate xref file, do updates, run pipeline on new references)
  - Note the populate new references pipeline uses the same inputs/* and pmids_by_mods, so they update.  Do not push them into github after run.
  - ./sample_reference_populate_update.sh
- Clean up / archive logs, generated input files (todo)
- Optionally reset database from pgadmin, or drop/re-create database
  - DELETE FROM public.reference_comments_and_corrections;
  - DELETE FROM public.cross_references WHERE reference_id IS NOT NULL;
  - DELETE FROM public."authors" WHERE reference_id IS NOT NULL;
  - DELETE FROM public."references" WHERE reference_id IS NOT NULL;
- TODO make update changes, write checks on update data
