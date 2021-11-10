# Take dqm data to load into ABC or process for updating ABC 

## Generate a test set of reference data for base load, generate a test set of reference data for update, populate test database, process update sample data into database.

- Make sure API_PORT is pointing to correct database and XML_PATH are pointing to your processing directory
  - export API_PORT=4005
  - export XML_PATH=<your_processing_path>
- Get dqm data into dqm_data/ 
  - pipenv run python3 get_dqm_data.py
- Optional: edit  inputs/sample_dqm_load.json  and  inputs/sample_dqm_update.json  for different data
- Generate sample data for base load into dqm_load_sample/
  - pipenv run python generate_dqm_json_test_set.py -i inputs/sample_dqm_load.json -d dqm_load_sample/ > log_generate_dqm_json_test_set_load
- Generate sample data for updates into dqm_load_sample/
  - pipenv run python generate_dqm_json_test_set.py -i inputs/sample_dqm_update.json -d dqm_update_sample/ > log_generate_dqm_json_test_set_update
- Load base data into database
  - ./sample_reference_populate_load.sh
- Process updates into database
  - ./sample_reference_populate_update.sh
- Clean up / archive logs, generated input files (todo)
- Optionally reset database from pgadmin, or drop/re-create database
  - DELETE FROM public.cross_references WHERE reference_id IS NOT NULL;
  - DELETE FROM public."authors" WHERE reference_id IS NOT NULL;
  - DELETE FROM public."references" WHERE reference_id IS NOT NULL;
- TODO write checks on load data and update data, probably inject changes also
