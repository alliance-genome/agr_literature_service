# updates are taking a while off of 4005, 52 seconds for checking 10 references

# outside docker, main run
# export XML_PATH=/home/azurebrd/git/dqm_updates/agr_literature_service/src/xml_processing/
# remove -s from process_many_pmids_to_json.py
#
# outside docker, tests run
export XML_PATH=/home/azurebrd/git/dqm_updates/agr_literature_service/src/xml_processing/tests/

# inside docker, tests run
# export XML_PATH=/workdir/src/xml_processing/tests/

pipenv run python3 get_datatypes_cross_references.py -d reference > log_get_datatypes_cross_references_01_reference
pipenv run python3 sort_dqm_json_reference_updates.py -f dqm_update_sample/ -m WB > log_sort_dqm_json_reference_updates

pipenv run python3 parse_dqm_json_reference.py -f dqm_data_updates_new/ -p > log_parse_dqm_json_reference_update_create_pmid_list
pipenv run python3 get_pubmed_xml.py -f inputs/alliance_pmids > log_get_pubmed_xml_update_create
pipenv run python3 xml_to_json.py -f inputs/alliance_pmids > log_xml_to_json_update_create
pipenv run python3 process_many_pmids_to_json.py -s -f inputs/alliance_pmids > log_process_many_pmids_to_json_update_create
pipenv run python3 parse_dqm_json_reference.py -f dqm_data_updates_new/ -m all > log_parse_dqm_json_reference_update_create_sanitize
pipenv run python3 parse_pubmed_json_reference.py -f inputs/pubmed_only_pmids > log_parse_pubmed_json_reference_update_create
pipenv run python3 post_reference_to_api.py > log_post_reference_to_api_update_create
pipenv run python3 post_comments_corrections_to_api.py -f inputs/all_pmids > log_post_comments_corrections_to_api_update_create
