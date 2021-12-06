# 22 seconds to call this shell script

# outside docker, main run
# export XML_PATH=/home/azurebrd/git/dqm_updates/agr_literature_service/src/xml_processing/
# remove -s from process_many_pmids_to_json.py
#
# outside docker, tests run
export XML_PATH=/home/azurebrd/git/dqm_updates/agr_literature_service/src/xml_processing/tests/

# inside docker, tests run
# export XML_PATH=/workdir/src/xml_processing/tests/

pipenv run python3 parse_dqm_json_reference.py -p -d ./ -f dqm_load_sample/ > log_parse_dqm_json_reference_load_pmid_list
pipenv run python3 process_many_pmids_to_json.py -s -f inputs/alliance_pmids > log_process_many_pmids_to_json_load
pipenv run python3 parse_dqm_json_reference.py -d ./ -f dqm_load_sample/ -m all > log_parse_dqm_json_reference_load_sanitize
pipenv run python3 parse_pubmed_json_reference.py -f inputs/pubmed_only_pmids > log_parse_pubmed_json_reference_load

pipenv run python3 post_reference_to_api.py > log_post_reference_to_api_load         # 16 seconds
pipenv run python3 post_comments_corrections_to_api.py -f inputs/all_pmids > log_post_comments_corrections_to_api_load
