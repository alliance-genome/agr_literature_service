# 22 seconds to call this shell script
pipenv run python3 parse_dqm_json_reference.py -p  -f dqm_load_sample/ > log_parse_dqm_json_reference_load_pmid_list
pipenv run python3 process_many_pmids_to_json.py -f inputs/alliance_pmids > log_process_many_pmids_to_json_load
pipenv run python3 parse_dqm_json_reference.py -f dqm_load_sample/ -m all > log_parse_dqm_json_reference_load_sanitize
pipenv run python3 parse_pubmed_json_reference.py -f inputs/pubmed_only_pmids > log_parse_pubmed_json_reference_load

pipenv run python3 post_reference_to_api.py > log_post_reference_to_api_load         # 16 seconds
