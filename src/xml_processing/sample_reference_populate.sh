# 22 seconds to call this shell script
pipenv run python3 parse_dqm_json_reference.py -p  -f dqm_sample/
pipenv run python3 process_many_pmids_to_json.py -f inputs/alliance_pmids
pipenv run python3 parse_dqm_json_reference.py -f dqm_sample/ -m all > log_parse_dqm_json_reference3
pipenv run python3 parse_pubmed_json_reference.py -f inputs/pubmed_only_pmids > log_parse_pubmed_json_reference3

pipenv run python3 post_reference_to_api.py > log_post_reference_to_api         # 16 seconds
