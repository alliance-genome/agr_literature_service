# 22 seconds to call this shell script

echo $PWD
echo "Running pqjr" > /logs/srpl.log
python3 ./src/xml_processing/parse_dqm_json_reference.py -p -d ./ -f dqm_load_sample/ > /logs/log_parse_dqm_json_reference_load_pmid_list
echo "Running pmptj"
python3 ./src/xml_processing/process_many_pmids_to_json.py -s -f inputs/alliance_pmids > /logs/log_process_many_pmids_to_json_load
echo "Running pdjr"
python3 ./src/xml_processing/parse_dqm_json_reference.py -d ./ -f dqm_load_sample/ -m all > /logs/log_parse_dqm_json_reference_load_sanitize
echo "Running ppjr"
python3 ./src/xml_processing/parse_pubmed_json_reference.py -f inputs/pubmed_only_pmids > /logs/log_parse_pubmed_json_reference_load
echo "Running prta"
python3 ./src/xml_processing/post_reference_to_api.py > /logs/log_post_reference_to_api_load         # 16 seconds
echo "Running pccta"
python3 ./src/xml_processing/post_comments_corrections_to_api.py -f inputs/all_pmids > /logs/log_post_comments_corrections_to_api_load
