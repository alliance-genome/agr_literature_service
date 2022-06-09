# updates are taking a while off of 4005, 52 seconds for checking 10 references

python3 ../agr_literature_service/lit_processing/sort_dqm_json_reference_updates.py -f dqm_update_sample/ -m WB > /logs/log_sort_dqm_json_reference_updates

# old update required a lot of sequential scripts, now all in sort_dqm_json_reference_updates.py
# python3 ./src/xml_processing/get_datatypes_cross_references.py -d reference > /logs/log_get_datatypes_cross_references_01_reference
# python3 ./src/xml_processing/sort_dqm_json_reference_updates.py -f dqm_update_sample/ -m WB > /logs/log_sort_dqm_json_reference_updates
#
# # Note the populate new references pipeline uses the same inputs/* and pmids_by_mods, so they update.  Do not push them into github after run.
# python3 ./src/xml_processing/parse_dqm_json_reference.py -f dqm_data_updates_new/ -p > /logs/log_parse_dqm_json_reference_update_create_pmid_list
# python3 ./src/xml_processing/get_pubmed_xml.py -f inputs/alliance_pmids > /logs/log_get_pubmed_xml_update_create
# python3 ./src/xml_processing/xml_to_json.py -f inputs/alliance_pmids > /logs/log_xml_to_json_update_create
# python3 ./src/xml_processing/process_many_pmids_to_json.py -s -f inputs/alliance_pmids > /logs/log_process_many_pmids_to_json_update_create
# python3 ./src/xml_processing/parse_dqm_json_reference.py -f dqm_data_updates_new/ -m all > /logs/log_parse_dqm_json_reference_update_create_sanitize
# python3 ./src/xml_processing/parse_pubmed_json_reference.py -f inputs/pubmed_only_pmids > /logs/log_parse_pubmed_json_reference_update_create
# python3 ./src/xml_processing/post_reference_to_api.py > /logs/log_post_reference_to_api_update_create
# python3 ./src/xml_processing/post_comments_corrections_to_api.py -f inputs/all_pmids > /logs/log_post_comments_corrections_to_api_update_create
