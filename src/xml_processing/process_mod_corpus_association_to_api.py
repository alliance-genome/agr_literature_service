"""
# query database for xrefs, extra MODs, post to populate mod_corpus_association
# python process_mod_corpus_association_to_api.py

# TODO: API to create mod_corpus_association is not live, when it is make sure new_entry is defined properly
"""

import json
import logging
import logging.config
from os import environ, path
import sys

from helper_file_processing import (generate_cross_references_file, load_ref_xref)
from helper_post_to_api import (generate_headers, get_authentication_token,
                                process_api_request)


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def do_everything():
    token = get_authentication_token()
    headers = generate_headers(token)
    api_server = environ.get('API_SERVER', 'localhost')
    api_port = environ.get('API_PORT', '8080')
    base_url = 'http://' + api_server + ':' + api_port + '/reference/mod_corpus_association/'

    #generate_cross_references_file('reference')   # this updates from references in the database, and takes 88 seconds. if updating this script, comment it out after running it once
    xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('reference')

    mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB', 'XB', 'GO']
    count=1;
    for agr in ref_xref_valid:
        for prefix in ref_xref_valid[agr]:
            if prefix in mods:
                count +=1
                post_mod_corpus_association(agr, prefix, headers, base_url)
                if count >50000000:
                    sys.exit()


def post_mod_corpus_association(agr, prefix, headers, base_url):
    #logger.info("%s %s %s %s", agr, prefix, base_url, headers)
    
    #here to first check if record exist with same reference_curie/mod_abbreviation, if yes, skip
    query_entry = {'reference_curie': agr, 'mod_abbreviation': prefix}


    new_entry = {'reference_curie': agr, 'mod_abbreviation': prefix, 'corpus': 'true', 'mod_corpus_sort_source': 'dqm_files'}
    #logger.info("headers here:")
    #logger.info(headers)
    api_response_tuple_query = process_api_request('PUT', base_url, headers, query_entry, agr, None, None)
    if api_response_tuple_query:
        #logger.info("response_status_code: %s", api_response_tuple_query[2])
        #logger.info("response_text %s", api_response_tuple_query[1])
        if (api_response_tuple_query[2] == 200):
            return api_response_tuple_query[2]
 
    #logger.info("start to post with new_entry:")
    #logger.info(new_entry)
    api_response_tuple = process_api_request('POST', base_url, headers, new_entry, agr, None, None)
    headers = api_response_tuple[0]
    response_text = api_response_tuple[1]
    response_status_code = api_response_tuple[2]
    log_info = api_response_tuple[3]
    #logger.info("after post new_entry, response_text:%s, response_statux_cod:%s", response_text, response_status_code)
    response_dict = json.loads(response_text)

    if log_info:
        logger.info(log_info)

    if response_status_code == 201:
        logger.info(f"{agr}\t{response_dict}")
    else:
        logger.info("api error: %s primaryId: %s message: %s", str(response_status_code), agr, response_dict['detail'])
    #sys.exit()
    return headers


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("Starting process_mod_corpus_association_to_api.py")
    do_everything()
    logger.info("Ending process_mod_corpus_association_to_api.py")

# pipenv run python process_mod_corpus_association_to_api.py
