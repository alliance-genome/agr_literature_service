
import json
import logging
import logging.config
from os import environ, path

from helper_file_processing import (generate_cross_references_file, load_ref_xref)
from helper_post_to_api import (generate_headers, get_authentication_token,
                                process_api_request)

# query database for xrefs, extra MODs, post to populate mod_corpus_association
# python process_mod_corpus_association_to_api.py

# TODO: API to create mod_corpus_association is not live, when it is make sure new_entry is defined properly


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def do_everything():
    token = get_authentication_token()
    headers = generate_headers(token)
    api_server = environ.get('API_SERVER', 'localhost')
    api_port = environ.get('API_PORT', '4001')
    base_url = 'http://' + api_server + ':' + api_port + '/reference/mod_corpus_association/'

    generate_cross_references_file('reference')   # this updates from references in the database, and takes 88 seconds. if updating this script, comment it out after running it once
    xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('reference')

    mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB']
    for agr in ref_xref_valid:
        for prefix in ref_xref_valid[agr]:
            if prefix in mods:
                headers = post_mod_corpus_association(agr, prefix, headers, base_url)


def post_mod_corpus_association(agr, prefix, headers, base_url):
    logger.info("%s %s", agr, prefix)

    new_entry = dict()
    new_entry['agr_curie'] = agr
    new_entry['mod'] = prefix
    new_entry['corpus'] = 'inside_corpus'
    new_entry['source'] = 'dqm_files'

    url = base_url + agr
    api_response_tuple = process_api_request('POST', url, headers, new_entry, agr, None, None)
    headers = api_response_tuple[0]
    response_text = api_response_tuple[1]
    response_status_code = api_response_tuple[2]
    log_info = api_response_tuple[3]
    response_dict = json.loads(response_text)

    if log_info:
        logger.info(log_info)

    if (response_status_code == 201):
        response_dict = response_dict.replace('"', '')
        logger.info("%s\t%s", agr, response_dict)
        # mapping_fh.write("%s\t%s\n" % (agr, response_dict))
    else:
        logger.info("api error %s primaryId %s message %s", str(response_status_code), agr, response_dict['detail'])
        # error_fh.write("api error %s primaryId %s message %s\n" % (str(response_status_code), agr, response_dict['detail']))

    return headers


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("Starting process_mod_corpus_association_to_api.py")

    do_everything()

    logger.info("ending process_mod_corpus_association_to_api.py")

# pipenv run python process_mod_corpus_association_to_api.py
