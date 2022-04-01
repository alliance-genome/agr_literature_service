"""
# query database for xrefs, extra MODs, post to populate mod_corpus_association
# python process_mod_corpus_association_to_api.py

# TODO: API to create mod_corpus_association is not live, when it is make sure new_entry is defined properly
"""

from curses import has_key
import json
import logging
import logging.config
from os import environ, path
import requests
import sys
from tqdm import tqdm
from literature.database.main import get_db
from literature.models import ModCorpusAssociationModel, ReferenceModel, ModModel
import time

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
    db_session = next(get_db())
    all_references_ids = db_session.query(ReferenceModel.curie, ReferenceModel.reference_id).all()
    ref_curie_id_dict = {curie_id[0]: curie_id[1] for curie_id in all_references_ids}
    all_mods = db_session.query(ModModel).all()
    mod_abbreviation_id_dict = {mod.abbreviation: mod.mod_id for mod in all_mods}
    start = time.time()
    for agr in ref_xref_valid:
        for prefix in ref_xref_valid[agr]:
            if prefix in mods:
                #post_mod_corpus_association(agr, prefix, headers, base_url)
                if agr in ref_curie_id_dict:
                    mod_corpus_association = ModCorpusAssociationModel(reference_id=ref_curie_id_dict[agr],
                                                                   mod_id=mod_abbreviation_id_dict[prefix],
                                                                   corpus=True, mod_corpus_sort_source="dqm_files")
                db_session.add(mod_corpus_association)
                db_session.commit()
    end = time.time()
    logger.info("finished in " + str(end - start) + " seconds")


def post_mod_corpus_association(agr, prefix, headers, base_url):
    #logger.info("%s %s %s %s", agr, prefix, base_url, headers)
    
    #here to first check if record exist with same reference_curie/mod_abbreviation, if yes, skip
    query_entry = {'reference_curie': agr, 'mod_abbreviation': prefix}

    new_entry = {'reference_curie': agr, 'mod_abbreviation': prefix, 'corpus': 'true', 'mod_corpus_sort_source': 'dqm_files'}
    #reference/mod_corpus_association/reference/AGR%3AAGR-Reference-0000000003/mod_abbreviation/FB
    query_url = base_url + "reference/" + agr + "/mod_abbreviation/" + prefix
    get_return = requests.get(query_url)
    get_return.status_code
    if (get_return.status_code ==200 ):
        logger.info("mod_corpus_association_id is: %s for reference_curie: %s and mod_abbreviation:%s", get_return.text, agr, prefix)
        return get_return.text
 
    api_response_tuple = process_api_request('POST', base_url, headers, new_entry, agr, None, None)
    headers = api_response_tuple[0]
    response_text = api_response_tuple[1]
    response_status_code = api_response_tuple[2]
    log_info = api_response_tuple[3]
    response_dict = json.loads(response_text)

    if log_info:
        logger.info(log_info)

    if response_status_code == 201:
        logger.info(f"{agr}\t{response_dict}")
    else:
        logger.info("api error: %s primaryId: %s message: %s", str(response_status_code), agr, response_dict['detail'])
    return headers


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("Starting process_mod_corpus_association_to_api.py")
    do_everything()
    logger.info("Ending process_mod_corpus_association_to_api.py")

# pipenv run python process_mod_corpus_association_to_api.py
