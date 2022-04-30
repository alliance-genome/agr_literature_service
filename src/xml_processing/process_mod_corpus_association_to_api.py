"""
# query database for xrefs, extra MODs, post to populate mod_corpus_association
# python process_mod_corpus_association_to_api.py

"""

import logging.config
from os import path
# from os import environ
from literature.database.main import get_db
from literature.models import ModCorpusAssociationModel, ReferenceModel, ModModel
import time

from helper_sqlalchemy import sqlalchemy_load_ref_xref
# from helper_file_processing import load_ref_xref_api_flatfile
# from helper_file_processing import generate_cross_references_file
# from helper_post_to_api import (generate_headers, get_authentication_token)


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def do_everything():
    # token = get_authentication_token()
    # headers = generate_headers(token)
    # api_server = environ.get('API_SERVER', 'localhost')
    # api_port = environ.get('API_PORT', '8080')
    # base_url = 'http://' + api_server + ':' + api_port + '/reference/mod_corpus_association/'

    # generate_cross_references_file('reference')   # this updates from references in the database, and takes 88 seconds. if updating this script, comment it out after running it once
    xref_ref, ref_xref_valid, ref_xref_obsolete = sqlalchemy_load_ref_xref('reference')

    db_session = next(get_db())
    all_references_ids = db_session.query(ReferenceModel.curie, ReferenceModel.reference_id).all()
    ref_curie_id_dict = {curie_id[0]: curie_id[1] for curie_id in all_references_ids}
    all_mods = db_session.query(ModModel).all()
    mod_abbreviation_id_dict = {mod.abbreviation: mod.mod_id for mod in all_mods}
    all_mod_abbr = set([mod.abbreviation for mod in all_mods])
    start = time.time()
    for agr in ref_xref_valid:
        for prefix in ref_xref_valid[agr]:
            if prefix in all_mod_abbr:
                if agr in ref_curie_id_dict:
                    mod_corpus_association = ModCorpusAssociationModel(reference_id=ref_curie_id_dict[agr],
                                                                       mod_id=mod_abbreviation_id_dict[prefix],
                                                                       corpus=True, mod_corpus_sort_source="dqm_files")
                    db_session.add(mod_corpus_association)
                    db_session.commit()
    end = time.time()
    logger.info("finished in " + str(end - start) + " seconds")


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("Starting process_mod_corpus_association_to_api.py")
    do_everything()
    logger.info("Ending process_mod_corpus_association_to_api.py")

# pipenv run python process_mod_corpus_association_to_api.py
