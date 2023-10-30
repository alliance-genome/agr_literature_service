import argparse
import logging.config
from os import environ, path

from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel, ModModel
# from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel, ModModel, ModCorpusAssociationModel
from agr_literature_service.api.schemas import ModCorpusAssociationSchemaPost
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.sanitize_pubmed_json import sanitize_pubmed_json_list
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json
from agr_literature_service.lit_processing.utils.s3_utils import upload_xml_file_to_s3
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.api.crud.mod_corpus_association_crud import create

# pipenv run python process_single_pmid.py -c 12345678
# enter a single pmid as an argument, download xml, convert to json, sanitize, post to api

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

init_tmp_dir()


def process_pmid(pmid: str, mod_curie: str, mod_mca: str):
    """

    :param pmid:
    :param mod_curie:
    :param mod_mca:
    :return:
    """
    db_session = create_postgres_session(False)
    exists = db_session.query(CrossReferenceModel).filter_by(curie="PMID:" + pmid).one_or_none()
    db_session.close()
    added_curie = None
    if not exists:
        base_path = environ.get('XML_PATH', '')
        pmids_wanted = [pmid]
        download_pubmed_xml(pmids_wanted)
        generate_json(pmids_wanted, [])
        sanitize_pubmed_json_list(pmids_wanted, [])
        # json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_' + pmid + '.json'
        json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_PMID.json'
        added_curie = post_references(json_filepath)[0]
        upload_xml_file_to_s3(pmid)
        reference_object = db_session.query(ReferenceModel).filter_by(curie=added_curie).one_or_none()
        if reference_object:
            reference_id = reference_object.reference_id
            if mod_curie != '':
                try:
                    cross_ref = CrossReferenceModel(curie=mod_curie,
                                                    curie_prefix=mod_curie.split(":")[0],
                                                    reference_id=reference_id,
                                                    pages=['reference'])
                    db_session.add(cross_ref)
                    db_session.commit()
                    logger.info(str(reference_id) + ": INSERT CROSS_REFERENCE: " + mod_curie)
                except Exception as e:
                    logger.info(str(reference_id) + ": INSERT CROSS_REFERENCE: " + mod_curie + " failed: " + str(e))
            mod_object = db_session.query(ModModel).filter_by(abbreviation=mod_mca).one_or_none()
            if mod_object and mod_mca != '':
                mod_id = mod_object.mod_id
                try:
                    new_mca_dict = {
                        "mod_abbreviation": mod_object.abbreviation,
                        "mod_corpus_sort_source": "manual_creation",
                        "corpus": True,
                        "reference_curie": reference_object.curie
                    }
                    new_mca = ModCorpusAssociationSchemaPost(**new_mca_dict)
                    create(db_session, new_mca)
                    # mca = ModCorpusAssociationModel(reference_id=reference_id,
                    #                                 mod_id=mod_id,
                    #                                 mod_corpus_sort_source='manual_creation',
                    #                                 corpus=True)
                    # db_session.add(mca)
                    # db_session.commit()
                    # logger.info("INSERT MOD_CORPUS_ASSOCIATION: for reference_id = " + str(
                    #     reference_id) + ", mod_id = " + str(mod_id) + ", mod_corpus_sort_source = manual_creation")
                except Exception as e:
                    logger.info("INSERT MOD_CORPUS_ASSOCIATION: for reference_id = " + str(
                        reference_id) + ", mod_id = " + str(mod_id) + ", mod_corpus_sort_source = manual_creation " + str(e))
    return added_curie


if __name__ == "__main__":
    """
    call main start function
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')

    args = vars(parser.parse_args())

    pmids_wanted = []

#    python process_single_pmid.py -c 1234 4576 1828
    if args['commandline']:
        logger.info("Processing commandline input")
        for pmid in args['commandline']:
            pmids_wanted.append(pmid)

    else:
        logger.info("Must enter a PMID through command line")

    if len(pmids_wanted) > 0:
        db_session = create_postgres_session(False)
        scriptNm = path.basename(__file__).replace(".py", "")
        set_global_user_id(db_session, scriptNm)
        db_session.close()

    for pmid in pmids_wanted:
        process_pmid(pmid, '', '')
