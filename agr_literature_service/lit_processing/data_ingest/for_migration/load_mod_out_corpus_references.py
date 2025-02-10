import logging
from os import environ, path
from sqlalchemy import text
import time

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ModCorpusAssociationModel
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.sanitize_pubmed_json import \
    sanitize_pubmed_json_list
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

datafile = "data/SGD_references_deleted.txt"
MOD = "SGD"
MCA_SRC = "mod_pubmed_search"
CORPUS = False
BATCH_COMMIT_SIZE = 250
DOWNLOAD_XML_MAX_SIZE = 5000
SLEEP_TIME = 10


def load_data():

    db = create_postgres_session(False)
    pmid_to_data = get_pmids(db)
    mod_id = get_mod_id(db)
    logger.info(f"mod_id={mod_id}")
    mod_reference_id_to_corpus = get_mod_references(db, mod_id)
    db.close()

    to_update_mca = set()
    to_add_to_mca = set()
    to_add_to_db = set()
    with open(datafile) as f:
        for line in f:
            pmid = line.strip()
            if pmid in pmid_to_data:
                (reference_id, is_obsolete) = pmid_to_data[pmid]
                if reference_id in mod_reference_id_to_corpus:
                    if mod_reference_id_to_corpus[reference_id] != CORPUS:
                        to_update_mca.add((pmid, reference_id))
                else:
                    to_add_to_mca.add((pmid, reference_id))
            else:
                to_add_to_db.add(pmid.replace('PMID:', ''))

    db = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, scriptNm)

    logger.info(f"Adding {len(to_add_to_mca)} rows into the mod_corpus_association table...")
    add_papers_to_mca_table(db, mod_id, to_add_to_mca)

    logger.info(f"Updating corpus for {len(to_update_mca)} rows in the mod_corpus_association table...")
    update_corpus_in_mca_table(db, mod_id, to_update_mca)

    logger.info(f"Adding the {len(to_add_to_db)} new papers into the database...")
    add_papers_to_database(db, list(to_add_to_db))

    db.close()


def add_papers_to_database(db, pmids_all):

    pmids_all.sort()

    logger.info("Downloading XML files...")
    if len(pmids_all) > DOWNLOAD_XML_MAX_SIZE:
        for index in range(0, len(pmids_all), DOWNLOAD_XML_MAX_SIZE):
            logger.info(f"Download xml from {index} to {index+DOWNLOAD_XML_MAX_SIZE}")
            pmids_slice = pmids_all[index:index + DOWNLOAD_XML_MAX_SIZE]
            download_pubmed_xml(pmids_slice)
            time.sleep(SLEEP_TIME)
    else:
        download_pubmed_xml(pmids_all)

    logger.info("Generating json files...")
    generate_json(pmids_all, [])

    mca_object = {
        'modCorpusAssociations': [
            {
                "modAbbreviation": MOD,
                "modCorpusSortSource": MCA_SRC,
                "corpus": CORPUS
            }
        ]
    }

    bad_date_published = sanitize_pubmed_json_list(pmids_all, [mca_object])
    logger.info(f"bad_date_published={bad_date_published}")

    base_path = environ.get('XML_PATH', "")
    json_file_with_path = f"{base_path}sanitized_reference_json/REFERENCE_PUBMED_PMID.json"
    post_references(json_file_with_path)


def add_papers_to_mca_table(db, mod_id, to_add_to_mca):

    count = 0
    for (pmid, reference_id) in to_add_to_mca:
        # mod_corpus_sort_source = "Mod_pubmed_search"

        mca = ModCorpusAssociationModel(reference_id=reference_id,
                                        mod_id=mod_id,
                                        mod_corpus_sort_source=MCA_SRC,
                                        corpus=CORPUS)
        db.add(mca)
        logger.info(f"{pmid}: adding MCA row for {MOD} reference with corpus = {CORPUS}.")
        count += 1
        if count % BATCH_COMMIT_SIZE == 0:
            db.commit()
    db.commit()


def update_corpus_in_mca_table(db, mod_id, to_update_mca):

    count = 0
    for (pmid, reference_id) in to_update_mca:
        db.execute(text(f"UPDATE mod_corpus_association "
                        f"SET corpus = {CORPUS} "
                        f"WHERE mod_id = {mod_id} "
                        f"AND reference_id = {reference_id}"))
        logger.info(f"{pmid}: SET corpus = {CORPUS} in mod_corpus_association table.")
        count += 1
        if count % BATCH_COMMIT_SIZE == 0:
            db.commit()
    db.commit()


def get_mod_id(db):

    query = text(
        f"SELECT mod_id "
        f"FROM mod "
        f"WHERE abbreviation = '{MOD}'"
    )
    row = db.execute(query).fetchone()
    return row[0]


def get_mod_references(db, mod_id):
    query = text(
        f"SELECT reference_id, corpus "
        f"FROM mod_corpus_association "
        f"WHERE mod_id = {mod_id}"
    )
    rows = db.execute(query).fetchall()
    return {row[0]: row[1] for row in rows}


def get_pmids(db):
    query = text(
        "SELECT reference_id, curie, is_obsolete "
        "FROM cross_reference "
        "WHERE curie_prefix = 'PMID'"
    )
    rows = db.execute(query).fetchall()
    return {row[1]: (row[0], row[2]) for row in rows}


if __name__ == "__main__":

    load_data()
