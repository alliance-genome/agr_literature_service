import logging
from os import environ, path
import json
from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import AuthorModel
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import \
    generate_json

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sleep_time = 60
commit_size = 300


def load_data():

    db_session = create_postgres_session(False)

    base_path = environ.get('XML_PATH', "")
    json_path = base_path + "pubmed_json/"

    logger.info("Getting data from the database...")

    (pmid_to_reference_id, reference_id_to_pmid) = get_pmid_to_reference_id_mapping(db_session)
    pmids_all = list(pmid_to_reference_id.keys())
    pmids_all.sort()

    db_session.close()

    logger.info("Downloading pubmed xml files for " + str(len(pmids_all)) + " PMIDs...")
    download_pubmed_xml(pmids_all)

    logger.info("Generating json files...")
    not_found_xml_set = set()
    generate_json(pmids_all, [], not_found_xml_set)

    logger.info("Parsing json data...")
    key_to_first_initial = parse_json_data(json_path, pmids_all, pmid_to_reference_id)

    logger.info("back load first_initial into the database...")
    update_author_table(key_to_first_initial, reference_id_to_pmid)

    logger.info("DONE!")


def update_author_table(key_to_first_initial, reference_id_to_pmid):

    db_session = create_postgres_session(False)

    row_count = 0
    for reference_id in reference_id_to_pmid:
        for x in db_session.query(AuthorModel).filter_by(reference_id=reference_id).all():
            row_count += 1
            if row_count % commit_size == 0:
                db_session.commit()
            key = (x.reference_id, x.name, x.order)
            if key in key_to_first_initial and x.first_initial is None:
                pmid = reference_id_to_pmid[x.reference_id]
                first_initial = key_to_first_initial[key]
                try:
                    x.first_initial = key_to_first_initial[key]
                    db_session.add(x)
                    logger.info(f"row_count={row_count} PMID:{pmid} reference_id={x.reference_id} author_id={x.author_id} name={x.name} order={x.order}: added first_initial={first_initial}")
                except Exception as e:
                    logger.info(f"row_count={row_count} PMID:{pmid} reference_id={x.reference_id} author_id={x.author_id} name={x.name} order={x.order}: adding first_initial={first_initial} Falied e={e}")

        db_session.commit()


def get_pmid_to_reference_id_mapping(db_session: Session):

    pmid_to_reference_id = {}
    reference_id_to_pmid = {}
    rows = db_session.execute(text("SELECT cr.reference_id, cr.curie "
                                   "FROM cross_reference cr, author a "
                                   "WHERE cr.curie_prefix = 'PMID' "
                                   "AND cr.reference_id = a.reference_id "
                                   "AND a.first_initial is NULL")).fetchall()

    for x in rows:
        reference_id_to_pmid[x[0]] = x[1].replace('PMID:', '')
        pmid_to_reference_id[x[1].replace('PMID:', '')] = x[0]

    return (pmid_to_reference_id, reference_id_to_pmid)


def parse_json_data(json_path, pmids_all, pmid_to_reference_id):

    key_to_first_initial = {}
    row_count = 0
    for pmid in pmids_all:
        row_count += 1
        json_file = json_path + pmid + ".json"
        if not path.exists(json_file):
            continue
        f = open(json_file)
        json_data = json.load(f)
        f.close()
        reference_id = pmid_to_reference_id.get(pmid)
        if reference_id is None:
            continue
        authorsJSON = json_data.get('authors', [])
        if row_count % 2000 == 0:
            logger.info(f"row_count={row_count} PMID:{pmid}")
        for author in authorsJSON:
            if author.get('firstinit') is None:
                # for example, no firstname/firstinit in the first author for PMID:2570681
                # {
                #    "affiliations": [
                #        "Institute of Molecular Genetics, USSR Academy of Sciences, Moscow."
                #    ],
                #    "authorRank": 1,
                #    "lastname": "Shevelyov YuYa",
                #    "name": "Shevelyov YuYa"
                # },
                continue
            if author.get('name') is None or author.get('authorRank') is None:
                # this should not happen, but will check anyway
                continue
            key = (reference_id, author['name'], author['authorRank'])
            key_to_first_initial[key] = author['firstinit']
    return key_to_first_initial


if __name__ == "__main__":

    load_data()
