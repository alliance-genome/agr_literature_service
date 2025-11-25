import logging
import requests
from os import environ
from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_cognito_auth import (
    get_authentication_token,
    generate_headers
)

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

post_url = environ['API_URL'] + "topic_entity_tag/"
datafile = "./data/sgd_triage_data_20230824.txt"
mod = 'SGD'
entity_id_validation = "alliance"


def load_data():

    token = get_authentication_token()
    auth_headers = generate_headers(token)

    db_session = create_postgres_session(False)

    topic_entity_tag_source_id = get_source_id(db_session)

    entity_type_to_atp = entity_type_mapping()
    topic_to_atp = topic_mapping()
    sgdid_to_ref_curie = ref_curie_mapping(db_session)

    """
    The input file contains the following tab-delimited fields:
    reference_pmid
    reference_sgdid
    curation_tag
    entity_type
    entity_name
    entity_sgdid
    note
    created_by
    date_created
    """

    f = open(datafile)
    for line in f:
        if line.startswith('reference'):
            # ignore the header line
            continue
        pieces = line.strip().split("\t")
        if len(pieces) < 9:
            continue
        sgdid = pieces[1]
        ref_curie = sgdid_to_ref_curie.get(pieces[1])
        if ref_curie is None:
            logger.info(f"The reference {pieces[0]} {pieces[1]} is not in ABC")
            continue
        topic_atp = topic_to_atp.get(pieces[2])
        if topic_atp is None:
            logger.info(f"The topic {pieces[2]} can't be mapped to an ATP ID.")
            continue
        entity_type_atp = None
        entity_sgdid = None
        source = None
        if pieces[3]:
            entity_type_atp = entity_type_to_atp.get(pieces[3])
            if entity_type_atp is None:
                logger.info(f"The entity_type {pieces[3]} can't be mapped to an ATP ID.")
                continue
            entity_sgdid = pieces[5]
            source = entity_id_validation

        note = pieces[6] if pieces[6] else None
        created_by = pieces[7]
        date_created = pieces[8]
        data = {
            "date_created": date_created,
            "date_updated": date_created,
            "created_by": created_by,
            "updated_by": created_by,
            "topic": topic_atp,
            "entity_type": entity_type_atp,
            "entity": entity_sgdid,
            "entity_id_validation": source,
            "species": "NCBITaxon:559292",
            "topic_entity_tag_source_id": topic_entity_tag_source_id,
            "negated": False,
            "note": note,
            "reference_curie": ref_curie
        }
        try:
            response = requests.post(url=post_url, json=data, headers=auth_headers)
            print("response.status_code = ", response.status_code)
            if response.status_code == 201:
                logger.info(f"POST request successful for {sgdid}!")
            else:
                logger.info(f"POST request failed with status code for {sgdid}: {response.status_code}")
                logger.info(response.text)
        except Exception as e:
            logger.info(f"An error occurred when posting data for {sgdid}: {e}")

    f.close()
    db_session.close()
    logger.info("DONE!")


def ref_curie_mapping(db_session):

    sgdid_to_ref_curie = dict([(x[0], x[1]) for x in db_session.execute(text(
        f"SELECT cr.curie, r.curie "
        f"FROM   cross_reference cr, reference r "
        f"WHERE  cr.reference_id = r.reference_id "
        f"AND    cr.curie_prefix = '{mod}' "
        f"AND    cr.is_obsolete is False")).fetchall()])

    return sgdid_to_ref_curie


def get_source_id(db_session):

    rows = db_session.execute(text(f"SELECT t.topic_entity_tag_source_id "
                                   f"FROM   topic_entity_tag_source t, mod m "
                                   f"WHERE  t.mod_id = m.mod_id "
                                   f"AND    m.abbreviation = '{mod}'")).fetchall()
    return rows[0][0]


def entity_type_mapping():

    return {
        'gene': 'ATP:0000005',
        'genomic_region': 'ATP:0000057',
        'allele': 'ATP:0000006',
        'complex': 'ATP:0000128',
        'pathway': 'ATP:0000022'
    }


def topic_mapping():

    return {
        'Alleles': 'ATP:0000006',
        'Classical phenotype information': 'ATP:0000079',
        'Delay': 'delay',
        'Engineering': 'ATP:0000149',
        'Gene model': 'ATP:0000054',
        'GO information': 'ATP:0000012',
        'Headline information': 'ATP:0000129',
        'Homology/Disease': 'ATP:0000011',
        'HTP phenotype': 'ATP:0000085',
        'Non-phenotype HTP': 'ATP:0000150',
        'Pathways': 'ATP:0000022',
        'Post-translational modifications': 'ATP:0000088',
        'Regulation information': 'ATP:0000070',
        'Review': 'review'
    }


if __name__ == "__main__":

    load_data()
