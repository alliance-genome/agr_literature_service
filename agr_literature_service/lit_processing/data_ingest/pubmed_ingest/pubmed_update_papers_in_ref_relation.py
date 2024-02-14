import logging
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_single_mod \
    import update_data

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

batch_size = 500


def batch_update_data():

    db = create_postgres_session(False)
    rows = db.execute("SELECT distinct curie "
                      "FROM   cross_reference "
                      "WHERE  curie_prefix = 'PMID' "
                      "AND    (reference_id in (select reference_id_from from reference_relation) "
                      "OR      reference_id in (select reference_id_to from reference_relation))").fetchall()
    pmids = []
    batch_count = 0
    for x in rows:
        # total 12,559 rows
        pmid = x['curie'].replace("PMID:", '')
        pmids.append(pmid)
        if len(pmids) >= batch_size:
            batch_count += 1
            logger.info(f"{batch_count} Updating PubMed papers for {len(pmids)} PMIDs")
            update_data(None, '|'.join(pmids))
            pmids = []

    if len(pmids) > 0:
        batch_count += 1
        logger.info(f"{batch_count} Updating PubMed papers for {len(pmids)} PMIDs")
        update_data(None, '|'.join(pmids))


if __name__ == "__main__":

    batch_update_data()
