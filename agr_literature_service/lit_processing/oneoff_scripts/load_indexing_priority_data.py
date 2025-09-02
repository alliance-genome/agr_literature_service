import logging
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

confidence_score_file_for_papers_in_csv_file = "data/classification_results_for_papers_in_csvfile.csv"
confidence_score_file_for_new_papers_in_db = "data/classification_results_for_new_papers_in_db.csv"
priority_to_atp_mapping = {
    'priority_1': 'ATP:0000211',
    'priority_2': 'ATP:0000212',
    'priority_3': 'ATP:0000213'
}


def load_data():

    db = create_postgres_session(False)

    # The source for 'abc_document_classifier' does not exist for ZFIN in PROD DB
    source_id = get_source_id(db)
    if source_id is None:
        logger.info("The source for 'abc_document_classifier' does not exist for ZFIN")
        return

    logger.info(f"source_id={source_id}")

    ref_curie_to_score = get_mapping()

    logger.info("Retrieving indexing priority data from workflow_tag table...")

    rows = db.execute(text("""
        SELECT r.curie, r.reference_id, wft.mod_id, wft.workflow_tag_id, wft.date_created,
               wft.date_updated
        FROM reference r, workflow_tag wft
        WHERE r.reference_id = wft.reference_id
        AND wft.workflow_tag_id in ('ATP:0000211', 'ATP:0000212', 'ATP:0000213')
    """)).fetchall()

    logger.info("Loading data into indexing_priority table...")

    i = 0
    for x in rows:
        i += 1
        ref_curie = x[0]
        reference_id = x[1]
        mod_id = x[2]
        indexing_priority = x[3]
        date_created = x[4]
        date_updated = x[5]
        confidence_score = None

        if ref_curie in ref_curie_to_score:
            (priority_name, score) = ref_curie_to_score[ref_curie]
            confidence_score = score
            if indexing_priority != priority_to_atp_mapping[priority_name]:
                logger.info(f"DIFF {ref_curie} {indexing_priority} {priority_to_atp_mapping[priority_name]} {score}")
                indexing_priority = priority_to_atp_mapping[priority_name]

        confidence_score_sql = (
            str(confidence_score) if confidence_score is not None else "NULL"
        )
        query = f"""
        INSERT INTO indexing_priority (
          reference_id, mod_id, indexing_priority, confidence_score, source_id, date_created,
          date_updated, created_by, updated_by
        )
        VALUES ({reference_id}, {mod_id}, '{indexing_priority}', {confidence_score_sql}, {source_id},
          '{date_created}', '{date_updated}', 'default_user', 'default_user'
        )
        """
        db.execute(text(query))
        logger.info(f"{i} adding {indexing_priority} {confidence_score} for {ref_curie}")
        if i % 250 == 0:
            db.commit()
            # db.rollback()
    db.commit()
    # db.rollback()
    db.close()

    logger.info("DONE!")


def get_source_id(db):

    row = db.execute(text("""
        SELECT topic_entity_tag_source_id
        FROM topic_entity_tag_source
        WHERE source_method = 'abc_document_classifier'
        AND data_provider = 'ZFIN'
    """)).fetchone()
    if row:
        return row[0]
    return None


def get_mapping():

    mapping = {}
    with open(confidence_score_file_for_papers_in_csv_file) as f:
        for line in f:
            if line.startswith('AGRKB:'):
                items = line.strip().split(',')
                ref_curie = items[0]
                priority_name = items[2]
                score = round(float(items[3]), 2)
                mapping[ref_curie] = (priority_name, score)

    with open(confidence_score_file_for_new_papers_in_db) as f:
        for line in f:
            if line.startswith('AGRKB:'):
                items = line.strip().split(',')
                ref_curie = items[0]
                priority_name = items[1]
                score = round(float(items[2]), 2)
                mapping[ref_curie] = (priority_name, score)
    return mapping


if __name__ == "__main__":
    load_data()
