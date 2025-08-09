from os import path
import logging
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import IndexingPriorityModel
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_data():

    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    source_id = get_source_id(db)
    if source_id is None:
        logger.info("The source for 'abc_document_classifier' does not exist for ZFIN")
        return

    logger.info(f"source_id={source_id}")

    logger.info("Retrieving indexing priority data from workflow_tag table...")

    rows = db.execute(text("""
        SELECT reference_id, mod_id, workflow_tag_id
        FROM workflow_tag
        WHERE workflow_tag_id in ('ATP:0000211', 'ATP:0000212', 'ATP:0000213')
    """)).fetchall()

    logger.info("Loading data into indexing_priority table...")

    i = 0
    for x in rows:
        i += 1
        reference_id = x[0]
        mod_id = x[1]
        workflow_tag_id = x[2]
        x = IndexingPriorityModel(
            indexing_priority=workflow_tag_id,
            reference_id=reference_id,
            mod_id=mod_id,
            source_id=source_id
        )
        db.add(x)
        logger.info(f"{i} adding {workflow_tag_id} for reference_id = {reference_id}")
        if i % 250 == 0:
            db.commit()
    db.commit()

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


if __name__ == "__main__":
    load_data()
