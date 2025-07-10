"""
Populate data_novelty column based on novel_topic_data and source_evidence_assertion.

This script sets the data_novelty column to "ATP:0000321" when:
- novel_topic_data is true AND
- topic_entity_tag_source.source_evidence_assertion is "ATP:0000036"
"""
from os import path
import logging
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def populate_data_novelty():
    """
    Populate data_novelty column based on novel_topic_data and source_evidence_assertion.
    """
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    # Get all topic_entity_tags where novel_topic_data is true
    # and join with source to check source_evidence_assertion
    rows = db.execute(text("""
        SELECT tet.topic_entity_tag_id, tet.novel_topic_data, tets.source_evidence_assertion
        FROM topic_entity_tag tet
        JOIN topic_entity_tag_source tets ON tet.topic_entity_tag_source_id = tets.topic_entity_tag_source_id
        WHERE tet.novel_topic_data = true
    """)).fetchall()

    updates = []
    for tag_id, novel_topic_data, source_evidence_assertion in rows:
        # Set data_novelty to ATP:0000321 when novel_topic_data is true
        # AND source_evidence_assertion is ATP:0000036
        if novel_topic_data and source_evidence_assertion == "ATP:0000036":
            updates.append((tag_id, "ATP:0000321"))
            logger.info(f"Will update topic_entity_tag_id {tag_id}: "
                        f"novel_topic_data={novel_topic_data}, "
                        f"source_evidence_assertion={source_evidence_assertion}, "
                        f"setting data_novelty=ATP:0000321")

    logger.info(f"Found {len(updates)} records to update")

    # Update records in batches
    i = 0
    for (tag_id, data_novelty) in updates:
        i += 1
        try:
            db.execute(
                text("""
                UPDATE topic_entity_tag
                SET data_novelty = :data_novelty
                WHERE topic_entity_tag_id = :topic_entity_tag_id
                """),
                {"data_novelty": data_novelty, "topic_entity_tag_id": tag_id}
            )
            if i % 100 == 0:
                logger.info(f"Updated {i} records...")
        except Exception as e:
            logger.error(f"Error updating topic_entity_tag_id {tag_id}: {e}")

        if i % 500 == 0:
            db.commit()
            logger.info(f"Committed {i} records")

    db.commit()
    logger.info(f"Successfully updated {i} records total")
    db.close()


if __name__ == "__main__":
    populate_data_novelty()
