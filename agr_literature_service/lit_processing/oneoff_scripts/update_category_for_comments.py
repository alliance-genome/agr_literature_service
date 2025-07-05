from os import path
import logging
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import \
    get_alliance_category_from_pubmed_types
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def update_categories():

    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    # load all (reference_id, pubmed_types, category)
    rows = db.execute(text("""
        SELECT reference_id, pubmed_types, category
        FROM reference
        WHERE 'Comment' = ANY(pubmed_types)
    """)).fetchall()

    updates = []
    for ref_id, pubmed_types, old_cat in rows:
        new_cat = get_alliance_category_from_pubmed_types(pubmed_types or [])
        if new_cat != old_cat:
            logger.info(f"old: {old_cat} new: {new_cat}")
            updates.append((ref_id, new_cat))

    i = 0
    for (ref_id, category) in updates:
        i += 1
        try:
            db.execute(
                text("""
                UPDATE reference
                SET category = :category
                WHERE reference_id = :reference_id
                """),
                {"category": category, "reference_id": ref_id}
            )
            logger.info(f"{i}: reference_id = {ref_id}, new_category = {category}")
        except Exception as e:
            logger.info(f"{i}: error:{e} setting new_category = {category} for reference_id = {ref_id}")
        if i % 200 == 0:
            # db.rollback()
            db.commit()
    # db.rollback()
    db.commit()

    rows = db.execute(text("""
        SELECT rr.reference_id_from, r.category
        FROM reference_relation rr, reference r
        WHERE rr.reference_relation_type = 'CommentOn'
        AND rr.reference_id_from = r.reference_id
        AND r.category != 'Comment'
    """)).fetchall()

    i = 0
    for ref_id, old_cat in rows:
        i += 1
        try:
            db.execute(
                text("""
                UPDATE reference
                SET category = :category
                WHERE reference_id = :reference_id
                """),
                {"category": 'Comment', "reference_id": ref_id}
            )
            logger.info(f"{i} REFERENCE RELATION: old: {old_cat} new: Comment")
        except Exception as e:
            logger.info(f"{i} error: {e} setting REFERENCE RELATION: old: {old_cat} new: Comment for reference_id = {ref_id}")
        if i % 200 == 0:
            # db.rollback()
            db.commit()
    # db.rollback()
    db.commit()

    logger.info("DONE!")


if __name__ == "__main__":
    update_categories()
