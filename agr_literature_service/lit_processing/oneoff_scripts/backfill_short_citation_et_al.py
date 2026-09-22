"""Backfill ``et al.`` in short citations for existing multi-author references.

Usage:
    python -m agr_literature_service.lit_processing.oneoff_scripts.backfill_short_citation_et_al
"""
import logging

from sqlalchemy import text

from agr_literature_service.api.triggers.citation_sql_func_triggers import add_citation_methods
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def backfill_short_citation_et_al():
    """Regenerate citations for references having multiple ordered authors."""
    db_session = create_postgres_session(False)
    try:
        logger.info("Installing updated citation functions...")
        add_citation_methods(db_session)

        reference_ids = db_session.execute(text("""
            SELECT reference_id
            FROM author
            WHERE author_order IS NOT NULL
            GROUP BY reference_id
            HAVING COUNT(*) > 1
            ORDER BY reference_id
        """)).scalars().all()
        total_count = len(reference_ids)
        logger.info("Found %s multi-author references to update", total_count)

        for count, reference_id in enumerate(reference_ids, start=1):
            db_session.execute(
                text("CALL update_citations(:reference_id)"),
                {"reference_id": reference_id}
            )
            if count % 1000 == 0:
                db_session.commit()
                logger.info("Processed %s/%s references", count, total_count)

        db_session.commit()
        logger.info("Completed updating %s citations", total_count)
    finally:
        db_session.close()


if __name__ == "__main__":
    backfill_short_citation_et_al()
