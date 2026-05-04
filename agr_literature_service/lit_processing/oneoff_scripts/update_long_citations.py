"""
One-off script to update all existing long citations with the new author format.

This script:
1. Installs the updated SQL trigger functions
2. Re-runs update_citations for all references to regenerate long citations

The new format uses:
- Author names as "Last name First initial" (e.g., "Smith JP" for "John-Paul Smith")
- First initials are derived from first_name, with spaces and hyphens treated as separators
- Title period is only added if title doesn't already end with punctuation (.?!)

Usage:
    python -m agr_literature_service.lit_processing.oneoff_scripts.update_long_citations
"""
import logging
from sqlalchemy import text

from agr_literature_service.api.triggers.citation_sql_func_triggers import add_citation_methods
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def update_long_citations():
    """Update all existing long citations with the new author format."""
    db_session = create_postgres_session(False)

    # First, install the updated SQL trigger functions
    logger.info("Installing updated SQL trigger functions...")
    add_citation_methods(db_session)

    # Get all reference IDs that have a citation
    logger.info("Fetching all references with citations...")
    rows = db_session.execute(
        text("SELECT reference_id FROM reference WHERE citation_id IS NOT NULL")
    ).fetchall()

    total_count = len(rows)
    logger.info(f"Found {total_count} references to update")

    count = 0
    for x in rows:
        count += 1
        ref_id = int(x[0])
        db_session.execute(
            text("CALL update_citations(:param)"),
            {'param': ref_id}
        )
        if count % 1000 == 0:
            logger.info(f"Processed {count}/{total_count} references")
            db_session.commit()

    db_session.commit()
    logger.info(f"Completed updating {count} citations")
    db_session.close()


if __name__ == "__main__":
    update_long_citations()
