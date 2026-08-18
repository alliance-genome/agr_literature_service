"""
One-off script to fix citations that contain empty separators, e.g.
"The Alliance of Genome Resources Curators (2018)   ():".

This script:
1. Installs the updated SQL trigger functions (update_citations et al.)
2. Re-runs update_citations for every reference whose stored citation or
   short_citation still shows an artifact of the old template, regenerating
   both strings with the new template:
   - volume/issue/pages separators are only emitted around non-empty values
   - the year parentheses are omitted when there is no publication year
   - the author/year comma is omitted when nothing follows the authors
   - the short citation falls back to the full journal title, then to the
     reference title (e.g. for category Internal_Process_Reference), when
     there is no resource abbreviation

The default selection matches every artifact class of the old template, not
just empty "()": leading ", " (no authors), trailing ":" (no page range),
trailing "," (authors only), double spaces (blank journal or abbreviation)
and a leading space in the short citation. Titles that legitimately contain
these patterns may also be selected; regenerating them is harmless since the
procedure is idempotent.

Usage:
    python -m agr_literature_service.lit_processing.oneoff_scripts.fix_empty_separator_citations
    python -m agr_literature_service.lit_processing.oneoff_scripts.fix_empty_separator_citations --all
    python -m agr_literature_service.lit_processing.oneoff_scripts.fix_empty_separator_citations --dry-run

With --all, every reference with a citation is regenerated (like
update_long_citations.py) instead of only the ones matching the malformed
patterns.
"""
import argparse
import logging
from sqlalchemy import text

from agr_literature_service.api.triggers.citation_sql_func_triggers import add_citation_methods
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

COMMIT_BATCH_SIZE = 1000

MALFORMED_CITATIONS_QUERY = """
    SELECT r.reference_id
    FROM reference r
    JOIN citation c ON r.citation_id = c.citation_id
    WHERE c.citation LIKE '%()%'
       OR c.short_citation LIKE '%()%'
       OR c.citation LIKE ', %'
       OR c.citation LIKE '%:'
       OR c.citation LIKE '%,'
       OR c.citation LIKE '%  %'
       OR c.short_citation LIKE '%:'
       OR c.short_citation LIKE '%  %'
       OR c.short_citation LIKE ' %'
"""

ALL_CITATIONS_QUERY = """
    SELECT reference_id FROM reference WHERE citation_id IS NOT NULL
"""


def fix_empty_separator_citations(update_all=False, dry_run=False):
    """Regenerate citations that still contain empty-separator artifacts."""
    db_session = create_postgres_session(False)

    try:
        query = ALL_CITATIONS_QUERY if update_all else MALFORMED_CITATIONS_QUERY
        logger.info("Fetching references to update...")
        rows = db_session.execute(text(query)).fetchall()
        total_count = len(rows)
        logger.info(f"Found {total_count} references to update")

        if dry_run:
            logger.info("Dry run: no citations were regenerated")
            return

        logger.info("Installing updated SQL trigger functions...")
        add_citation_methods(db_session)

        count = 0
        for x in rows:
            count += 1
            ref_id = int(x[0])
            db_session.execute(
                text("CALL update_citations(:param)"),
                {'param': ref_id}
            )
            if count % COMMIT_BATCH_SIZE == 0:
                logger.info(f"Processed {count}/{total_count} references")
                db_session.commit()

        db_session.commit()
        logger.info(f"Completed updating {count} citations")
    finally:
        db_session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Regenerate citations containing empty-separator artifacts")
    parser.add_argument(
        "--all", action="store_true",
        help="regenerate citations for all references, not just malformed ones")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="only report how many references would be updated")
    args = parser.parse_args()
    fix_empty_separator_citations(update_all=args.all, dry_run=args.dry_run)
