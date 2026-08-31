"""Backfill topic_entity_tag.data_context (SCRUM-5697).

``data_context`` records what kind of data a tag represents, as one of four
disjoint ATP terms:

    ATP:0000325  experimentally studied data
    ATP:0000360  background information
    ATP:0000328  expression marker
    ATP:0000327  genetic marker

Every existing tag needs a value before the column can be made NOT NULL. Nothing
in the historical data distinguishes the four cases, and the curators' position
(SCRUM-5697, and confirmed for FlyBase in the ticket description) is that
everything already loaded represents experimentally studied data -- so this sets
``ATP:0000325`` for every row where ``data_context IS NULL``.

Two things make this backfill load-bearing rather than cosmetic:

  * ``check_for_duplicate_tags`` keys on *every* field of an incoming payload, so
    a backfilled value that disagrees with what the producers send would turn
    would-be 409 duplicates into brand-new rows. The value here and
    ``EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP`` in ``topic_entity_tag_crud`` must
    stay in step.
  * The NOT NULL revision cannot run until this reports zero remaining NULLs.

Run with ``--dry-run`` first: it reports the per-MOD counts without writing, which
is what to show curators before committing to the assertion.

Usage (the filename contains a hyphen, so run it by path, not with ``-m``)::

    python agr_literature_service/lit_processing/oneoff_scripts/SCRUM-5697_backfill_data_context.py --dry-run
    python agr_literature_service/lit_processing/oneoff_scripts/SCRUM-5697_backfill_data_context.py
"""
import argparse
import logging
from os import path

from sqlalchemy import text

from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP = "ATP:0000325"

BATCH_SIZE = 500


def report_null_counts(db) -> int:
    """Log how many tags still lack a data_context, broken down by owning MOD."""
    rows = db.execute(text("""
        SELECT m.abbreviation, count(*)
        FROM topic_entity_tag tet
        JOIN topic_entity_tag_source tets
          ON tet.topic_entity_tag_source_id = tets.topic_entity_tag_source_id
        JOIN mod m ON tets.secondary_data_provider_id = m.mod_id
        WHERE tet.data_context IS NULL
        GROUP BY m.abbreviation
        ORDER BY count(*) DESC
    """)).fetchall()
    total = 0
    for abbreviation, count in rows:
        logger.info(f"  {abbreviation}: {count}")
        total += count
    logger.info(f"Tags with data_context IS NULL: {total}")
    return total


def backfill_data_context(dry_run: bool = False):
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    logger.info("Counting tags without a data_context, by MOD:")
    total = report_null_counts(db)

    if total == 0:
        logger.info("Nothing to backfill.")
        db.close()
        return

    if dry_run:
        logger.info(f"--dry-run: would set data_context="
                    f"{EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP} on {total} tags")
        db.close()
        return

    # Update in bounded batches rather than one statement, so a very large table
    # does not hold a single long transaction open against the live API.
    updated = 0
    while True:
        result = db.execute(
            text("""
            UPDATE topic_entity_tag
            SET data_context = :data_context
            WHERE topic_entity_tag_id IN (
                SELECT topic_entity_tag_id
                FROM topic_entity_tag
                WHERE data_context IS NULL
                LIMIT :batch_size
            )
            """),
            {"data_context": EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP,
             "batch_size": BATCH_SIZE}
        )
        if not result.rowcount:
            break
        updated += result.rowcount
        db.commit()
        logger.info(f"Updated {updated} tags...")

    logger.info(f"Successfully set data_context on {updated} tags")
    logger.info("Re-checking:")
    remaining = report_null_counts(db)
    if remaining:
        logger.warning(f"{remaining} tags still have a NULL data_context; "
                       f"the NOT NULL migration will fail until this is zero")
    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="report the per-MOD counts without writing anything")
    args = parser.parse_args()
    backfill_data_context(dry_run=args.dry_run)
