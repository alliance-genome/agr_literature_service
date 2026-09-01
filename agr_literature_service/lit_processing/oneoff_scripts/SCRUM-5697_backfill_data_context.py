"""Backfill topic_entity_tag.data_context (SCRUM-5697).

``data_context`` records what kind of data a tag represents. It is a hierarchy,
not a flat set:

    ATP:0000323  data context
    |-- ATP:0000324  mentioned data
    |   |-- ATP:0000360  background information
    |   +-- ATP:0000325  experimentally studied data
    +-- ATP:0000326  marker data
        |-- ATP:0000328  expression marker
        +-- ATP:0000327  genetic marker

Every existing tag needs a value before the column can be made NOT NULL
(revision e4f9a2c81b57). Nothing in the historical data says which term applies,
so curators specified per-MOD rules; each is a separate, separately-counted rule
below rather than one blanket UPDATE.

Rules, applied in order. Each only ever touches rows still NULL, so an earlier
rule wins and re-running is safe:

  1. WB, Alliance-provided, classification (no entity)  -> ATP:0000323
     Machine classification output for WormBase. The root term: the paper was
     classified for the topic, with no claim about what kind of data that is.

  2. WB, Alliance-provided, entity extraction (has entity) -> ATP:0000325
     Excludes anything already marked ATP:0000328 (expression marker) -- those
     keep the more specific term. Because the rule targets NULLs only, that
     exclusion is automatic, but it is stated explicitly so the intent survives
     a future edit.

  3. FB, any tag -> ATP:0000325

  4. Everything still NULL -> DEFAULT_DATA_CONTEXT
     The catch-all. The curator rules cover WB and FB; this is what MGI, RGD,
     ZFIN and SGD tags get, plus any WB/FB tag the rules above missed (e.g. a
     MOD-provided rather than Alliance-provided source). Review rule 4's
     dry-run count before running for real -- it is the least-specified part.

Note on the going-forward values, which differ from the backfill on purpose:
the WB entity-extraction pipelines are configured to send ATP:0000323, whereas
rule 2 gives historical WB entity tags ATP:0000325. That asymmetry is per
curator instruction, not an oversight.

Two things make this load-bearing rather than cosmetic:

  * ``check_for_duplicate_tags`` keys on every field of an incoming payload, so a
    backfilled value that disagrees with what the producers send turns would-be
    409 duplicates into brand-new rows.
  * The NOT NULL revision cannot run until this reports zero remaining NULLs.

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

DATA_CONTEXT_ROOT = "ATP:0000323"                 # data context
EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP = "ATP:0000325"
EXPRESSION_MARKER_DATA_CONTEXT_ATP = "ATP:0000328"

# What a tag gets when no curator rule covers it. Keep in step with
# EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP in topic_entity_tag_crud, which is what
# create_tag stamps on a tag whose caller omits data_context.
DEFAULT_DATA_CONTEXT = EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP

ALLIANCE_DATA_PROVIDER = "Alliance"

BATCH_SIZE = 500

# Each rule: (name, value, extra WHERE clause). "tet" is topic_entity_tag,
# "tets" its source, "m" the owning MOD. Every rule is implicitly scoped to
# tet.data_context IS NULL.
RULES = [
    (
        "1. WB / Alliance / classification (no entity)",
        DATA_CONTEXT_ROOT,
        f"m.abbreviation = 'WB' AND tets.data_provider = '{ALLIANCE_DATA_PROVIDER}' "
        f"AND tet.entity IS NULL",
    ),
    (
        "2. WB / Alliance / entity extraction (has entity)",
        EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP,
        f"m.abbreviation = 'WB' AND tets.data_provider = '{ALLIANCE_DATA_PROVIDER}' "
        f"AND tet.entity IS NOT NULL "
        f"AND tet.data_context IS DISTINCT FROM '{EXPRESSION_MARKER_DATA_CONTEXT_ATP}'",
    ),
    (
        "3. FB / any tag",
        EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP,
        "m.abbreviation = 'FB'",
    ),
    (
        "4. catch-all (all other MODs and anything the rules above missed)",
        DEFAULT_DATA_CONTEXT,
        "TRUE",
    ),
]

_SCOPE_JOIN = """
    FROM topic_entity_tag tet
    JOIN topic_entity_tag_source tets
      ON tet.topic_entity_tag_source_id = tets.topic_entity_tag_source_id
    JOIN mod m ON tets.secondary_data_provider_id = m.mod_id
    WHERE tet.data_context IS NULL AND ({extra})
"""


def count_for_rule(db, extra: str) -> int:
    sql = "SELECT count(*)" + _SCOPE_JOIN.format(extra=extra)
    return db.execute(text(sql)).scalar() or 0


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
        logger.info(f"    {abbreviation}: {count}")
        total += count
    logger.info(f"  Total with data_context IS NULL: {total}")
    return total


def apply_rule(db, name: str, value: str, extra: str) -> int:
    """Apply one rule in bounded batches. Returns the number of rows updated."""
    updated = 0
    while True:
        result = db.execute(
            text(f"""
            UPDATE topic_entity_tag
            SET data_context = :value
            WHERE topic_entity_tag_id IN (
                SELECT tet.topic_entity_tag_id
                {_SCOPE_JOIN.format(extra=extra)}
                LIMIT :batch_size
            )
            """),
            {"value": value, "batch_size": BATCH_SIZE}
        )
        if not result.rowcount:
            break
        updated += result.rowcount
        db.commit()
        logger.info(f"    ...{updated}")
    logger.info(f"  {name}: set {updated} tags to {value}")
    return updated


def backfill_data_context(dry_run: bool = False):
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    logger.info("Tags without a data_context, by MOD:")
    total = report_null_counts(db)
    if total == 0:
        logger.info("Nothing to backfill.")
        db.close()
        return

    if dry_run:
        logger.info("\n--dry-run: rows each rule WOULD claim (in order, "
                    "first rule wins):")
        remaining = total
        for name, value, extra in RULES:
            n = count_for_rule(db, extra)
            # Later rules only see what earlier ones leave behind; counting them
            # independently would double-count, so report against the running
            # remainder.
            claimed = min(n, remaining)
            logger.info(f"  {name}\n      -> {claimed} tags to {value}")
            remaining -= claimed
        logger.info(f"  Unclaimed after all rules: {remaining} (must be 0)")
        logger.info("\nNothing was written.")
        db.close()
        return

    logger.info("\nApplying rules in order:")
    total_updated = 0
    for name, value, extra in RULES:
        total_updated += apply_rule(db, name, value, extra)

    logger.info(f"\nSuccessfully set data_context on {total_updated} tags")
    logger.info("Re-checking:")
    remaining = report_null_counts(db)
    if remaining:
        logger.warning(f"{remaining} tags still have a NULL data_context; the NOT NULL "
                       f"migration (e4f9a2c81b57) will refuse to run until this is zero")
    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="report per-rule counts without writing anything")
    args = parser.parse_args()
    backfill_data_context(dry_run=args.dry_run)
