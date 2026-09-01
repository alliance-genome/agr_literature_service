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

  1. WB, Alliance classification pipelines -> ATP:0000323 data context
     abc_document_classifier and abc_string_matching_antibody. The root term:
     the paper was classified for the topic, with no claim about what kind of
     data that is.

  2a. WB, Alliance entity extraction whose topic or entity_type is ATP:0000328
      expression marker -> ATP:0000328
  2b. All other WB Alliance entity extraction -> ATP:0000325
      abc_entity_extractor, including its topic-only "no data" negatives -- they
      come from the extraction pipeline, so they belong with it rather than with
      classification. On stage 2a matches 0 rows: no WB tag carries ATP:0000328
      as a topic or entity_type. It is kept so the carve-out survives if that
      changes.

  3. FB, any tag -> ATP:0000325

  4a. WB or FB tags the rules above did not claim -> ATP:0000325
  4b. All other MODs                              -> ATP:0000325

     Same value; split only so the dry-run distinguishes them. 4b is the
     curators' rule for the remaining MODs. 4a is the WB/FB remainder: WormBase's
     own pipelines (nnc_*, svm_*, ACKnowledge_*, *_script) and curator/author
     entry, which are not Alliance tags and so are not covered by rules 1-2.

On identifying "Alliance": data_provider never holds the value "Alliance" -- it
is always a MOD abbreviation. Verified against stage 2026-09-01, exactly 3 of
WormBase's 56 sources describe themselves as Alliance pipelines, and they are
the three abc_-prefixed machine sources named above.

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
from typing import List

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

# "Alliance tags" in the curator instruction. data_provider never holds
# "Alliance" -- it is always a MOD abbreviation (verified on stage 2026-09-01) --
# so the Alliance pipelines are identified by source_method. These are exactly
# the WB sources whose topic_entity_tag_source.description calls them an
# Alliance pipeline: 3 of 56, all abc_-prefixed and all machine evidence.
# abc_literature_system is deliberately absent: it is the ABC curator entry form
# (ATP:0000036), not a pipeline.
ALLIANCE_CLASSIFICATION_METHODS = ("abc_document_classifier",
                                   "abc_string_matching_antibody")
ALLIANCE_ENTITY_EXTRACTION_METHODS = ("abc_entity_extractor",)


def _in_list(values):
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


BATCH_SIZE = 500

# Each rule: (name, value, extra WHERE clause). "tet" is topic_entity_tag,
# "tets" its source, "m" the owning MOD. Every rule is implicitly scoped to
# tet.data_context IS NULL.
RULES = [
    (
        "1. WB / Alliance classification pipelines",
        DATA_CONTEXT_ROOT,
        f"m.abbreviation = 'WB' "
        f"AND tets.source_method IN {_in_list(ALLIANCE_CLASSIFICATION_METHODS)}",
    ),
    (
        "2a. WB / Alliance entity extraction, expression marker",
        EXPRESSION_MARKER_DATA_CONTEXT_ATP,
        f"m.abbreviation = 'WB' "
        f"AND tets.source_method IN {_in_list(ALLIANCE_ENTITY_EXTRACTION_METHODS)} "
        f"AND (coalesce(tet.topic, '') = '{EXPRESSION_MARKER_DATA_CONTEXT_ATP}' "
        f"     OR coalesce(tet.entity_type, '') = '{EXPRESSION_MARKER_DATA_CONTEXT_ATP}')",
    ),
    (
        "2b. WB / Alliance entity extraction, everything else",
        EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP,
        f"m.abbreviation = 'WB' "
        f"AND tets.source_method IN {_in_list(ALLIANCE_ENTITY_EXTRACTION_METHODS)}",
    ),
    (
        "3. FB / any tag",
        EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP,
        "m.abbreviation = 'FB'",
    ),
    (
        "4a. WB or FB tags the rules above did not claim",
        DEFAULT_DATA_CONTEXT,
        "m.abbreviation IN ('WB', 'FB')",
    ),
    (
        "4b. all other MODs",
        DEFAULT_DATA_CONTEXT,
        "TRUE",
    ),
]

_SCOPE_JOIN = """
    FROM topic_entity_tag tet
    JOIN topic_entity_tag_source tets
      ON tet.topic_entity_tag_source_id = tets.topic_entity_tag_source_id
    JOIN mod m ON tets.secondary_data_provider_id = m.mod_id
    WHERE {unset} AND ({extra})
"""


def data_context_column_exists(db) -> bool:
    """Whether the migration that adds the column (d7b3e1c95a24) has been applied.

    A --dry-run is useful before it has: the counts are what curators need in
    order to sign the rules off, and waiting for the deploy to produce them puts
    the decision on the critical path for no reason.
    """
    return bool(db.execute(text("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'topic_entity_tag' AND column_name = 'data_context'
    """)).fetchone())


def unset_clause(column_exists: bool) -> str:
    """Rows the backfill would still have to set.

    Once the column exists that is the NULL ones; before it exists every row
    qualifies, which is exactly what the counts should reflect.
    """
    return "tet.data_context IS NULL" if column_exists else "TRUE"


def count_for_rule(db, extra: str, unset: str = "tet.data_context IS NULL") -> int:
    sql = "SELECT count(*)" + _SCOPE_JOIN.format(extra=extra, unset=unset)
    return db.execute(text(sql)).scalar() or 0


def report_null_counts(db, unset: str = "tet.data_context IS NULL") -> int:
    """Log how many tags still need a data_context, broken down by owning MOD."""
    rows = db.execute(text(f"""
        SELECT m.abbreviation, count(*)
        FROM topic_entity_tag tet
        JOIN topic_entity_tag_source tets
          ON tet.topic_entity_tag_source_id = tets.topic_entity_tag_source_id
        JOIN mod m ON tets.secondary_data_provider_id = m.mod_id
        WHERE {unset}
        GROUP BY m.abbreviation
        ORDER BY count(*) DESC
    """)).fetchall()
    total = 0
    for abbreviation, count in rows:
        logger.info(f"    {abbreviation}: {count}")
        total += count
    logger.info(f"  Total needing a value: {total}")
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
                {_SCOPE_JOIN.format(extra=extra, unset='tet.data_context IS NULL')}
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
    if not dry_run:
        # set_global_user_id -> _ensure_automation_user INSERTs a users row when the
        # automation user does not exist yet. Harmless for a real run, but it would
        # make --dry-run write to the database it is only supposed to be reading.
        set_global_user_id(db, script_name)

    column_exists = data_context_column_exists(db)
    unset = unset_clause(column_exists)
    if not column_exists:
        if not dry_run:
            raise SystemExit(
                "topic_entity_tag.data_context does not exist on this database. "
                "Apply migration d7b3e1c95a24 first, then re-run.\n"
                "(--dry-run works without it, and reports every tag as needing a value.)")
        logger.info("NOTE: data_context does not exist on this database yet "
                    "(migration d7b3e1c95a24 not applied), so every tag counts as "
                    "needing a value. These are the numbers the rules would produce "
                    "on deploy day.\n")

    logger.info("Tags needing a data_context, by MOD:")
    total = report_null_counts(db, unset)
    if total == 0:
        logger.info("Nothing to backfill.")
        db.close()
        return

    if dry_run:
        logger.info("\n--dry-run: rows each rule WOULD claim (in order, "
                    "first rule wins):")
        # A later rule's clause can be a superset of an earlier one's -- 2b
        # contains 2a -- so counting each independently overstates the later
        # rule. Exclude everything the earlier rules would already have taken,
        # which is what the ordered UPDATEs actually do.
        claimed_so_far: List[str] = []
        remaining = total
        for name, value, extra in RULES:
            if claimed_so_far:
                # COALESCE is load-bearing: a clause comparing a nullable column
                # (entity_type on a topic-only tag) evaluates to NULL, not FALSE,
                # and NOT (FALSE OR NULL) is NULL -- which silently drops the row
                # from the count. The real run is unaffected, since apply_rule
                # relies on rule order rather than an exclusion, so without this
                # the dry-run under-reports what the apply would do.
                scoped = (f"({extra}) AND NOT COALESCE("
                          f"{' OR '.join(claimed_so_far)}, FALSE)")
            else:
                scoped = extra
            claimed = count_for_rule(db, scoped, unset)
            logger.info(f"  {name}\n      -> {claimed} tags to {value}")
            claimed_so_far.append(f"({extra})")
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
