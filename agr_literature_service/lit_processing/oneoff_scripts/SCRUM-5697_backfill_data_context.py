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

  1. WB, expression marker -> ATP:0000328
     Any WB tag whose topic or entity_type is ATP:0000328. Verified against
     stage 2026-09-02 this matches 0 rows -- no tag in any MOD carries that term
     as topic or entity_type. It is kept, and kept first, so the carve-out
     survives if that changes.

  2. WB, topic-only tags (no entity) -> ATP:0000323 data context
     The root term: the paper was tagged for the topic, with no claim about what
     kind of data that is.

  3. WB, tags with an entity -> ATP:0000325 experimentally studied data

     Rules 2 and 3 are Ceri Van Slyke's instruction on SCRUM-5697 (2026-09-01):
     "For WB entities are always experimentally studied ATP:0000325 ... Topic
     tags should get the 'ATP:0000323 data context' value. This won't be
     incorrect and is conservative."

     Note the axis: the split is by what the tag *is*, not by which pipeline
     produced it. That supersedes the earlier source-based split, under which
     only the three Alliance abc_ sources were treated specially. WormBase's own
     classifiers (nnc_*, svm_*), the ACKnowledge pipeline, the legacy *_script
     loaders and curator/author entry now all fall under the same two rules --
     which is what the question in the ticket asked for, since an nnc_/svm_
     classification tag is the same kind of claim as an abc_ one.

     One consequence worth naming: abc_entity_extractor emits 62,385 topic-only
     "no data" negatives alongside its 623,458 entity tags, and rule 2 gives
     those negatives ATP:0000323 rather than grouping them with the rest of
     their pipeline's output. That follows the instruction's wording -- it is
     about the tag, not the producer -- and is the conservative reading it asked
     for.

  4. FB, any tag -> ATP:0000325
     Per the FlyBase note on the ticket that all should be experimentally
     studied.

  5. All other MODs -> ATP:0000325

     Provisional: this is the ticket description's blanket default, and it
     stamps ATP:0000325 on AGR, MGI, SGD and ZFIN tags on the strength of
     reasoning that came from WB and FB. Question 1 of comment 97487 asks the
     curators to confirm it. Re-check before the real run.

Going-forward values are set per ml_model row rather than in pipeline code, and
after rules 2/3 they agree in shape with the backfill: a model that emits topic
tags should carry ATP:0000323, one that emits entity tags ATP:0000325. Two
places in agr_automated_information_extraction still assume ATP:0000325 for
classification and need to change with the ml_model values (comment 97488):
agr_antibody_string_matching_classifier.DATA_CONTEXT, and the
agr_document_classifier_trainer --data_context default.

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

# A tag with no entity is a topic tag. On stage every entity-less tag has a true
# NULL (1,508,213 NULL, 0 empty-string, checked 2026-09-02); the '' arm is
# defensive, since nothing in the schema stops a loader writing one.
NO_ENTITY = "(tet.entity IS NULL OR tet.entity = '')"

BATCH_SIZE = 500

# Each rule: (name, value, extra WHERE clause). "tet" is topic_entity_tag,
# "tets" its source, "m" the owning MOD. Every rule is implicitly scoped to
# tet.data_context IS NULL.
RULES = [
    (
        "1. WB / expression marker",
        EXPRESSION_MARKER_DATA_CONTEXT_ATP,
        f"m.abbreviation = 'WB' "
        f"AND (coalesce(tet.topic, '') = '{EXPRESSION_MARKER_DATA_CONTEXT_ATP}' "
        f"     OR coalesce(tet.entity_type, '') = '{EXPRESSION_MARKER_DATA_CONTEXT_ATP}')",
    ),
    (
        "2. WB / topic tags (no entity)",
        DATA_CONTEXT_ROOT,
        f"m.abbreviation = 'WB' AND {NO_ENTITY}",
    ),
    (
        "3. WB / tags with an entity",
        EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP,
        "m.abbreviation = 'WB'",
    ),
    (
        "4. FB / any tag",
        EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP,
        "m.abbreviation = 'FB'",
    ),
    (
        "5. all other MODs",
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
