"""Populate ml_model.data_context on the production models (SCRUM-5697).

``data_context`` records what kind of data a topic entity tag represents. The
terms are a hierarchy, not four disjoint options::

    ATP:0000323  data context
    |-- ATP:0000324  mentioned data
    |   |-- ATP:0000360  background information
    |   +-- ATP:0000325  experimentally studied data
    +-- ATP:0000326  marker data
        |-- ATP:0000328  expression marker
        +-- ATP:0000327  genetic marker

``ml_model.data_context`` is the authoritative source for every tag a pipeline
creates from that model: ``resolve_default_data_context`` in
``api/crud/topic_entity_tag_crud`` reads it when a producer omits the field, and
the extraction pipelines read it off the model row and send it explicitly. So
this is where the curation policy lives, and this script is what puts it there.

Which rows, and why
-------------------
Only ``production IS TRUE`` rows. ``production`` marks the current model; older
versions stay in the table but the pipelines only ever fetch the production one,
so historical rows can keep a NULL data_context (the column is nullable).

The rules, from the curators (Jira SCRUM-5697, comments 97488 and 97529):

  1. WB ``biocuration_topic_classification`` -> ATP:0000323 data context.
     Ceri Van Slyke, 2026-09-01: for WB, tags with an entity are experimentally
     studied data and topic tags take the root term -- classifying a paper for a
     topic makes no claim about what kind of data it holds. The backfill gives
     WB's existing topic-only tags the same value, so the two agree.

  2. Every other tag-producing model -> ATP:0000325 experimentally studied data.
     That covers WB's entity extractors (an entity tag is rule 1's other half),
     FB's topic classifiers, and ZFIN's. It is also what the server falls back
     to, so these rows make explicit what would otherwise be implicit.

  3. ``tfidf_vectorization`` rows are left NULL. They are vectorizer artifacts
     that create no tags, so there is no data context to record.

Agreement is load-bearing, not cosmetic: ``check_for_duplicate_tags`` filters on
every field of an incoming payload, so a model whose data_context disagrees with
what its existing tags carry turns would-be 409 duplicates into new rows.

A value that is already set is never overwritten. A row holding a term other
than the rule's is reported as a conflict and left alone -- a disagreement is
somebody's decision, not something to silently reverse.

Unlike ``SCRUM-5697_backfill_data_context.py`` this script needs no
``set_global_user_id``: it makes no ORM writes, so there is no automation user
for the audit hooks to attribute.

Usage::

    python agr_literature_service/lit_processing/oneoff_scripts/load_ml_model_data_context.py --dry-run
    python agr_literature_service/lit_processing/oneoff_scripts/load_ml_model_data_context.py
"""
import argparse
import logging
from typing import Dict, List, NamedTuple, Optional, Tuple

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

DATA_CONTEXT_ROOT = "ATP:0000323"                 # data context
EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP = "ATP:0000325"

# Keep in step with EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP in
# topic_entity_tag_crud, which is what the server falls back to when neither the
# client nor the model supplies a value.
DEFAULT_DATA_CONTEXT = EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP

TOPIC_CLASSIFICATION_TASK_TYPE = "biocuration_topic_classification"

# Task types that produce no topic entity tags, so carry no data context.
NO_TAG_TASK_TYPES = frozenset({"tfidf_vectorization"})

# MODs whose topic classifiers take the root term rather than the default.
# WB only, per Ceri's answer; FB's and ZFIN's take the default.
ROOT_TERM_TOPIC_CLASSIFIER_MODS = frozenset({"WB"})


class ModelRow(NamedTuple):
    ml_model_id: int
    abbreviation: str
    task_type: str
    data_context: Optional[str]


class UpdatePlan(NamedTuple):
    to_set: Dict[int, str]
    skipped_no_tags: List[int]
    already_correct: List[int]
    # (ml_model_id, the value the row holds, the value the rule wanted)
    conflicts: List[Tuple[int, str, Optional[str]]]


def data_context_for(abbreviation: str, task_type: str) -> Optional[str]:
    """The data_context a model should carry, or None to leave it unset."""
    if task_type in NO_TAG_TASK_TYPES:
        return None
    if (abbreviation in ROOT_TERM_TOPIC_CLASSIFIER_MODS
            and task_type == TOPIC_CLASSIFICATION_TASK_TYPE):
        return DATA_CONTEXT_ROOT
    return DEFAULT_DATA_CONTEXT


def fetch_production_models(db) -> List[ModelRow]:
    rows = db.execute(text("""
        SELECT ml.ml_model_id, m.abbreviation, ml.task_type, ml.data_context
        FROM ml_model ml
        JOIN mod m ON ml.mod_id = m.mod_id
        WHERE ml.production IS TRUE
        ORDER BY m.abbreviation, ml.task_type, ml.ml_model_id
    """)).fetchall()
    return [ModelRow(ml_model_id=r[0], abbreviation=r[1], task_type=r[2],
                     data_context=r[3]) for r in rows]


def plan_updates(rows: List[ModelRow]) -> UpdatePlan:
    """Decide what to write without writing it, so --dry-run is exact."""
    to_set: Dict[int, str] = {}
    skipped_no_tags: List[int] = []
    already_correct: List[int] = []
    conflicts: List[Tuple[int, str, Optional[str]]] = []
    for row in rows:
        wanted = data_context_for(row.abbreviation, row.task_type)
        if row.data_context is not None:
            if row.data_context == wanted:
                already_correct.append(row.ml_model_id)
            else:
                conflicts.append((row.ml_model_id, row.data_context, wanted))
            continue
        if wanted is None:
            skipped_no_tags.append(row.ml_model_id)
            continue
        to_set[row.ml_model_id] = wanted
    return UpdatePlan(to_set=to_set, skipped_no_tags=skipped_no_tags,
                      already_correct=already_correct, conflicts=conflicts)


def apply_updates(db, to_set: Dict[int, str], dry_run: bool) -> int:
    """Write the planned values. Returns the number of rows updated.

    The UPDATE repeats the ``data_context IS NULL`` guard rather than trusting
    the plan: it costs nothing and means a value written between the SELECT and
    here is left alone instead of being clobbered.
    """
    if dry_run:
        return 0
    updated = 0
    for ml_model_id, data_context in to_set.items():
        result = db.execute(
            text("""
            UPDATE ml_model
            SET data_context = :data_context
            WHERE ml_model_id = :ml_model_id
              AND data_context IS NULL
            """),
            {"ml_model_id": ml_model_id, "data_context": data_context}
        )
        updated += result.rowcount
    db.commit()
    return updated


def report(rows: List[ModelRow], plan: UpdatePlan) -> None:
    logger.info(f"Production models: {len(rows)}")
    for row in rows:
        wanted = data_context_for(row.abbreviation, row.task_type)
        if row.ml_model_id in plan.to_set:
            state = f"-> {plan.to_set[row.ml_model_id]}"
        elif row.ml_model_id in plan.already_correct:
            state = f"already {row.data_context}"
        elif row.ml_model_id in plan.skipped_no_tags:
            state = "left NULL (creates no tags)"
        else:
            state = f"CONFLICT: holds {row.data_context}, rule wants {wanted}"
        logger.info(f"    {row.ml_model_id:>5}  {row.abbreviation:<5} "
                    f"{row.task_type:<45} {state}")
    logger.info(f"\n  to set: {len(plan.to_set)}"
                f"\n  already correct: {len(plan.already_correct)}"
                f"\n  left NULL (no tags): {len(plan.skipped_no_tags)}"
                f"\n  conflicts: {len(plan.conflicts)}")
    if plan.conflicts:
        logger.warning("Conflicting rows were NOT changed. Each one holds a term "
                       "other than the rule's; resolve them by hand if the rule "
                       "is right, or update the rule if the row is.")


def load_ml_model_data_context(dry_run: bool = False) -> None:
    db = create_postgres_session(False)
    try:
        if not data_context_column_exists(db):
            raise SystemExit(
                "ml_model.data_context does not exist on this database. Apply "
                "migration d7b3e1c95a24 first, then re-run.")
        rows = fetch_production_models(db)
        plan = plan_updates(rows)
        report(rows, plan)
        if dry_run:
            logger.info("\n--dry-run: nothing was written.")
            return
        updated = apply_updates(db, plan.to_set, dry_run=False)
        logger.info(f"\nSet data_context on {updated} ml_model row(s).")
        if updated != len(plan.to_set):
            logger.warning(f"{len(plan.to_set) - updated} planned row(s) were not "
                           f"updated -- a value was written between the plan and "
                           f"the apply. Re-run to see the current state.")
    finally:
        db.close()


def data_context_column_exists(db) -> bool:
    return bool(db.execute(text("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'ml_model' AND column_name = 'data_context'
    """)).fetchone())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would be written without writing it")
    args = parser.parse_args()
    load_ml_model_data_context(dry_run=args.dry_run)
