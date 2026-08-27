"""SCRUM-5764: Register the workflow rows that drive the ZFIN molecular-probe
abstract classifier.

Molecular probe classification is a sub-state of the new "abstract
classification" workflow process Ceri minted on 2026-08-21 (ATP:0000379, under
ATP:0000177 "workflow process"). The generic states are the parents and the
probe states are their children, so a second abstract classifier (SCRUM-5765
protocol papers) later slots in as another sibling without restructuring here.

Decision (Valerio, 2026-08-26): the probe queue reuses the existing
``classification_job`` label rather than getting its own. ``get_jobs`` filters
``workflow_transition.condition.contains(job_str)``, so the entry transition
below is picked up by the ``load_all_jobs("classification_job")`` call already
in agr_automated_information_extraction/agr_document_classifier_classify.py:553
with no code change there. Routing stays correct because that pipeline groups
jobs by ``(mod_id, topic_id)`` and selects features from the model's own
``ml_model.embedding_profile``, not from the queue -- so an abstract-profile
model reads title+abstract while a fulltext model reads its parquet, even in
the same batch.

What this script does (idempotent -- safe to re-run):

  (a) INSERTs six transitions for ZFIN, mirroring the shape of ZFIN's working
      pre-indexing chain (ATP:0000306), which also defines its terminal states
      from both "needed" and "in progress":
       - START_STATE -> ATP:0000380 (condition='abstract_classification_job')
         -- the virtual job-poll connector that makes ATP:0000380 visible to
         load_all_jobs. The condition contains 'classification_job' as a
         substring, and get_jobs filters with condition.contains(job_str), so
         the existing load_all_jobs("classification_job") call picks these up
         with no change on the classifier side.
       - ATP:0000380 -> ATP:0000383 (on_start)
       - ATP:0000380 -> ATP:0000387 (on_success)   <- see NEW_TRANSITIONS
       - ATP:0000380 -> ATP:0000385 (on_failed)    <- see NEW_TRANSITIONS
       - ATP:0000383 -> ATP:0000387 (on_success)
       - ATP:0000383 -> ATP:0000385 (on_failed)

  (b) INSERTs a workflow_tag_topic row mapping ATP:0000380 -> ATP:0000370 so
      load_all_jobs can resolve the topic through its outerjoin. Without this
      row get_jobs returns topic_id=None and load_all_jobs silently drops every
      probe job ("Skipping job with missing topic_id or mod_id").

NOT done here -- granting ATP:0000380 in the first place. The antibody script
appended a ``proceed_on_value::...`` action to the transition leading into its
upstream state so references acquired the "needed" tag automatically. The
equivalent for ZFIN depends on which transition (if any) grants UPSTREAM_STATE,
which has to be read off the live table -- run with --inspect first.

ATP curies:
  ATP:0000370 = molecular probe (topic)
  ATP:0000379 = abstract classification                      (process parent)
  ATP:0000380 = molecular probe classification needed
  ATP:0000383 = molecular probe classification in progress
  ATP:0000385 = molecular probe classification failed
  ATP:0000387 = molecular probe classification complete

Target DB comes from the environment (DB_HOST/DB_NAME/DB_USER/DB_PASSWORD), so
dev vs prod is selected by which env file is sourced. Writes require --apply;
without it the script reports what it would do and writes nothing.

Usage:
    set -a; source .env.rdsdev; set +a
    python3 SCRUM-5764_add_zfin_probe_transitions.py --inspect
    python3 SCRUM-5764_add_zfin_probe_transitions.py            # dry run
    python3 SCRUM-5764_add_zfin_probe_transitions.py --apply
"""

import argparse
import logging
from os import path

from sqlalchemy import text

# Models must load before api.user, otherwise the import is circular.
from agr_literature_service.api.models import WorkflowTransitionModel  # noqa: F401
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


MOD = "ZFIN"

PROBE_NEEDED = "ATP:0000380"
PROBE_IN_PROGRESS = "ATP:0000383"
PROBE_COMPLETE = "ATP:0000387"
PROBE_FAILED = "ATP:0000385"

PROBE_TOPIC = "ATP:0000370"

PROBE_STATES = (PROBE_NEEDED, PROBE_IN_PROGRESS, PROBE_COMPLETE, PROBE_FAILED)

# Verified against prod 2026-08-26: ZFIN's own acquisition-stage queue uses a
# START_STATE sentinel rather than a real prior tag --
#     START_STATE | ATP:0000306 | indexing_priority_job | any | {}
# -- which is the right shape here, because Ceri's trigger is "inside corpus"
# and nothing precedes it. mod_corpus_association_crud.py:82-91 grants ZFIN
# ATP:0000306 when the corpus association is created, so probe classification
# fires at the same moment. transition_type is 'any' to match that working row
# (the SCRUM-5601 antibody entry used 'action', but it hung off a real prior
# state; START_STATE is held by no reference so 'any' cannot misfire).
UPSTREAM_STATE = "START_STATE"

# Contains 'classification_job' as a substring, so the existing
# load_all_jobs("classification_job") call picks these jobs up with no code
# change, while still naming the process and leaving room for a future
# load_all_jobs("abstract_classification_job") that targets abstracts only.
# ZFIN has no other classification_job transitions today (only WB and FB do),
# so there is nothing to collide with either way.
JOB_CONDITION = "abstract_classification_job"

# (transition_from, transition_to, condition, actions, transition_type)
#
# The terminal transitions are defined from BOTH "needed" and "in progress",
# mirroring ZFIN's pre-indexing chain (ATP:0000306), which has five rows for the
# same reason. agr_document_classifier_classify.py calls set_job_success without
# a preceding set_job_started on the happy path -- set_job_started appears only
# on its failure branches -- so a successful job transitions straight out of
# "needed". Without PROBE_NEEDED -> PROBE_COMPLETE the POST matches no
# transition, the reference stays in "needed", and every subsequent run
# reclassifies it forever; for a positive, the drop-path writes then retry into
# a 422 duplicate. Verified on stage 2026-08-26: with only the in-progress rows
# all 9 test jobs stayed at ATP:0000380; with these two added, all 9 reached
# ATP:0000387.
NEW_TRANSITIONS = [
    (UPSTREAM_STATE, PROBE_NEEDED, JOB_CONDITION, [], "any"),
    (PROBE_NEEDED, PROBE_IN_PROGRESS, "on_start", [], "any"),
    (PROBE_NEEDED, PROBE_COMPLETE, "on_success", [], "any"),
    (PROBE_NEEDED, PROBE_FAILED, "on_failed", [], "any"),
    (PROBE_IN_PROGRESS, PROBE_COMPLETE, "on_success", [], "any"),
    (PROBE_IN_PROGRESS, PROBE_FAILED, "on_failed", [], "any"),
]


def describe_target(db):
    row = db.execute(text("SELECT current_database(), inet_server_addr()")).first()
    logger.info(f"target: db={row[0]} host={row[1]}")


def inspect(db, mod_id):
    """Read-only: report what already exists, so UPSTREAM_STATE can be confirmed."""
    logger.info(f"--- existing {MOD} transitions touching the probe states ---")
    rows = db.execute(text("""
        SELECT transition_from, transition_to, condition, actions, transition_type
          FROM workflow_transition
         WHERE mod_id = :mod_id
           AND (transition_to = ANY(:probe) OR transition_from = ANY(:probe))
         ORDER BY transition_from, transition_to
    """), {"mod_id": mod_id, "probe": list(PROBE_STATES)}).all()
    for r in rows or []:
        logger.info(f"  {r[0]} -> {r[1]}  condition={r[2]!r} actions={r[3]} type={r[4]}")
    if not rows:
        logger.info("  (none)")

    logger.info(f"--- {MOD} transitions whose condition mentions {JOB_CONDITION!r} ---")
    rows = db.execute(text("""
        SELECT transition_from, transition_to, condition, transition_type
          FROM workflow_transition
         WHERE mod_id = :mod_id AND condition LIKE :like
         ORDER BY transition_from
    """), {"mod_id": mod_id, "like": f"%{JOB_CONDITION}%"}).all()
    for r in rows or []:
        logger.info(f"  {r[0]} -> {r[1]}  condition={r[2]!r} type={r[3]}")
    if not rows:
        logger.info("  (none)")

    logger.info(f"--- is {UPSTREAM_STATE} actually reachable for {MOD}? ---")
    rows = db.execute(text("""
        SELECT transition_from, transition_to, condition, actions
          FROM workflow_transition
         WHERE mod_id = :mod_id AND transition_to = :up
    """), {"mod_id": mod_id, "up": UPSTREAM_STATE}).all()
    for r in rows or []:
        logger.info(f"  {r[0]} -> {r[1]}  condition={r[2]!r} actions={r[3]}")
    if not rows:
        logger.info(f"  no transition grants {UPSTREAM_STATE}; it is seeded "
                    f"elsewhere (acquisition script) or is the wrong choice")
    cnt = db.execute(text("""
        SELECT count(*) FROM workflow_tag
         WHERE mod_id = :mod_id AND workflow_tag_id = :up
    """), {"mod_id": mod_id, "up": UPSTREAM_STATE}).scalar()
    logger.info(f"  {MOD} references currently holding {UPSTREAM_STATE}: {cnt}")

    logger.info("--- workflow_tag_topic rows for the probe states ---")
    rows = db.execute(text("""
        SELECT workflow_tag, topic FROM workflow_tag_topic
         WHERE workflow_tag = ANY(:probe)
    """), {"probe": list(PROBE_STATES)}).all()
    for r in rows or []:
        logger.info(f"  {r[0]} -> {r[1]}")
    if not rows:
        logger.info("  (none)")


def insert_new_transitions(db, mod_id, apply_changes):
    inserted = skipped = 0
    for trans_from, trans_to, condition, actions, transition_type in NEW_TRANSITIONS:
        existing = db.execute(text("""
            SELECT 1 FROM workflow_transition
             WHERE mod_id = :mod_id
               AND transition_from = :tf
               AND transition_to = :tt
               AND COALESCE(condition, '') = :cond
        """), {"mod_id": mod_id, "tf": trans_from, "tt": trans_to,
               "cond": condition or ""}).first()
        if existing:
            logger.info(f"  [skip] {trans_from} -> {trans_to} "
                        f"(condition='{condition}') already exists")
            skipped += 1
            continue
        if apply_changes:
            db.execute(text("""
                INSERT INTO workflow_transition
                    (mod_id, transition_from, transition_to, condition, actions,
                     transition_type, date_created)
                VALUES
                    (:mod_id, :tf, :tt, :cond, :actions, :ttype, NOW())
            """), {"mod_id": mod_id, "tf": trans_from, "tt": trans_to,
                   "cond": condition, "actions": actions, "ttype": transition_type})
        logger.info(f"  [{'insert' if apply_changes else 'would insert'}] "
                    f"{trans_from} -> {trans_to} (condition='{condition}', "
                    f"actions={actions}, transition_type='{transition_type}')")
        inserted += 1
    if apply_changes:
        db.commit()
    logger.info(f"transitions: {'inserted' if apply_changes else 'pending'}={inserted} "
                f"skipped={skipped}")


def upsert_workflow_tag_topic(db, apply_changes):
    """Map PROBE_NEEDED -> PROBE_TOPIC. Idempotent: workflow_tag is UNIQUE, so
    INSERT only when missing, and never overwrite a conflicting mapping.
    """
    existing = db.execute(text("""
        SELECT topic FROM workflow_tag_topic WHERE workflow_tag = :wt
    """), {"wt": PROBE_NEEDED}).first()
    if existing:
        if existing[0] == PROBE_TOPIC:
            logger.info(f"  [skip] workflow_tag_topic {PROBE_NEEDED} -> "
                        f"{PROBE_TOPIC} already present")
        else:
            logger.warning(f"  [conflict] workflow_tag_topic has {PROBE_NEEDED} -> "
                           f"{existing[0]} (expected {PROBE_TOPIC}); leaving as-is")
        return
    if apply_changes:
        db.execute(text("""
            INSERT INTO workflow_tag_topic (workflow_tag, topic, date_created)
            VALUES (:wt, :topic, NOW())
        """), {"wt": PROBE_NEEDED, "topic": PROBE_TOPIC})
        db.commit()
    logger.info(f"  [{'insert' if apply_changes else 'would insert'}] "
                f"workflow_tag_topic {PROBE_NEEDED} -> {PROBE_TOPIC}")


def main():
    parser = argparse.ArgumentParser(description="SCRUM-5764 ZFIN probe workflow rows")
    parser.add_argument("--apply", action="store_true",
                        help="commit the changes; without it nothing is written")
    parser.add_argument("--inspect", action="store_true",
                        help="read-only report of the current state, then exit")
    args = parser.parse_args()

    db = create_postgres_session(False)
    set_global_user_id(db, path.basename(__file__).replace(".py", ""))
    describe_target(db)

    mod_row = db.execute(text("SELECT mod_id FROM mod WHERE abbreviation = :m"),
                         {"m": MOD}).fetchone()
    if not mod_row:
        logger.error(f"mod '{MOD}' not found in mod table")
        return
    mod_id = int(mod_row[0])
    logger.info(f"Operating on mod_id={mod_id} ({MOD})")

    if args.inspect:
        inspect(db, mod_id)
        return

    if not args.apply:
        logger.info("DRY RUN -- pass --apply to write. Current state:")
        inspect(db, mod_id)

    logger.info("(a) four-state transitions for molecular probe classification")
    insert_new_transitions(db, mod_id, args.apply)

    logger.info(f"(b) mapping {PROBE_NEEDED} -> {PROBE_TOPIC} in workflow_tag_topic")
    upsert_workflow_tag_topic(db, args.apply)

    logger.info("done." if args.apply else "dry run complete -- nothing written.")


if __name__ == "__main__":
    main()
