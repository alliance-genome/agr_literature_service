"""SCRUM-5601: Register the workflow_transition rows that drive the WB
antibody string-matching topic classifier.

Antibody string matching is its OWN workflow process (separate parent
from "reference classification") — so we follow the entity_extraction.py
pattern, NOT the classification.py pattern:

  - The entry transition is a virtual job-poll connector with
    transition_type='action' and condition='antibody_string_matching_job'.
    Its transition_from is a prior, semantically unrelated state
    (file converted to text, ATP:0000163) — same shape as
    entity_extraction.py:65-73 which uses
    'reference classification complete' -> '<entity> extraction needed'.
  - Intra-process transitions (needed/in_progress -> complete/failed)
    have NO sub_task_*::... actions because there is no parent process
    aggregating these states.

What this script does (idempotent — safe to re-run):

  (a) INSERTs four new transitions for WB:
       - file converted to text (ATP:0000163) -> ATP:0000366
         (condition='antibody_string_matching_job', transition_type='action')
         makes ATP:0000366 poll-able by
         load_all_jobs("antibody_string_matching_job").
       - ATP:0000366 -> ATP:0000365 (on_start)
       - ATP:0000365 -> ATP:0000363 (on_success)
       - ATP:0000365 -> ATP:0000364 (on_failed)

  (b) UPDATEs the two existing WB 'text conversion needed/in progress
      -> file converted to text (on_success)' rows by appending
       'proceed_on_value::reference_type::Experimental::ATP:0000366' to
      their actions arrays — so newly text-converted WB Experimental
      references automatically receive ATP:0000366.

ATP curies (confirmed):
  ATP:0000162 = text conversion needed
  ATP:0000198 = text conversion in progress
  ATP:0000163 = file converted to text
  ATP:0000363 = antibody string matching classification complete
  ATP:0000364 = antibody string matching classification failed
  ATP:0000365 = antibody string matching classification in progress
  ATP:0000366 = antibody string matching classification needed
"""

import logging
from os import path

from sqlalchemy import text

from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


WB = "WB"

ANTIBODY_NEEDED = "ATP:0000366"
ANTIBODY_IN_PROGRESS = "ATP:0000365"
ANTIBODY_COMPLETE = "ATP:0000363"
ANTIBODY_FAILED = "ATP:0000364"

TEXT_CONV_NEEDED = "ATP:0000162"
TEXT_CONV_IN_PROGRESS = "ATP:0000198"
FILE_CONVERTED = "ATP:0000163"

NEW_ACTION = (
    f"proceed_on_value::reference_type::Experimental::{ANTIBODY_NEEDED}"
)


# (transition_from, transition_to, condition, actions, transition_type)
NEW_TRANSITIONS = [
    # Entry transition: virtual job-poll connector. transition_type='action'
    # so transition_to_workflow_status (which only matches 'any' or
    # 'automated_only') skips it; it's solely there for the load_all_jobs
    # join. Mirrors entity_extraction.py:65-73.
    (FILE_CONVERTED, ANTIBODY_NEEDED, "antibody_string_matching_job", [], "action"),
    # Intra-process state transitions. No sub_task_*::... actions because
    # this is a standalone process, not a sub-task of reference classification.
    (ANTIBODY_NEEDED, ANTIBODY_IN_PROGRESS, "on_start", [], "any"),
    (ANTIBODY_IN_PROGRESS, ANTIBODY_COMPLETE, "on_success", [], "any"),
    (ANTIBODY_IN_PROGRESS, ANTIBODY_FAILED, "on_failed", [], "any"),
]


def insert_new_transitions(db, mod_id):
    inserted = 0
    skipped = 0
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
        db.execute(text("""
            INSERT INTO workflow_transition
                (mod_id, transition_from, transition_to, condition, actions,
                 transition_type, date_created)
            VALUES
                (:mod_id, :tf, :tt, :cond, :actions, :ttype, NOW())
        """), {"mod_id": mod_id, "tf": trans_from, "tt": trans_to,
               "cond": condition, "actions": actions, "ttype": transition_type})
        logger.info(f"  [insert] {trans_from} -> {trans_to} "
                    f"(condition='{condition}', actions={actions}, "
                    f"transition_type='{transition_type}')")
        inserted += 1
    db.commit()
    logger.info(f"transitions: inserted={inserted} skipped={skipped}")


def append_action_to_text_conversion_rows(db, mod_id):
    """Append NEW_ACTION to the WB on_success rows transitioning into
    'file converted to text'. Uses array_append + a NOT-already-in guard
    to stay idempotent.
    """
    sql = text("""
        UPDATE workflow_transition
           SET actions = array_append(COALESCE(actions, ARRAY[]::text[]), :new_action)
         WHERE mod_id = :mod_id
           AND transition_to = :file_converted
           AND transition_from IN (:tc_needed, :tc_in_progress)
           AND condition = 'on_success'
           AND NOT (:new_action = ANY(COALESCE(actions, ARRAY[]::text[])))
    """)
    result = db.execute(sql, {
        "new_action": NEW_ACTION,
        "mod_id": mod_id,
        "file_converted": FILE_CONVERTED,
        "tc_needed": TEXT_CONV_NEEDED,
        "tc_in_progress": TEXT_CONV_IN_PROGRESS,
    })
    db.commit()
    logger.info(f"text-conversion action append: rows updated = {result.rowcount}")


def main():
    db = create_postgres_session(False)
    set_global_user_id(db, path.basename(__file__).replace(".py", ""))

    mod_row = db.execute(text("SELECT mod_id FROM mod WHERE abbreviation = :m"),
                         {"m": WB}).fetchone()
    if not mod_row:
        logger.error(f"mod '{WB}' not found in mod table")
        return
    mod_id = int(mod_row[0])
    logger.info(f"Operating on mod_id={mod_id} ({WB})")

    logger.info("(a) inserting four-state transitions for antibody string matching")
    insert_new_transitions(db, mod_id)

    logger.info("(b) appending action to text-conversion -> file converted to text rows")
    append_action_to_text_conversion_rows(db, mod_id)

    logger.info("done.")


if __name__ == "__main__":
    main()
