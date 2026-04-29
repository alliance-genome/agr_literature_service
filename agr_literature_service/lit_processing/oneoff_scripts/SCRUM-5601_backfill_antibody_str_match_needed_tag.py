"""SCRUM-5601: backfill ATP:0000366 (antibody string matching classification
needed) for WB in-corpus references the legacy caltech antibody import did
NOT cover.

Skip rules:
  1. Skip references that already have a TET emitted by the caltech import
     (topic_entity_tag_source.source_method = 'string_matching_antibody').
  2. Skip references that already have any tag in the new four-state
     antibody-string-matching workflow process (idempotent re-runs).
"""

import logging
from os import path

from sqlalchemy import bindparam, text

from agr_literature_service.api.models import WorkflowTagModel
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
PROCESS_TAGS = (ANTIBODY_NEEDED, ANTIBODY_IN_PROGRESS,
                ANTIBODY_COMPLETE, ANTIBODY_FAILED)
LEGACY_CALTECH_SOURCE_METHOD = "string_matching_antibody"

BATCH_COMMIT_SIZE = 250


def backfill():
    db = create_postgres_session(False)
    set_global_user_id(db, path.basename(__file__).replace(".py", ""))

    mod_row = db.execute(text("SELECT mod_id FROM mod WHERE abbreviation = :m"),
                         {"m": WB}).fetchone()
    if not mod_row:
        logger.error(f"mod '{WB}' not found")
        return
    mod_id = int(mod_row[0])

    in_corpus = {r[0] for r in db.execute(text("""
        SELECT reference_id
          FROM mod_corpus_association
         WHERE mod_id = :mod_id
           AND corpus = TRUE
    """), {"mod_id": mod_id})}

    legacy_covered = {r[0] for r in db.execute(text("""
        SELECT DISTINCT tet.reference_id
          FROM topic_entity_tag tet
          JOIN topic_entity_tag_source tets
            ON tet.topic_entity_tag_source_id = tets.topic_entity_tag_source_id
         WHERE tets.source_method = :sm
    """), {"sm": LEGACY_CALTECH_SOURCE_METHOD})}

    in_new_process = {r[0] for r in db.execute(text("""
        SELECT reference_id FROM workflow_tag
         WHERE mod_id = :mod_id AND workflow_tag_id IN :tags
    """).bindparams(bindparam("tags", expanding=True)),
        {"mod_id": mod_id, "tags": list(PROCESS_TAGS)})}

    missing = in_corpus - legacy_covered - in_new_process
    logger.info(
        f"WB in-corpus: {len(in_corpus)} | "
        f"legacy-caltech-covered: {len(legacy_covered & in_corpus)} | "
        f"already in new process: {len(in_new_process & in_corpus)} | "
        f"to backfill: {len(missing)}"
    )

    inserted = 0
    for ref_id in missing:
        db.add(WorkflowTagModel(
            reference_id=ref_id,
            mod_id=mod_id,
            workflow_tag_id=ANTIBODY_NEEDED,
        ))
        inserted += 1
        if inserted % BATCH_COMMIT_SIZE == 0:
            db.commit()
            logger.info(f"  committed {inserted} so far")
    db.commit()
    logger.info(f"inserted {inserted} ATP:0000366 tags")


if __name__ == "__main__":
    backfill()
