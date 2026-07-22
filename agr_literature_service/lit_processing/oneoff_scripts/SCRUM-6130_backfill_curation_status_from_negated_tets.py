"""SCRUM-6130: backfill curation_status rows from negated curation-status-form TETs.

For every negated topic_entity_tag whose source has
source_method = 'curation_status_form', create a curation_status row that mirrors
the original curation event:

  topic            <- TET.topic
  reference_id     <- TET.reference_id
  mod_id           <- TET.topic_entity_tag_source.secondary_data_provider_id
  curation_status  =  ATP:0000299   (constant)
  curation_tag     =  ATP:0000226   (constant)
  note             <- TET.note
  created_by       <- TET.created_by
  updated_by       <- TET.created_by
  date_created     <- TET.date_created
  date_updated     <- TET.date_created

The audit fields are copied verbatim to preserve the original curator and date, so
the row is built directly (NOT via curation_status_crud.create(), which overwrites
date_created with datetime.now()). AuditedModel.before_insert only fills date/user
fields when they are None, so the explicit values pass through untouched.

Idempotent: a curation_status row already present for a (topic, reference_id, mod_id)
key is skipped, so re-runs insert nothing new.
"""

import logging

from agr_literature_service.api.models import (
    CurationStatusModel,
    TopicEntityTagModel,
    TopicEntityTagSourceModel,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session


logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


CURATION_STATUS = "ATP:0000299"
CURATION_TAG = "ATP:0000226"
SOURCE_METHOD = "curation_status_form"

BATCH_COMMIT_SIZE = 500


def backfill():
    db = create_postgres_session(False)
    try:
        # Existing keys, so re-runs stay idempotent.
        existing_keys = {
            (topic, reference_id, mod_id)
            for topic, reference_id, mod_id in db.query(
                CurationStatusModel.topic,
                CurationStatusModel.reference_id,
                CurationStatusModel.mod_id,
            )
        }
        logger.info(f"existing curation_status rows: {len(existing_keys)}")

        rows = (
            db.query(
                TopicEntityTagModel.topic,
                TopicEntityTagModel.reference_id,
                TopicEntityTagSourceModel.secondary_data_provider_id.label("mod_id"),
                TopicEntityTagModel.created_by,
                TopicEntityTagModel.date_created,
                TopicEntityTagModel.note,
            )
            .join(
                TopicEntityTagSourceModel,
                TopicEntityTagModel.topic_entity_tag_source_id
                == TopicEntityTagSourceModel.topic_entity_tag_source_id,
            )
            .filter(
                TopicEntityTagModel.negated.is_(True),
                TopicEntityTagSourceModel.source_method == SOURCE_METHOD,
            )
            .all()
        )
        logger.info(f"eligible source TETs: {len(rows)}")

        inserted = 0
        skipped = 0
        for topic, reference_id, mod_id, created_by, date_created, note in rows:
            key = (topic, reference_id, mod_id)
            if key in existing_keys:
                skipped += 1
                continue

            db.add(CurationStatusModel(
                topic=topic,
                reference_id=reference_id,
                mod_id=mod_id,
                curation_status=CURATION_STATUS,
                curation_tag=CURATION_TAG,
                note=note,
                created_by=created_by,
                updated_by=created_by,
                date_created=date_created,
                date_updated=date_created,
            ))
            existing_keys.add(key)
            inserted += 1
            if inserted % BATCH_COMMIT_SIZE == 0:
                db.commit()
                logger.info(f"  committed {inserted} so far")

        db.commit()
        logger.info(f"done: inserted {inserted}, skipped {skipped}")
    except Exception as e:
        db.rollback()
        logger.error(f"error during backfill, rolled back: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    backfill()
