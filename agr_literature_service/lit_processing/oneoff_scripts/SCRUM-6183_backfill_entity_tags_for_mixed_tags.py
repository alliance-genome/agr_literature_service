"""SCRUM-6183: back-populate companion "pure entity" tags for existing POSITIVE
MIXED topic+entity tags.

For every positive mixed tag (``negated IS FALSE``, ``entity IS NOT NULL`` and
``topic <> entity_type``) that does not yet have a companion pure entity tag
(``topic == entity_type`` for the same reference + entity + entity_type), create
that companion tag with ``data_novelty`` set to the "existing data" term, reusing
the originating tag's source and entity fields.

Skip rules:
  1. SGD is excluded (it has its own data_novelty/display handling).
  2. Mixed tags that already have a companion pure entity tag are skipped
     (idempotent re-runs); ``create_entity_tag_for_mixed_tag`` re-checks too.
"""

import logging
from os import path

from sqlalchemy import text

from agr_literature_service.api.models import TopicEntityTagModel
from agr_literature_service.api.crud.topic_entity_tag_crud import \
    create_entity_tag_for_mixed_tag
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session


logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


SGD = "SGD"

# Columns carried from the originating mixed tag into the companion. topic,
# data_novelty and negated (plus the reset fields) are overridden by
# create_entity_tag_for_mixed_tag; date_created/date_updated are intentionally
# omitted so the new row gets fresh defaults.
CARRY_COLUMNS = [
    "reference_id", "topic", "entity_type", "entity", "entity_id_validation",
    "species", "display_tag", "confidence_level",
    "confidence_score", "negated", "note", "topic_entity_tag_source_id",
    "created_by", "updated_by", "data_novelty", "ml_model_id",
]


def backfill():
    db = create_postgres_session(False)
    set_global_user_id(db, path.basename(__file__).replace(".py", ""))

    candidate_ids = [r[0] for r in db.execute(text("""
        SELECT m.topic_entity_tag_id
          FROM topic_entity_tag m
          JOIN topic_entity_tag_source s
            ON m.topic_entity_tag_source_id = s.topic_entity_tag_source_id
         WHERE m.entity IS NOT NULL
           AND m.topic <> m.entity_type
           AND m.negated IS FALSE
           AND s.data_provider <> :sgd
           AND NOT EXISTS (
                 SELECT 1 FROM topic_entity_tag e
                  WHERE e.reference_id = m.reference_id
                    AND e.entity = m.entity
                    AND e.entity_type = m.entity_type
                    AND e.topic = e.entity_type)
    """), {"sgd": SGD})]

    logger.info(f"positive mixed tags missing a companion entity tag (excl SGD): "
                f"{len(candidate_ids)}")

    created = 0
    for tag_id in candidate_ids:
        mixed_tag = db.query(TopicEntityTagModel).filter(
            TopicEntityTagModel.topic_entity_tag_id == tag_id).one_or_none()
        if mixed_tag is None:
            continue
        mixed_tag_data = {col: getattr(mixed_tag, col) for col in CARRY_COLUMNS}
        before = db.query(TopicEntityTagModel).filter(
            TopicEntityTagModel.reference_id == mixed_tag.reference_id,
            TopicEntityTagModel.entity == mixed_tag.entity,
            TopicEntityTagModel.entity_type == mixed_tag.entity_type,
            TopicEntityTagModel.topic == mixed_tag.entity_type).count()
        create_entity_tag_for_mixed_tag(db, mixed_tag_data, mixed_tag.reference_id)
        after = db.query(TopicEntityTagModel).filter(
            TopicEntityTagModel.reference_id == mixed_tag.reference_id,
            TopicEntityTagModel.entity == mixed_tag.entity,
            TopicEntityTagModel.entity_type == mixed_tag.entity_type,
            TopicEntityTagModel.topic == mixed_tag.entity_type).count()
        if after > before:
            created += 1
            logger.info(f"  created companion entity tag for mixed tag {tag_id}")

    logger.info(f"created {created} companion entity tag(s)")


if __name__ == "__main__":
    backfill()
