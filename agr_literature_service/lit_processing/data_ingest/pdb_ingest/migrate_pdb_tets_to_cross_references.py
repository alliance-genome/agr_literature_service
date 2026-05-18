"""One-off migration: convert PDB topic_entity_tag rows into cross_reference
rows, then replace them with a single topic-only TET per reference.

After this runs:
- Every PDB ID that was in a TET `entity` field lives as a `PDB:xxxx` row in
  `cross_reference`.
- Every reference that previously had any PDB TET still has a single
  topic-only TET (`topic=ATP:0000091`, `entity=None`) under the same TET
  source row, flagging it as a protein-structure paper for downstream
  curation workflows.
- The TET source row itself is preserved (the ongoing load script keeps
  writing topic-only TETs through it).

Idempotent. Looks up the PDB TET source row by its identifying fields and
exits cleanly if it's already gone.
"""
import logging
from os import path
from typing import Dict, Optional

from fastapi import HTTPException
from sqlalchemy import delete
from sqlalchemy.orm import Session

from agr_literature_service.api.crud import cross_reference_crud
from agr_literature_service.api.crud.topic_entity_tag_crud import create_tag
from agr_literature_service.api.models import (
    CrossReferenceModel,
    ReferenceModel,
    TopicEntityTagModel,
    TopicEntityTagSourceModel,
)
from agr_literature_service.api.schemas.cross_reference_schemas import (
    CrossReferenceSchemaPost,
)
from agr_literature_service.api.schemas.topic_entity_tag_schemas import (
    TopicEntityTagSchemaPost,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

# Mirrors the constants in load_pdb_associations.py. Hardcoded here so the
# migration remains valid if those constants are renamed later.
SOURCE_METHOD = "PDB association pipeline"
SOURCE_DATA_PROVIDER = "PDB"
SOURCE_EVIDENCE_ASSERTION = "ECO_0006156"
PROTEIN_STRUCTURE_ATP = "ATP:0000091"
DATA_NOVELTY_NOT_NEW = "ATP:0000335"

CURIE_PREFIX = "PDB"
PAGE_REFERENCE = "reference"

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _resolve_reference_curie(
    db: Session, reference_id: int, cache: Dict[int, str],
) -> Optional[str]:
    if reference_id in cache:
        return cache[reference_id]
    ref = db.query(ReferenceModel).filter_by(reference_id=reference_id).one_or_none()
    if ref is None:
        return None
    cache[reference_id] = ref.curie
    return ref.curie


def _migrate_one_tet(
    db: Session, tet, reference_curie: str, counts: Dict[str, int],
) -> None:
    """Convert a single entity-bearing TET to a cross_reference row.
    Updates `counts` in place. No-op for TETs with NULL entity."""
    if tet.entity is None:
        counts["skipped_no_entity"] += 1
        return
    curie = f"{CURIE_PREFIX}:{tet.entity.upper()}"
    existing = db.query(CrossReferenceModel).filter_by(
        curie=curie, reference_id=tet.reference_id,
    ).one_or_none()
    if existing is not None:
        counts["skipped_duplicate"] += 1
        return
    try:
        cross_reference_crud.create(
            db,
            CrossReferenceSchemaPost(
                curie=curie, reference_curie=reference_curie, pages=[PAGE_REFERENCE],
            ),
        )
        counts["migrated"] += 1
    except HTTPException as e:
        if e.status_code == 409:
            counts["skipped_duplicate"] += 1
        else:
            counts["errors"] += 1
            logger.warning(
                "cross_reference create failed for TET %s (%s): %s",
                tet.topic_entity_tag_id, curie, e.detail,
            )
    except Exception as e:
        counts["errors"] += 1
        logger.warning(
            "cross_reference create failed for TET %s (%s): %s",
            tet.topic_entity_tag_id, curie, e,
        )


def _create_topic_tets(
    db: Session, source_id: int, touched_reference_ids: Dict[int, str],
    counts: Dict[str, int],
) -> None:
    for reference_id, reference_curie in touched_reference_ids.items():
        try:
            result = create_tag(
                db,
                TopicEntityTagSchemaPost(
                    reference_curie=reference_curie,
                    topic=PROTEIN_STRUCTURE_ATP,
                    topic_entity_tag_source_id=source_id,
                    data_novelty=DATA_NOVELTY_NOT_NEW,
                    negated=False,
                ),
            )
        except Exception as e:
            counts["errors"] += 1
            logger.warning(
                "topic-only TET create failed for reference_id=%s: %s",
                reference_id, e,
            )
            continue
        if result.get("status") == "success":
            counts["topic_tet_created"] += 1
        else:
            counts["topic_tet_skipped_duplicate"] += 1


def migrate(db: Optional[Session] = None) -> Dict[str, int]:
    own_session = db is None
    if own_session:
        db = create_postgres_session(False)
    assert db is not None
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)
    counts = {
        "migrated": 0, "skipped_duplicate": 0, "skipped_no_entity": 0,
        "errors": 0, "tets_deleted": 0,
        "topic_tet_created": 0, "topic_tet_skipped_duplicate": 0,
    }
    try:
        source = db.query(TopicEntityTagSourceModel).filter_by(
            source_evidence_assertion=SOURCE_EVIDENCE_ASSERTION,
            source_method=SOURCE_METHOD,
            data_provider=SOURCE_DATA_PROVIDER,
        ).one_or_none()
        if source is None:
            logger.info("No PDB TET source row found; migration already complete.")
            return counts
        source_id = source.topic_entity_tag_source_id

        tets = db.query(TopicEntityTagModel).filter_by(
            topic_entity_tag_source_id=source_id,
        ).all()
        logger.info("Found %d PDB TET rows to migrate", len(tets))

        ref_curie_cache: Dict[int, str] = {}
        # Every reference_id that had at least one PDB TET. After migrating
        # the entity-bearing TETs to cross_refs, each of these gets a single
        # topic-only TET created (replacing the old per-PDB-ID TETs).
        touched_reference_ids: Dict[int, str] = {}

        for tet in tets:
            reference_curie = _resolve_reference_curie(db, tet.reference_id, ref_curie_cache)
            if reference_curie is None:
                counts["errors"] += 1
                logger.warning(
                    "Reference %s not found for TET %s",
                    tet.reference_id, tet.topic_entity_tag_id,
                )
                continue
            touched_reference_ids[tet.reference_id] = reference_curie
            _migrate_one_tet(db, tet, reference_curie, counts)

        if counts["errors"] > 0:
            logger.warning(
                "Encountered %d errors during migration; leaving TETs and "
                "source row in place for follow-up. Rerun after addressing "
                "the errors above.", counts["errors"],
            )
            return counts

        # Delete all existing PDB TETs for this source. The topic-only TETs
        # will be (re)created below.
        tet_ids = [t.topic_entity_tag_id for t in tets]
        if tet_ids:
            result = db.execute(
                delete(TopicEntityTagModel).where(
                    TopicEntityTagModel.topic_entity_tag_id.in_(tet_ids)
                )
            )
            db.commit()
            counts["tets_deleted"] = result.rowcount or 0  # type: ignore[attr-defined]

        _create_topic_tets(db, source_id, touched_reference_ids, counts)

        logger.info(
            "PDB TET migration done: migrated=%d skipped_duplicate=%d "
            "skipped_no_entity=%d errors=%d tets_deleted=%d "
            "topic_tet_created=%d topic_tet_skipped_duplicate=%d",
            counts["migrated"], counts["skipped_duplicate"],
            counts["skipped_no_entity"], counts["errors"],
            counts["tets_deleted"], counts["topic_tet_created"],
            counts["topic_tet_skipped_duplicate"],
        )
        return counts
    finally:
        if own_session:
            db.close()


if __name__ == "__main__":
    migrate()
