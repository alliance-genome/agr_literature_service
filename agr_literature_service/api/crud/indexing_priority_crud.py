import logging
from typing import Any, Dict, List, Optional
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.workflow_tag_crud import patch as wft_patch
from agr_literature_service.api.models import (
    IndexingPriorityModel,
    ModModel,
    ReferenceModel,
    WorkflowTagModel,
)
from agr_literature_service.api.schemas.indexing_priority_schemas import IndexingPrioritySchemaPost
from agr_literature_service.api.crud.ateam_db_helpers import get_name_to_atp_for_descendants
from agr_literature_service.api.crud.workflow_tag_crud import get_workflow_tags_from_process, \
    add_email_and_name
from agr_literature_service.api.crud.reference_utils import normalize_reference_curie
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


def get_ref_ids_with_indexing_priority(
    db: Session, predicted_indexing_priority: str, mod_abbreviation: Optional[str] = None
) -> List[int]:
    """
    Return reference_ids that have the given predicted_indexing_priority,
    optionally filtered by MOD.
    """
    q = db.query(IndexingPriorityModel.reference_id).filter(
        IndexingPriorityModel.predicted_indexing_priority == predicted_indexing_priority
    )
    if mod_abbreviation is not None:
        mod = db.query(ModModel.mod_id).filter(ModModel.abbreviation == mod_abbreviation).first()
        if not mod:
            return []
        q = q.filter(IndexingPriorityModel.mod_id == mod.mod_id)

    return [row.reference_id for row in q.all()]


def create(db: Session, indexing_priority_tag: IndexingPrioritySchemaPost) -> int:
    """
    Create a new indexing_priority entry and return its ID.
    """
    data: Dict[str, Any] = jsonable_encoder(indexing_priority_tag)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    reference_curie: str = data.pop("reference_curie")
    mod_abbreviation: str = data.pop("mod_abbreviation")

    reference = (
        db.query(ReferenceModel)
        .filter(ReferenceModel.curie == reference_curie)
        .first()
    )
    if not reference:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Reference with curie {reference_curie} does not exist",
        )

    mod = (
        db.query(ModModel)
        .filter(ModModel.abbreviation == mod_abbreviation)
        .first()
    )
    if not mod:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Mod with abbreviation {mod_abbreviation} does not exist",
        )

    existing = (
        db.query(IndexingPriorityModel)
        .filter(IndexingPriorityModel.reference_id == reference.reference_id)
        .filter(IndexingPriorityModel.mod_id == mod.mod_id)
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "IndexingPriority already exists for "
                f"reference_curie={reference_curie}, "
                f"mod_abbreviation={mod_abbreviation} "
                f"(id:{existing.indexing_priority_id}); cannot create duplicate record."
            ),
        )

    data["reference_id"] = reference.reference_id
    data["mod_id"] = mod.mod_id

    db_obj = IndexingPriorityModel(**data)
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)

    return int(db_obj.indexing_priority_id)


def destroy(db: Session, indexing_priority_id: int) -> None:
    tag = (
        db.query(IndexingPriorityModel)
        .filter(IndexingPriorityModel.indexing_priority_id == indexing_priority_id)
        .first()
    )
    if not tag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"IndexingPriorityTag with indexing_priority_id {indexing_priority_id} not found",
        )
    db.delete(tag)
    db.commit()


def patch(db: Session, indexing_priority_id: int, indexing_priority_update: Dict[str, Any]) -> None:
    """
    Partial update an indexing_priority record. `indexing_priority_update` is expected
    to be a dict from `model_dump(exclude_unset=True)` (router enforces Pydantic v2).
    """
    data: Dict[str, Any] = jsonable_encoder(indexing_priority_update)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    obj = (
        db.query(IndexingPriorityModel)
        .filter(IndexingPriorityModel.indexing_priority_id == indexing_priority_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"IndexingPriorityTag with indexing_priority_id {indexing_priority_id} not found",
        )

    for field, value in data.items():
        if field == "reference_curie":
            if value is not None:
                ref = db.query(ReferenceModel).filter(ReferenceModel.curie == value).first()
                if not ref:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=f"Reference with curie {value} does not exist",
                    )
                obj.reference_id = ref.reference_id

        elif field == "mod_abbreviation":
            if value is None:
                continue
            if isinstance(value, str) and value.strip() == "":
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="mod_abbreviation cannot be empty",
                )
            mod = db.query(ModModel).filter(ModModel.abbreviation == value).first()
            if not mod:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Mod with abbreviation {value} does not exist",
                )
            obj.mod_id = mod.mod_id

        else:
            setattr(obj, field, value)

    db.commit()


def show(db: Session, indexing_priority_id: int) -> Dict[str, Any]:
    tag: Optional[IndexingPriorityModel] = (
        db.query(IndexingPriorityModel)
        .filter(IndexingPriorityModel.indexing_priority_id == indexing_priority_id)
        .first()
    )
    if not tag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                "IndexingPriorityTag with the indexing_priority_id "
                f"{indexing_priority_id} is not available"
            ),
        )

    data: Dict[str, Any] = jsonable_encoder(tag)

    if data.get("reference_id"):
        ref = (
            db.query(ReferenceModel)
            .filter(ReferenceModel.reference_id == data["reference_id"])
            .first()
        )
        data["reference_curie"] = ref.curie if ref else None
    data.pop("reference_id", None)

    if data.get("mod_id"):
        mod = db.query(ModModel).filter(ModModel.mod_id == data["mod_id"]).first()
        data["mod_abbreviation"] = mod.abbreviation if mod else None
    else:
        data["mod_abbreviation"] = ""
    data.pop("mod_id", None)

    data = add_email_and_name(db, data)

    return data


def get_indexing_priority_tag(db: Session, curie: str):

    reference_curie = normalize_reference_curie(db, curie)

    process_atp_id = "ATP:0000210"
    priority_tags = get_workflow_tags_from_process(process_atp_id)
    _, atp_to_name = get_name_to_atp_for_descendants(process_atp_id)
    priority_tag_to_name = {atp: atp_to_name.get(atp, atp) for atp in priority_tags}

    sql = """
    SELECT
        ip.indexing_priority_id,
        ip.predicted_indexing_priority,
        ip.confidence_score,
        ip.curator_indexing_priority,
        ip.date_updated,
        r.curie AS reference_curie,
        m.abbreviation AS mod_abbreviation,
        COALESCE(e.email_address, ip.updated_by) AS updated_by_email,
        COALESCE(p.display_name, ip.updated_by) AS updated_by_name,
        ip.updated_by
    FROM indexing_priority ip
    JOIN reference r ON r.reference_id = ip.reference_id
    JOIN mod m ON m.mod_id = ip.mod_id
    LEFT JOIN users u  ON u.id = ip.updated_by
    LEFT JOIN person p ON p.person_id = u.person_id
    LEFT JOIN LATERAL (
        SELECT em.email_address
        FROM email em
        WHERE em.person_id = u.person_id
        AND em.date_invalidated IS NULL
        ORDER BY em.email_id ASC
        LIMIT 1
    ) e ON TRUE
    WHERE r.curie = :ref_curie
    """
    rows = db.execute(text(sql), {"ref_curie": reference_curie}).mappings().all()

    tags = []
    for row in rows:
        d = dict(row)
        predicted_code = d.get("predicted_indexing_priority")
        d["predicted_indexing_priority_name"] = priority_tag_to_name.get(predicted_code, predicted_code)
        curator_code = d.get("curator_indexing_priority")
        d["curator_indexing_priority_name"] = priority_tag_to_name.get(curator_code, curator_code)
        date_updated = d.get("date_updated")
        d["date_updated"] = date_updated.isoformat() if date_updated else None
        tags.append(d)

    return {
        "current_priority_tag": tags[0] if tags else {},
        "all_priority_tags": priority_tag_to_name,
    }


def set_priority(
    db: Session,
    reference_curie: str,
    mod_abbreviation: str,
    predicted_indexing_priority: Optional[str] = None,
    confidence_score: Optional[float] = None,
    curator_indexing_priority: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Set a predicted indexing priority (expects an ATP:... code), and update the
    pre-indexing workflow tag to success/failure accordingly.
    Returns the created record (incl. date_updated).
    """
    # Workflow tags for pre-indexing status
    pre_indexing_prioritization_to_atp = {
        "failed": "ATP:0000304",
        "success": "ATP:0000303",
    }

    # Ensure the reference has the pre-indexing workflow tag ATP:0000306 for this MOD
    reference_workflow_tag_id = (
        db.query(WorkflowTagModel.reference_workflow_tag_id)
        .join(ReferenceModel, WorkflowTagModel.reference_id == ReferenceModel.reference_id)
        .join(ModModel, WorkflowTagModel.mod_id == ModModel.mod_id)
        .filter(
            ModModel.abbreviation == mod_abbreviation,
            WorkflowTagModel.workflow_tag_id == "ATP:0000306",
            ReferenceModel.curie == reference_curie,
        )
        .scalar()
    )

    if reference_workflow_tag_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No workflow‐tag ATP:0000306 for paper {reference_curie} "
                f"in MOD {mod_abbreviation}"
            ),
        )

    # Build a Post schema so we benefit from its validation (ATP prefix + confidence rounding)
    payload = IndexingPrioritySchemaPost(
        predicted_indexing_priority=predicted_indexing_priority,
        curator_indexing_priority=curator_indexing_priority,
        mod_abbreviation=mod_abbreviation,
        reference_curie=reference_curie,
        confidence_score=confidence_score,
    )

    try:
        new_id = create(db, payload)
        # mark success on the workflow tag
        wft_patch(
            db,
            reference_workflow_tag_id,
            {"workflow_tag_id": pre_indexing_prioritization_to_atp["success"]},
        )
        # Return the created record (with date_updated, updated_by_email, etc.)
        return show(db, new_id)
    except Exception as e:
        # mark failure on the workflow tag
        wft_patch(
            db,
            reference_workflow_tag_id,
            {"workflow_tag_id": pre_indexing_prioritization_to_atp["failed"]},
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Setting indexing_priority failed: {e} "
                f"for paper {reference_curie} in MOD {mod_abbreviation}"
            ),
        )
