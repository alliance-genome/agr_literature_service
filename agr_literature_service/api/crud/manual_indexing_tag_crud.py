import logging
from typing import Any, Dict, List, Optional
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.models import (
    ManualIndexingTagModel,
    ModModel,
    ReferenceModel,
)
from agr_literature_service.api.schemas.manual_indexing_tag_schemas import ManualIndexingTagSchemaPost
from agr_literature_service.api.crud.ateam_db_helpers import get_name_to_atp_for_all_children
from agr_literature_service.api.crud.workflow_tag_crud import get_workflow_tags_from_process, \
    add_email_and_name
from agr_literature_service.api.crud.reference_utils import normalize_reference_curie
logger = logging.getLogger(__name__)


def get_ref_ids_with_curation_tag(
    db: Session, curation_tag: str, mod_abbreviation: Optional[str] = None
) -> List[int]:
    """
    Return reference_ids that have the given curation_tag, optionally filtered by MOD.
    """
    q = db.query(ManualIndexingTagModel.reference_id).filter(
        ManualIndexingTagModel.curation_tag == curation_tag
    )
    if mod_abbreviation is not None:
        mod = db.query(ModModel.mod_id).filter(ModModel.abbreviation == mod_abbreviation).first()
        if not mod:
            return []
        q = q.filter(ManualIndexingTagModel.mod_id == mod.mod_id)

    return [row.reference_id for row in q.all()]


def create(db: Session, manual_indexing_tag: ManualIndexingTagSchemaPost) -> int:
    """
    Create a new manual_indexing_tag entry and return its ID.
    """
    data: Dict[str, Any] = jsonable_encoder(manual_indexing_tag)

    reference_curie: str = data.pop("reference_curie")
    mod_abbreviation: str = data.pop("mod_abbreviation")
    curation_tag: str = data["curation_tag"]

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
        db.query(ManualIndexingTagModel)
        .filter(ManualIndexingTagModel.reference_id == reference.reference_id)
        .filter(ManualIndexingTagModel.mod_id == mod.mod_id)
        .filter(ManualIndexingTagModel.curation_tag == curation_tag)
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "ManualIndexingTag already exists for "
                f"reference_curie={reference_curie}, "
                f"mod_abbreviation={mod_abbreviation}, "
                f"curation_tag={curation_tag} "
                f"(id:{existing.manual_indexing_tag_id}); cannot create duplicate record."
            ),
        )

    data["reference_id"] = reference.reference_id
    data["mod_id"] = mod.mod_id

    db_obj = ManualIndexingTagModel(**data)
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)

    return int(db_obj.manual_indexing_tag_id)


def destroy(db: Session, manual_indexing_tag_id: int) -> None:
    tag = (
        db.query(ManualIndexingTagModel)
        .filter(ManualIndexingTagModel.manual_indexing_tag_id == manual_indexing_tag_id)
        .first()
    )
    if not tag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ManualIndexingTagTag with manual_indexing_tag_id {manual_indexing_tag_id} not found",
        )
    db.delete(tag)
    db.commit()


def patch(db: Session, manual_indexing_tag_id: int, manual_indexing_tag_update: Dict[str, Any]) -> None:

    data: Dict[str, Any] = jsonable_encoder(manual_indexing_tag_update)

    obj = (
        db.query(ManualIndexingTagModel)
        .filter(ManualIndexingTagModel.manual_indexing_tag_id == manual_indexing_tag_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ManualIndexingTagTag with manual_indexing_tag_id {manual_indexing_tag_id} not found",
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


def show(db: Session, manual_indexing_tag_id: int) -> Dict[str, Any]:
    tag: Optional[ManualIndexingTagModel] = (
        db.query(ManualIndexingTagModel)
        .filter(ManualIndexingTagModel.manual_indexing_tag_id == manual_indexing_tag_id)
        .first()
    )
    if not tag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                "ManualIndexingTagTag with the manual_indexing_tag_id "
                f"{manual_indexing_tag_id} is not available"
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


def get_manual_indexing_tag(db: Session, curie: str, mod_abbreviation: str):

    if mod_abbreviation not in ["FB", "WB", "ZFIN"]:
        return {}

    reference_curie = normalize_reference_curie(db, curie)
    curation_tag_to_name = {}
    _, atp_to_name = get_name_to_atp_for_all_children("ATP:0000197")
    for process_atp_id in ["ATP:0000227", "ATP:0000208"]:
        curation_tags = get_workflow_tags_from_process(process_atp_id)
        # _, atp_to_name = get_name_to_atp_for_all_children(process_atp_id)

        # include children
        curation_tag_to_name.update({
            atp: atp_to_name.get(atp, atp) for atp in curation_tags
        })

        # also include the process_atp_id itself
        curation_tag_to_name[process_atp_id] = atp_to_name.get(process_atp_id, process_atp_id)

    sql = """
    SELECT
        mit.manual_indexing_tag_id,
        mit.curation_tag,
        mit.confidence_score,
        mit.validation_by_biocurator,
        mit.date_updated,
        r.curie AS reference_curie,
        m.abbreviation AS mod_abbreviation,
        COALESCE(e.email_address, mit.updated_by) AS updated_by_email,
        COALESCE(p.display_name, mit.updated_by) AS updated_by_name,
        mit.updated_by
    FROM manual_indexing_tag mit
    JOIN reference r ON r.reference_id = mit.reference_id
    JOIN mod m ON m.mod_id = mit.mod_id
    LEFT JOIN users u  ON u.id = mit.updated_by
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
        if d["mod_abbreviation"] != mod_abbreviation:
            continue
        code = d.get("curation_tag")
        d["curation_tag_name"] = curation_tag_to_name.get(code, code)
        d["date_updated"] = d["date_updated"].isoformat()
        tags.append(d)
    return {
        "current_curation_tag": tags[0] if tags else {},
        "all_curation_tags": curation_tag_to_name,
    }


def set_manual_indexing_tag(
    db: Session,
    reference_curie: str,
    mod_abbreviation: str,
    curation_tag: str,
    confidence_score: float,
) -> Dict[str, Any]:

    payload = ManualIndexingTagSchemaPost(
        curation_tag=curation_tag,
        mod_abbreviation=mod_abbreviation,
        reference_curie=reference_curie,
        confidence_score=confidence_score,
    )

    try:
        new_id = create(db, payload)
        # Return the created record (with date_updated, updated_by_email, etc.)
        return show(db, new_id)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Setting manual_indexing_tag failed: {e} "
                f"for paper {reference_curie} in MOD {mod_abbreviation}"
            ),
        )
