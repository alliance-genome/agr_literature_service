"""
indexing_priority_crud.py
"""
import logging
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.workflow_tag_crud import\
    patch as wft_patch
from agr_literature_service.api.models import IndexingPriorityModel, \
    ModModel, ReferenceModel, WorkflowTagModel
from agr_literature_service.api.schemas import IndexingPrioritySchemaPost

logger = logging.getLogger(__name__)


def get_ref_ids_with_indexing_priority(db: Session, indexing_priority: str, mod_abbreviation: str = None):
    query = db.query(IndexingPriorityModel.reference_id).filter(IndexingPriorityModel.indexing_priority == indexing_priority)
    if mod_abbreviation is not None:
        mod = db.query(ModModel.mod_id).filter(ModModel.abbreviation == mod_abbreviation).first()
        query = query.filter(IndexingPriorityModel.mod_id == mod.mod_id)
    return [ref.reference_id for ref in query.all()]


def create(db: Session, indexing_priority_tag: IndexingPrioritySchemaPost) -> int:
    """
    Create a new indexing_priority_tag
    :param db:
    :param indexing_priority_tag:
    :return:
    """

    indexing_priority_tag_data = jsonable_encoder(indexing_priority_tag)
    reference_curie = indexing_priority_tag_data["reference_curie"]
    del indexing_priority_tag_data["reference_curie"]
    mod_abbreviation = indexing_priority_tag_data["mod_abbreviation"]
    del indexing_priority_tag_data["mod_abbreviation"]
    indexing_priority = indexing_priority_tag_data["indexing_priority"]

    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {reference_curie} does not exist")
    mod_id = None
    if mod_abbreviation:
        mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
        if not mod:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Mod with abbreviation {mod_abbreviation} does not exist")
        mod_id = mod.mod_id
    if not mod_id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Mod with abbreviation {mod_abbreviation} does not exist")
    indexing_priority_obj = db.query(IndexingPriorityModel).filter(
        IndexingPriorityModel.reference_id == reference.reference_id).filter(
        IndexingPriorityModel.mod_id == mod_id).filter(
        IndexingPriorityModel.indexing_priority == indexing_priority).first()
    if indexing_priority_obj:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"IndexingPriority with the reference_curie {reference_curie} "
                                   f"and mod_abbreviation {mod_abbreviation} and "
                                   f"{indexing_priority} already exist, "
                                   f"with id:{indexing_priority_obj.indexing_priority} can not "
                                   f"create duplicate record.")

    indexing_priority_tag_data["reference_id"] = reference.reference_id
    indexing_priority_tag_data["mod_id"] = mod_id
    db_obj = IndexingPriorityModel(**indexing_priority_tag_data)
    db.add(db_obj)
    db.commit()

    return int(db_obj.indexing_priority_id)


def destroy(db: Session, indexing_priority_id: int) -> None:
    """

    :param db:
    :param indexing_priority_id:
    :return:
    """

    priority_tag = db.query(IndexingPriorityModel).\
        filter(IndexingPriorityModel.indexing_priority_id == indexing_priority_id).first()
    if not priority_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"IndexingPriorityTag with indexing_priority_id {indexing_priority_id} not found")
    db.delete(priority_tag)
    db.commit()

    return None


def patch(db: Session, indexing_priority_id: int, indexing_priority_update):
    """
    Update an indexing_priority_tag
    :param db:
    :param indexing_priority_id
    :param indexing_priority_update:
    :return:
    """
    indexing_priority_tag_data = jsonable_encoder(indexing_priority_update)
    indexing_priority_obj = db.query(IndexingPriorityModel).\
        filter(IndexingPriorityModel.indexing_priority_id == indexing_priority_id).first()
    if not indexing_priority_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"IndexingPriorityTag with indexing_priority_id {indexing_priority_id} not found")

    for field, value in indexing_priority_tag_data.items():
        if field == "reference_curie":
            if value is not None:
                reference_curie = value
                new_reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
                if not new_reference:
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail=f"Reference with curie {reference_curie} does not exist")
                indexing_priority_obj.reference = new_reference
        elif field == "mod_abbreviation":
            if ((value is not None) and (len(value))) == 0:
                indexing_priority_obj.mod_id = None
            elif value is not None:
                mod_abbreviation = value
                new_mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
                if not new_mod:
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail=f"Mod with abbreviation {mod_abbreviation} does not exist")
                indexing_priority_obj.mod_id = new_mod.mod_id
        else:
            setattr(indexing_priority_obj, field, value)
    db.commit()
    return {"message": "updated"}


def show(db: Session, indexing_priority_id: int):
    """

    :param db:
    :param indexing_priority_id:
    :return:
    """

    indexing_priority_tag: IndexingPriorityModel = db.query(IndexingPriorityModel).\
        filter(IndexingPriorityModel.indexing_priority_id == indexing_priority_id).first()
    if not indexing_priority_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"IndexingPriorityTag with the indexing_priority_id {indexing_priority_id} is not available")

    indexing_priority_tag_data = jsonable_encoder(indexing_priority_tag)

    if indexing_priority_tag_data["reference_id"]:
        indexing_priority_tag_data["reference_curie"] = db.query(ReferenceModel).\
            filter(ReferenceModel.reference_id == indexing_priority_tag_data["reference_id"]).first().curie
    del indexing_priority_tag_data["reference_id"]
    if indexing_priority_tag_data["mod_id"]:
        indexing_priority_tag_data["mod_abbreviation"] = db.query(ModModel).\
            filter(ModModel.mod_id == indexing_priority_tag_data["mod_id"]).first().abbreviation
    else:
        indexing_priority_tag_data["mod_abbreviation"] = ""
    del indexing_priority_tag_data["mod_id"]
    ## add email address for updated_by
    sql_query_str = """
        SELECT email
        FROM users
        WHERE id = :okta_id
    """
    sql_query = text(sql_query_str)
    result = db.execute(sql_query, {'okta_id': indexing_priority_tag_data["updated_by"]})
    row = result.fetchone()
    indexing_priority_tag_data["updated_by_email"] = indexing_priority_tag_data["updated_by"] if row is None else row[0]
    if not indexing_priority_tag_data["updated_by_email"]:
        indexing_priority_tag_data["updated_by_email"] = indexing_priority_tag_data["updated_by"]

    return indexing_priority_tag_data


def set_priority(db: Session, reference_curie, mod_abbreviation, priority, confidence_score):

    priority_to_atp_mapping = {
        "priority_1": "ATP:0000211",
        "priority_2": "ATP:0000212",
        "priority_3": "ATP:0000213"
    }
    pre_indexing_prioritization_to_atp = {
        "failed": "ATP:0000304",
        "success": "ATP:0000303"
    }

    priority_atp = priority_to_atp_mapping.get(priority)
    reference_workflow_tag_id = (
        db.query(WorkflowTagModel.reference_workflow_tag_id).join(
            ReferenceModel,
            WorkflowTagModel.reference_id == ReferenceModel.reference_id
        ).join(
            ModModel,
            WorkflowTagModel.mod_id == ModModel.mod_id
        ).filter(
            ModModel.abbreviation == mod_abbreviation,
            WorkflowTagModel.workflow_tag_id == 'ATP:0000306',
            ReferenceModel.curie == reference_curie
        ).scalar()
    )

    if reference_workflow_tag_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No workflow‐tag ATP:0000306 for paper {reference_curie} in MOD {mod_abbreviation}"
        )

    if priority_atp is None:
        wft_patch(
            db,
            reference_workflow_tag_id,
            {"workflow_tag_id": pre_indexing_prioritization_to_atp.get("failed")}
        )
    else:
        data = IndexingPrioritySchemaPost(
            indexing_priority=priority_atp,
            mod_abbreviation=mod_abbreviation,
            reference_curie=reference_curie,
            confidence_score=confidence_score
        )
        try:
            create(db, data)
            wft_patch(
                db,
                reference_workflow_tag_id,
                {"workflow_tag_id": pre_indexing_prioritization_to_atp.get("success")}
            )
        except Exception as e:
            wft_patch(
                db,
                reference_workflow_tag_id,
                {"workflow_tag_id": pre_indexing_prioritization_to_atp.get("failed")}
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Setting priority failed {e} for paper {reference_curie} in MOD {mod_abbreviation}"
            )
