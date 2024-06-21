"""
workflow_tag_crud.py
===========================
"""
import cachetools.func
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_
from sqlalchemy.orm import Session

from agr_literature_service.api.models import WorkflowTagModel, ReferenceModel, ModModel, WorkflowTransitionModel
from agr_literature_service.api.schemas import WorkflowTagSchemaPost
from agr_literature_service.api.crud.topic_entity_tag_utils import get_descendants, \
    get_reference_id_from_curie_or_id  # get_ancestors,
import logging

logger = logging.getLogger(__name__)


@cachetools.func.ttl_cache(ttl=24 * 60 * 60)
def load_workflow_parent_children(root_node='ATP:0000177'):
    workflow_children = {}
    workflow_parent = {}
    nodes_to_process = [root_node]
    while nodes_to_process:
        parent = nodes_to_process.pop()
        children = get_descendants(parent)
        workflow_children[parent] = children
        for child in children:
            workflow_parent[child] = parent
            nodes_to_process.append(child)
    return workflow_children, workflow_parent


def get_parent_or_children(atp_name: str, parent_or_children: str = "parent"):
    workflow_children, workflow_parent = load_workflow_parent_children()
    workflow_to_check = workflow_children if parent_or_children == "children" else workflow_parent
    if atp_name not in workflow_to_check:
        logger.error("Could not find parent for {}".format(atp_name))
        return None
    return workflow_to_check[atp_name]


def get_workflow_process_from_tag(workflow_tag_atp_id: str):
    return get_parent_or_children(workflow_tag_atp_id, parent_or_children="parent")


def get_workflow_tags_from_process(workflow_process_atp_id: str):
    return get_parent_or_children(workflow_process_atp_id, parent_or_children="children")


def transition_to_workflow_status(db: Session, curie_or_reference_id: str, mod_abbreviation: str,
                                  new_workflow_tag_atp_id: str):
    reference_id = get_reference_id_from_curie_or_id(db=db, curie_or_reference_id=curie_or_reference_id)
    process_atp_id = get_workflow_process_from_tag(workflow_tag_atp_id=new_workflow_tag_atp_id)
    current_workflow_tag_db_obj: WorkflowTagModel = _get_current_workflow_tag_db_obj(db, str(reference_id),
                                                                                     process_atp_id,
                                                                                     mod_abbreviation)
    if not current_workflow_tag_db_obj or db.query(WorkflowTransitionModel).filter(
            and_(
                WorkflowTransitionModel.transition_from == current_workflow_tag_db_obj.workflow_tag_id,
                WorkflowTransitionModel.transition_to == new_workflow_tag_atp_id
            )
    ).first():
        current_workflow_tag_db_obj.workflow_tag_id = new_workflow_tag_atp_id
        db.commit()


def _get_current_workflow_tag_db_obj(db: Session, curie_or_reference_id: str, workflow_process_atp_id: str,
                                     mod_abbreviation: str):
    reference_id = get_reference_id_from_curie_or_id(db=db, curie_or_reference_id=curie_or_reference_id)
    all_workflow_tags_for_process = get_workflow_tags_from_process(workflow_process_atp_id)
    return db.query(WorkflowTagModel).join(ModModel).filter(
        and_(
            WorkflowTagModel.workflow_tag_id.in_(all_workflow_tags_for_process),
            WorkflowTagModel.reference_id == reference_id,
            ModModel.abbreviation == mod_abbreviation
        )
    ).one_or_none()


def get_current_workflow_status(db: Session, curie_or_reference_id: str, workflow_process_atp_id: str,
                                mod_abbreviation: str):
    current_workflow_tag_db_obj = _get_current_workflow_tag_db_obj(db, curie_or_reference_id,
                                                                   workflow_process_atp_id, mod_abbreviation)
    return None if not current_workflow_tag_db_obj else current_workflow_tag_db_obj.workflow_tag_id


def create(db: Session, workflow_tag: WorkflowTagSchemaPost) -> int:
    """
    Create a new workflow_tag
    :param db:
    :param workflow_tag:
    :return:
    """

    workflow_tag_data = jsonable_encoder(workflow_tag)
    reference_curie = workflow_tag_data["reference_curie"]
    del workflow_tag_data["reference_curie"]
    mod_abbreviation = workflow_tag_data["mod_abbreviation"]
    del workflow_tag_data["mod_abbreviation"]
    workflow_tag_id = workflow_tag_data["workflow_tag_id"]

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
    workflow_tag_db_obj = db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_id == reference.reference_id).filter(
        WorkflowTagModel.mod_id == mod_id).filter(
        WorkflowTagModel.workflow_tag_id == workflow_tag_id).first()
    if workflow_tag_db_obj:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"WorkflowTag with the reference_curie {reference_curie} "
                                   f"and mod_abbreviation {mod_abbreviation} and "
                                   f"{workflow_tag_id} already exist, "
                                   f"with id:{workflow_tag_db_obj.workflow_tag_id} can not "
                                   f"create duplicate record.")

    workflow_tag_data["reference_id"] = reference.reference_id
    workflow_tag_data["mod_id"] = mod_id
    db_obj = WorkflowTagModel(**workflow_tag_data)
    # db_obj.reference = reference
    # db_obj.mod = mod
    db.add(db_obj)
    db.commit()

    return db_obj.reference_workflow_tag_id


def destroy(db: Session, reference_workflow_tag_id: int) -> None:
    """

    :param db:
    :param workflow_tag_id:
    :return:
    """

    workflow_tag = db.query(WorkflowTagModel).filter(WorkflowTagModel.reference_workflow_tag_id == reference_workflow_tag_id).first()
    if not workflow_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"WorkflowTag with reference_workflow_tag_id {reference_workflow_tag_id} not found")
    db.delete(workflow_tag)
    db.commit()

    return None


def patch(db: Session, reference_workflow_tag_id: int, workflow_tag_update):
    """
    Update a workflow_tag
    :param db:
    :param reference_workflow_tag_id:
    :param workflow_tag_update:
    :return:
    """
    workflow_tag_data = jsonable_encoder(workflow_tag_update)
    workflow_tag_db_obj = db.query(WorkflowTagModel).filter(WorkflowTagModel.reference_workflow_tag_id == reference_workflow_tag_id).first()
    if not workflow_tag_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"WorkflowTag with workflow_tag_id {reference_workflow_tag_id} not found")

    for field, value in workflow_tag_data.items():
        if field == "reference_curie":
            if value is not None:
                reference_curie = value
                new_reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
                if not new_reference:
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail=f"Reference with curie {reference_curie} does not exist")
                workflow_tag_db_obj.reference = new_reference
        elif field == "mod_abbreviation":
            if ((value is not None) and (len(value))) == 0:
                workflow_tag_db_obj.mod_id = None
            elif value is not None:
                mod_abbreviation = value
                new_mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
                if not new_mod:
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail=f"Mod with abbreviation {mod_abbreviation} does not exist")
                workflow_tag_db_obj.mod_id = new_mod.mod_id
        else:
            setattr(workflow_tag_db_obj, field, value)
    db.commit()
    return {"message": "updated"}


def show(db: Session, reference_workflow_tag_id: int):
    """

    :param db:
    :param workflow_tag_id:
    :return:
    """

    workflow_tag = db.query(WorkflowTagModel).filter(WorkflowTagModel.reference_workflow_tag_id == reference_workflow_tag_id).first()
    if not workflow_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"WorkflowTag with the workflow_tag_id {reference_workflow_tag_id} is not available")

    workflow_tag_data = jsonable_encoder(workflow_tag)

    if workflow_tag_data["reference_id"]:
        workflow_tag_data["reference_curie"] = db.query(ReferenceModel).filter(ReferenceModel.reference_id == workflow_tag_data["reference_id"]).first().curie
    del workflow_tag_data["reference_id"]
    if workflow_tag_data["mod_id"]:
        workflow_tag_data["mod_abbreviation"] = db.query(ModModel).filter(ModModel.mod_id == workflow_tag_data["mod_id"]).first().abbreviation
    else:
        workflow_tag_data["mod_abbreviation"] = ""
    del workflow_tag_data["mod_id"]

    return workflow_tag_data


def show_by_reference_mod_abbreviation(db: Session, reference_curie: str, mod_abbreviation: str) -> list:
    """

    :param db:
    :param workflow_tag_id:
    :return: list of id's (int)
    """
    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod with the abbreviation {mod_abbreviation} is not available")
    elif not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the curie {reference_curie} is not available")
    else:
        workflow_tag_list = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == reference.reference_id).filter(
            WorkflowTagModel.mod_id == mod.mod_id).all()
        if not workflow_tag_list:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"WorkflowTag with the reference_curie {reference_curie} "
                                       f"and mod_abbreviation {mod_abbreviation} are not available")
        else:
            ont_list = []
            for ref_ont in workflow_tag_list:
                ont_list.append(ref_ont.workflow_tag_id)
            return ont_list


def show_changesets(db: Session, reference_workflow_tag_id: int):
    """

    :param db:
    :param workflow_tag_id:
    :return:
    """

    workflow_tag = db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_workflow_tag_id == reference_workflow_tag_id).first()
    if not workflow_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"WorkflowTag with the workflow_tag_id {reference_workflow_tag_id} is not available")

    history = []
    for version in workflow_tag.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
