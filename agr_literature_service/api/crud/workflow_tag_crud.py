"""
workflow_tag_crud.py
===========================
"""
import cachetools.func
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_, text
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound
from datetime import datetime, timedelta
from typing import Union

from agr_literature_service.api.crud.reference_utils import get_reference
from agr_literature_service.api.models import WorkflowTagModel, WorkflowTransitionModel, ModModel, ReferenceModel
from agr_literature_service.api.schemas import WorkflowTagSchemaPost
from agr_literature_service.api.crud.topic_entity_tag_utils import get_descendants, \
    get_reference_id_from_curie_or_id  # get_ancestors,
from agr_literature_service.api.crud.topic_entity_tag_utils import get_map_ateam_curies_to_names
import logging
from agr_literature_service.api.crud.workflow_transition_requirements import *  # noqa
from agr_literature_service.api.crud.workflow_transition_requirements import (
    ADMISSIBLE_WORKFLOW_TRANSITION_REQUIREMENT_FUNCTIONS)
from agr_literature_service.api.crud.workflow_transition_actions.process_action import (process_action)
process_atp_multiple_allowed = [
    'ATP:ont1',  # used in testing
    'ATP:0000165', 'ATP:0000169', 'ATP:0000189', 'ATP:0000178', 'ATP:0000166'  # classifications and subtasks
]
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
        logger.error(f"Could not find {parent_or_children} for {atp_name}")
        return None
    return workflow_to_check[atp_name]


def get_workflow_process_from_tag(workflow_tag_atp_id: str):
    return get_parent_or_children(workflow_tag_atp_id, parent_or_children="parent")


def get_workflow_tags_from_process(workflow_process_atp_id: str):
    return get_parent_or_children(workflow_process_atp_id, parent_or_children="children")


def workflow_tag_add(current_workflow_tag_db_obj: WorkflowTagModel, new_tag: str = None):
    WorkflowTagModel(reference=current_workflow_tag_db_obj.reference,
                     mod=current_workflow_tag_db_obj.mod,
                     workflow_tag_id=new_tag)


def workflow_tag_remove(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, delete_tag: str = None):
    """
    Get an existing tag to remove (this is not the current_workflow_tag_db_obj we use this to get the mod and reference)
    and remove it.
    Example if we move from out of "file uploaded" then we want to remove "file conversion needed"
    So here we want to remove "file conversion needed" existing_tag = ATP:0000162
    """
    # there are some extra checks here that need to be done on paper type etc but
    # will fill in here when known.
    # lookup tag then delete
    db.query(WorkflowTagModel).\
        filter(WorkflowTagModel.reference_id == current_workflow_tag_db_obj.reference.reference_id,
               WorkflowTagModel.mod_id == current_workflow_tag_db_obj.mod_id,
               WorkflowTagModel.workflow_tag_id == delete_tag).delete()


def process_transition_actions(db: Session,
                               transition: WorkflowTransitionModel,
                               current_workflow_tag_db_obj: WorkflowTagModel):
    """
    :param db: Session: database session
    :param transition: WorkflowTransitionModel: workflow transition model to process
    :param current_workflow_tag_db_obj: WorkflowTagModel: current workflow tag model

    Get actions from transition.actions
         This is an array of strings that contain the method to be processed and args that
         are seperated by '::'
    Get ref_id and mod_id from current_workflow_tag_db_obj
    From the list of job_names to methods call the appropriate method with args.
    """
    for action in transition.actions:
        process_action(db, current_workflow_tag_db_obj, action)


def get_jobs(db: Session, job_str: str, limit: int = 1000, offset: int = 0):
    """
    :param db: Session: database session
    :param job_str: string can be just general "job" or job types like "extract_job"
                    We may have different jobs running on different systems so this
                    allows more flexibility.
    :param limit: maximum number of jobs to return. Maximum allowed value is 1000

    we need to join the workflow_transition table and workflow_tag table via transition_to and workflow_tag_id
    and condition contains the string defined in job_str.
    """
    if limit > 1000:
        limit = 1000
    if offset < 0:
        offset = 0
    jobs = []
    wft_list = (db.query(WorkflowTagModel, WorkflowTransitionModel)
                .filter(WorkflowTagModel.workflow_tag_id == WorkflowTransitionModel.transition_to,
                        WorkflowTagModel.mod_id == WorkflowTransitionModel.mod_id,
                        WorkflowTransitionModel.condition.contains(job_str))
                .order_by(WorkflowTagModel.date_updated.desc()).limit(limit).offset(offset).all())
    for wft in wft_list:
        conditions = wft[1].condition.split(',')
        for condition in conditions:
            if job_str in condition:
                new_job = {}
                new_job['job_name'] = condition
                new_job['workflow_tag_id'] = wft[0].workflow_tag_id
                new_job['reference_id'] = wft[0].reference_id
                new_job['reference_workflow_tag_id'] = wft[0].reference_workflow_tag_id
                # new_job['reference'] = wft[0].reference
                new_job['mod_id'] = wft[0].mod_id
                jobs.append(new_job)
    return jobs


def job_condition_on_start_process(db: Session, workflow_tag: WorkflowTagModel, orig_wft):
    """
           On a task we need to set the main one to in_progress too
           Code should check actions for the atp code it is and set
           the main one from needed to in progress
           Similarly for success of all subtasks or failure of any.
           1) ["ATP:ont1", "ATP:main_needed", ["proceed_on_value::category::thesis::ATP:task1_needed",
                                              "proceed_on_value::category::thesis::ATP:task2_needed"]], None],
           2)  ["ATP:main_needed", "ATP:main_in_progress", None, "on_start"],
    """
    transitions = db.query(WorkflowTransitionModel). \
        filter(WorkflowTransitionModel.actions != None,  # noqa
               WorkflowTransitionModel.mod_id == workflow_tag.mod_id).all()
    if not transitions:
        return
    else:
        first_transition = None
        for transition in transitions:
            for action in transition.actions:
                if orig_wft in action:
                    first_transition = transition
        if not first_transition:
            return
    # New Lookup of transition_to from 2). Presume only one of these
    # Once we know the hierarchy we can probably do this easier
    # by getting parent and then the condition 'on_start'
    # from = "ATP:main_needed", to = "ATP:main_in_progress", cond = "on_start"]
    second_transition = db.query(WorkflowTransitionModel). \
        filter(WorkflowTransitionModel.transition_from == first_transition.transition_to,
               WorkflowTransitionModel.condition.contains('on_start'),
               WorkflowTransitionModel.mod_id == workflow_tag.mod_id).one_or_none()
    if not second_transition:
        return
    # second_transition.workflow_tag_id = new_transition.transition_to
    main_workflow_tag = db.query(WorkflowTagModel). \
        filter(WorkflowTagModel.workflow_tag_id == second_transition.transition_from,
               WorkflowTagModel.reference_id == workflow_tag.reference_id,
               WorkflowTagModel.mod_id == workflow_tag.mod_id).one_or_none()
    if main_workflow_tag:
        # replace needed with in_progress
        main_workflow_tag.workflow_tag_id = second_transition.transition_to
        db.commit()


def job_change_atp_code(db: Session, reference_workflow_tag_id: int, condition: str = ""):
    """
    param db: Session:          database session
    param reference_workflow_tag_id: int  WorkflowTagModel.reference_workflow_tag_id
    param condition: str        workflow transition condition
                                "on_success", "on_failure" and "on_start" are the available options currently.

    Change the current workflow tag based on condition. If this is a sub task then alter the main
    one accordingly.

    If a condition is set:-
       call the appropriate function to do the extra bit.

    """
    # Get the workflow_tag
    try:
        workflow_tag = db.query(WorkflowTagModel).\
            filter(WorkflowTagModel.reference_workflow_tag_id == reference_workflow_tag_id).one()
        orig_wft = workflow_tag.workflow_tag_id
    except NoResultFound:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Bad reference_workflow_tag_id {reference_workflow_tag_id}")
    # Get what it is transitioning too
    try:
        new_transition = db.query(WorkflowTransitionModel).\
            filter(WorkflowTransitionModel.transition_from == workflow_tag.workflow_tag_id,
                   WorkflowTransitionModel.condition.contains(condition),
                   WorkflowTransitionModel.mod_id == workflow_tag.mod_id).one()
        # Set to new tag
        workflow_tag.workflow_tag_id = new_transition.transition_to
        # if we have any actions then do these.
        if new_transition.actions:
            process_transition_actions(db, new_transition, workflow_tag)
            db.commit()
        db.commit()
    except NoResultFound:
        error = f"""
            Could not find condition {condition} and
            transition_from {workflow_tag.workflow_tag_id},
            for mod {workflow_tag.mod_id}"""
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=error)

    # get main atp. There may not be one. If not just return
    if condition == "on_start":
        job_condition_on_start_process(db, workflow_tag, orig_wft)


def transition_sanity_check(db, transition_type, mod_abbreviation, curie_or_reference_id, new_workflow_tag_atp_id):
    logger.info("Transition sanity check")
    if transition_type not in ["manual", "automated"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Transition type must be manual or automated")
    reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id)
    mod: ModModel = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod abbreviation {mod_abbreviation} does not exist")

    # Get the parent/process and see if it allows multiple values
    process_atp_id = get_workflow_process_from_tag(workflow_tag_atp_id=new_workflow_tag_atp_id)
    if not process_atp_id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"process_atp_id {new_workflow_tag_atp_id} has NO process.")
    return mod, process_atp_id, reference


def check_requirements(reference, mod, transition):
    for requirement_function_str in transition.requirements:
        negated_function = False
        if requirement_function_str.startswith('not_'):
            requirement_function_str = requirement_function_str[4:]
            negated_function = True
        if requirement_function_str in ADMISSIBLE_WORKFLOW_TRANSITION_REQUIREMENT_FUNCTIONS:
            check_passed = ADMISSIBLE_WORKFLOW_TRANSITION_REQUIREMENT_FUNCTIONS[
                requirement_function_str](reference, mod)
            if negated_function:
                check_passed = not check_passed
            if not check_passed:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"{requirement_function_str} requirement not met")


def transition_to_workflow_status(db: Session, curie_or_reference_id: str, mod_abbreviation: str,
                                  new_workflow_tag_atp_id: str, transition_type: str = "automated"):
    mod, process_atp_id, reference = transition_sanity_check(db, transition_type, mod_abbreviation,
                                                             curie_or_reference_id, new_workflow_tag_atp_id)
    # current_workflow_tag_db_obj: Union[WorkflowTagModel, None] = None
    transition: Union[WorkflowTransitionModel, None] = None
    if process_atp_id in process_atp_multiple_allowed:
        current_workflow_tag_db_obj = WorkflowTagModel(reference=reference, mod=mod,
                                                       workflow_tag_id=new_workflow_tag_atp_id)
        transition = db.query(WorkflowTransitionModel).filter(
            and_(
                WorkflowTransitionModel.transition_to == new_workflow_tag_atp_id,
                WorkflowTransitionModel.mod_id == mod.mod_id,
                WorkflowTransitionModel.transition_type.in_(["any", f"{transition_type}_only"]))).one()
        if transition and transition.requirements:
            check_requirements(reference, mod, transition)
        if transition and transition.actions:
            process_transition_actions(db, transition, current_workflow_tag_db_obj)
            db.commit()
        return
    else:
        try:
            current_workflow_tag_db_obj = _get_current_workflow_tag_db_obj(db, str(reference.reference_id),
                                                                           process_atp_id, mod_abbreviation)
        except TypeError:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Could not find wft for {reference.reference_id} {process_atp_id} {mod_abbreviation}")
    if current_workflow_tag_db_obj:
        transition = db.query(WorkflowTransitionModel).filter(
            and_(
                WorkflowTransitionModel.mod_id == mod.mod_id,
                WorkflowTransitionModel.transition_from == current_workflow_tag_db_obj.workflow_tag_id,
                WorkflowTransitionModel.transition_to == new_workflow_tag_atp_id,
                WorkflowTransitionModel.transition_type.in_(["any", f"{transition_type}_only"])
            )
        ).first()
    if not transition and new_workflow_tag_atp_id != "ATP:0000141":
        message = f"Transition to {new_workflow_tag_atp_id} not allowed as not initial state."
        "Please set initial WFT first."
        if current_workflow_tag_db_obj and current_workflow_tag_db_obj.workflow_tag_id:
            message = f"Transition from {current_workflow_tag_db_obj.workflow_tag_id} to {new_workflow_tag_atp_id} "
            "NOT in the transition table and hence NOT allowed."
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=message)
    if not current_workflow_tag_db_obj or transition:
        if transition and transition.requirements:
            check_requirements(reference, mod, transition)
        if not current_workflow_tag_db_obj:
            current_workflow_tag_db_obj = WorkflowTagModel(reference=reference, mod=mod,
                                                           workflow_tag_id=new_workflow_tag_atp_id)
        else:
            current_workflow_tag_db_obj.workflow_tag_id = new_workflow_tag_atp_id
        db.commit()
        # So new tag has been set.
        # Now do the necessary actions if they are specified.
        if transition and transition.actions:
            process_transition_actions(db, transition, current_workflow_tag_db_obj)
            db.commit()
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="Workflow status transition not supported")


def _get_current_workflow_tag_db_obj(db: Session, curie_or_reference_id: str, workflow_process_atp_id: str,
                                     mod_abbreviation: str):
    reference_id = get_reference_id_from_curie_or_id(db=db, curie_or_reference_id=curie_or_reference_id)
    all_workflow_tags_for_process = get_workflow_tags_from_process(workflow_process_atp_id)
    if not all_workflow_tags_for_process:  # No process set at the moment
        return None
    mod_id = db.query(ModModel.mod_id).filter(ModModel.abbreviation == mod_abbreviation).first().mod_id
    return db.query(WorkflowTagModel).filter(
        and_(
            WorkflowTagModel.workflow_tag_id.in_(all_workflow_tags_for_process),
            WorkflowTagModel.reference_id == reference_id,
            WorkflowTagModel.mod_id == mod_id
        )
    ).one_or_none()


def get_current_workflow_status(db: Session, curie_or_reference_id: str, workflow_process_atp_id: str,
                                mod_abbreviation: str):
    current_workflow_tag_db_obj = _get_current_workflow_tag_db_obj(db, curie_or_reference_id,
                                                                   workflow_process_atp_id, mod_abbreviation)
    return None if not current_workflow_tag_db_obj else current_workflow_tag_db_obj.workflow_tag_id


def get_ref_ids_with_workflow_status(db: Session, workflow_atp_id: str, mod_abbreviation: str = None):
    query = db.query(WorkflowTagModel.reference_id).filter(WorkflowTagModel.workflow_tag_id == workflow_atp_id)
    if mod_abbreviation is not None:
        mod = db.query(ModModel.mod_id).filter(ModModel.abbreviation == mod_abbreviation).first()
        query = query.filter(WorkflowTagModel.mod_id == mod.mod_id)
    return [ref.reference_id for ref in query.all()]


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
    :param reference_workflow_tag_id:
    :return:
    """

    workflow_tag = db.query(WorkflowTagModel).\
        filter(WorkflowTagModel.reference_workflow_tag_id == reference_workflow_tag_id).first()
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
    workflow_tag_db_obj = db.query(WorkflowTagModel).\
        filter(WorkflowTagModel.reference_workflow_tag_id == reference_workflow_tag_id).first()
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
    :param reference_workflow_tag_id:
    :return:
    """

    workflow_tag: WorkflowTagModel = db.query(WorkflowTagModel).\
        filter(WorkflowTagModel.reference_workflow_tag_id == reference_workflow_tag_id).first()
    if not workflow_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"WorkflowTag with the workflow_tag_id {reference_workflow_tag_id} is not available")

    workflow_tag_data = jsonable_encoder(workflow_tag)

    if workflow_tag_data["reference_id"]:
        workflow_tag_data["reference_curie"] = db.query(ReferenceModel).\
            filter(ReferenceModel.reference_id == workflow_tag_data["reference_id"]).first().curie
    del workflow_tag_data["reference_id"]
    if workflow_tag_data["mod_id"]:
        workflow_tag_data["mod_abbreviation"] = db.query(ModModel).\
            filter(ModModel.mod_id == workflow_tag_data["mod_id"]).first().abbreviation
    else:
        workflow_tag_data["mod_abbreviation"] = ""
    del workflow_tag_data["mod_id"]

    return workflow_tag_data


def show_by_reference_mod_abbreviation(db: Session, reference_curie: str, mod_abbreviation: str) -> list:
    """

    :param db:
    :param reference_curie:
    :param mod_abbreviation:
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
    :param reference_workflow_tag_id:
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


def counters(db: Session, mod_abbreviation: str = None, workflow_process_atp_id: str = None):  # pragma: no cover

    all_WF_tags_for_process = None
    if workflow_process_atp_id:
        all_WF_tags_for_process = get_workflow_tags_from_process(workflow_process_atp_id)
        if all_WF_tags_for_process is None:
            message = f"WorkflowTag with the workflow_process_atp_id: {workflow_process_atp_id} is not available"
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=message)
        atp_curies = all_WF_tags_for_process
    else:
        rows = db.execute("SELECT distinct workflow_tag_id FROM workflow_tag").fetchall()
        atp_curies = [x[0] for x in rows]
    atp_curie_to_name = get_map_ateam_curies_to_names(curies_category="atpterm", curies=atp_curies)

    where_clauses = []
    params = {}
    if mod_abbreviation:
        where_clauses.append("m.abbreviation = :mod_abbreviation")
        params["mod_abbreviation"] = mod_abbreviation

    if all_WF_tags_for_process:
        where_clauses.append("wt.workflow_tag_id = ANY(:all_WF_tags_for_process)")
        params["all_WF_tags_for_process"] = all_WF_tags_for_process

    where = ""
    if where_clauses:
        where = "WHERE " + " AND ".join(where_clauses)

    query = f"""
    SELECT m.abbreviation, wt.workflow_tag_id, COUNT(*) AS tag_count
    FROM mod m
    JOIN workflow_tag wt ON m.mod_id = wt.mod_id
    {where}
    GROUP BY m.abbreviation, wt.workflow_tag_id
    ORDER BY m.abbreviation, wt.workflow_tag_id
    """

    try:
        rows = db.execute(query, params).fetchall()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    data = []
    for x in rows:
        data.append({
            "mod_abbreviation": x['abbreviation'],
            "workflow_tag_id": x['workflow_tag_id'],
            "workflow_tag_name": atp_curie_to_name[x['workflow_tag_id']],
            "tag_count": x['tag_count']
        })
    return data


def get_reference_workflow_tags_by_mod(
    db: Session,
    mod_abbreviation: str,
    workflow_tag_id: str,
    startDate: str = None,
    endDate: str = None
):  # pragma: no cover

    curie_prefix = "Xenbase" if mod_abbreviation == 'XB' else mod_abbreviation

    if not startDate:
        startDate = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    if not endDate:
        endDate = datetime.now().strftime('%Y-%m-%d')
    # startDate: "2024-08-19"
    # endDate:   "2024-08-26"
    query = text(
        "SELECT r.curie AS reference_curie, cr.curie AS cross_reference_curie, wft.date_updated "
        "FROM reference r "
        "JOIN cross_reference cr ON r.reference_id = cr.reference_id "
        "JOIN workflow_tag wft ON cr.reference_id = wft.reference_id "
        "JOIN mod m ON wft.mod_id = m.mod_id "
        "WHERE cr.curie_prefix = :curie_prefix "
        "AND m.abbreviation = :mod_abbreviation "
        "AND wft.workflow_tag_id = :workflow_tag_id "
        "AND wft.date_updated BETWEEN :startDate AND :endDate"
    )

    rows = db.execute(query, {
        'curie_prefix': curie_prefix,
        'mod_abbreviation': mod_abbreviation,
        'workflow_tag_id': workflow_tag_id,
        'startDate': startDate,
        'endDate': endDate
    }).fetchall()

    tags = [dict(row) for row in rows]
    return tags
