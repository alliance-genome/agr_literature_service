"""
workflow_tag_crud.py
===========================
"""
import cachetools.func
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

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
process_atp_multiple_allowed = ['ATP:ont1']
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
        print(action)
        process_action(db, current_workflow_tag_db_obj, action)


def get_jobs(db: Session, job_str: str):
    """
    :param db: Session: database session
    :param job_str: string can be just general "job" or job types like "extract_job"
                    We may have different jobs running on different systems so this
                    allows more flexibility.

    we need to join the workflow_transition table and workflow_tag table via transition_to and workflow_tag_id
    and condition contains the string defined in job_str.
    """
    jobs = []
    wft_list = db.query(WorkflowTagModel, WorkflowTransitionModel).\
        filter(WorkflowTagModel.workflow_tag_id == WorkflowTransitionModel.transition_to,
               WorkflowTransitionModel.condition.contains(job_str)).all()
    for wft in wft_list:
        # print(f"WFT: {wft}")
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


def job_change_atp_code(db: Session, reference_workflow_tag_id: int, condition: str):
    """
    param db: Session:          database session
    param reference_workflow_tag_id': int  WorkflowTagModel: reference_workflow_tag_id
    param condition: str        WorkflowTransitionModel: workflow transition condition
                     "on_success", "on_failure" and "on_start_job" are the available options currently.

    Lookup the workflow_tag via the reference_workflow_tag_id.
    Lookup the new workflow_tag via the transition workflow table
    with transition_from == workflow_tag_id
    AND condition contains the condition value.
    Set workflow tag_id to be the transition_to for this.
    """
    # Get the workflow_tag
    try:
        workflow_tag = db.query(WorkflowTagModel).\
            filter(WorkflowTagModel.reference_workflow_tag_id == reference_workflow_tag_id).one()
    except NoResultFound:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Bad reference_workflow_tag_id {reference_workflow_tag_id}")
    # Get what it is transitioning too
    try:
        new_transition = db.query(WorkflowTransitionModel).\
            filter(WorkflowTransitionModel.transition_from == workflow_tag.workflow_tag_id,
                   WorkflowTransitionModel.condition.contains(condition),
                   WorkflowTransitionModel.mod_id == workflow_tag.mod_id).one()
    except NoResultFound:
        error = f"""
            Could not find condition {condition} and
            transition_from {workflow_tag.workflow_tag_id},
            for mod {workflow_tag.mod_id}"""
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=error)
    # Set to new tag
    workflow_tag.workflow_tag_id = new_transition.transition_to


def transition_to_workflow_status(db: Session, curie_or_reference_id: str, mod_abbreviation: str,
                                  new_workflow_tag_atp_id: str, transition_type: str = "automated"):
    if transition_type not in ["manual", "automated"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Transition type must be manual or automated")
    reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id)
    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    # Get the parent/process and see if it allows multiple values
    print(f"pre get parent for {new_workflow_tag_atp_id}")
    process_atp_id = get_workflow_process_from_tag(workflow_tag_atp_id=new_workflow_tag_atp_id)
    print(f"parent id {process_atp_id }")
    if process_atp_id in process_atp_multiple_allowed:
        current_workflow_tag_db_obj = WorkflowTagModel(reference=reference, mod=mod,
                                                       workflow_tag_id=new_workflow_tag_atp_id)
        transition :WorkflowTransitionModel = db.query(WorkflowTransitionModel).filter(
            and_(
                WorkflowTransitionModel.transition_to == new_workflow_tag_atp_id,
                WorkflowTransitionModel.transition_type.in_(["any", f"{transition_type}_only"]))).one()
        print(transition)
        if transition and transition.actions:
            print(transition.actions)
            process_transition_actions(db, transition, current_workflow_tag_db_obj)
            db.commit()
        else:
            print(f"NO actions for {transition.workflow_transition_id}")
        return
    else:
        current_workflow_tag_db_obj: WorkflowTagModel = _get_current_workflow_tag_db_obj(db, str(reference.reference_id),
                                                                                         process_atp_id,
                                                                                         mod_abbreviation)
    transition = None
    if current_workflow_tag_db_obj:
        transition = db.query(WorkflowTransitionModel).filter(
            and_(
                WorkflowTransitionModel.transition_from == current_workflow_tag_db_obj.workflow_tag_id,
                WorkflowTransitionModel.transition_to == new_workflow_tag_atp_id,
                WorkflowTransitionModel.transition_type.in_(["any", f"{transition_type}_only"])
            )
        ).first()
    print("BOB: before transition")
    print(f"requirement_function_str: {transition}")
    if transition and transition.requirements:
        print(f"requirement_function_str: {transition.requirements}")
    else:
        print("NO requirements")
    print(f"ADMISSIBLE_WORKFLOW_TRANSITION_REQUIREMENT_FUNCTIONS: {ADMISSIBLE_WORKFLOW_TRANSITION_REQUIREMENT_FUNCTIONS}")
    print(f"{locals()}")
    if not current_workflow_tag_db_obj or transition:
        if transition and transition.requirements:
            transition_requirements_met = True
            for requirement_function_str in transition.requirements:
                negated_function = False
                if requirement_function_str.startswith('not_'):
                    requirement_function_str = requirement_function_str[4:]
                    negated_function = True
                if requirement_function_str in ADMISSIBLE_WORKFLOW_TRANSITION_REQUIREMENT_FUNCTIONS:
                    print(f"BOB: {locals()}")
                    check_passed = locals()[requirement_function_str](reference.reference_id, mod.mod_id)
                    if negated_function:
                        check_passed = not check_passed
                    if not check_passed:
                        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                            detail=f"{requirement_function_str} requirement not met")
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

    workflow_tag = db.query(WorkflowTagModel).\
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
    atp_curies = []
    if workflow_process_atp_id:
        all_WF_tags_for_process = get_workflow_tags_from_process(workflow_process_atp_id)
        if all_WF_tags_for_process is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"WorkflowTag with the workflow_process_atp_id: {workflow_process_atp_id} is not available")
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

