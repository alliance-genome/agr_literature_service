from agr_literature_service.api.models import (
    WorkflowTagModel
)

from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from typing import Dict

# List jobs that are available with their immediate branches.
jobs_types: Dict[str, Dict[str, str] | Dict[str, str]] = {
    'reference classification': {
        'main': 'ATP:0000165',
        'in_progress': 'ATP:0000178',
        'complete': 'ATP:0000169',
        'failed': 'ATP:0000189',
        'needed': 'ATP:0000166'},
    'curation classification': {
        'main': 'ATP:0000311',
        'in_progress': 'ATP:0000314',
        'complete': 'ATP:0000312',
        'failed': 'ATP:0000315',
        'needed': 'ATP:0000313'},
    'entity extraction': {
        'main': 'ATP:00001672',
        'in_progress': 'ATP:0000190',
        'complete': 'ATP:0000174',
        'failed': 'ATP:0000187',
        'needed': 'ATP:0000173'},
    'email extraction': {
        'main': 'ATP:0000354',
        'in_progress': 'ATP:0000357',
        'complete': 'ATP:0000355',
        'failed': 'ATP:0000356',
        'needed': 'ATP:0000358'
    }
}


def check_type(checktype: str):
    """ Check the type is valid, i.e. has a key in the jobs_types dict """
    if checktype not in jobs_types.keys():
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=f"Method sub_task_in_progress with first arg {checktype} not in known list")


def get_current_status_obj(db: Session, job_type: str, reference_id, mod_id):
    """ Get the current status of the "main" job.
        So for this job type look for the overall status.
        """
    global jobs_types
    cur = db.query(WorkflowTagModel).\
        filter(WorkflowTagModel.reference_id == reference_id,
               WorkflowTagModel.mod_id == mod_id,
               WorkflowTagModel.workflow_tag_id.in_((jobs_types[job_type].values()))).one_or_none()
    return cur


def sub_task_in_progress(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, args: list):
    """
    This is called if the workflow transition actions has 'sub_task_in_progress::XXXXXX' specified.
    i.e. if it has 'sub_task_in_progress::reference classification'.

    args: [0] type of main flow i.e. 'reference classification' or 'entity extraction'

    """
    global jobs_types

    checktype = args[0]
    check_type(checktype)
    main_status_obj = get_current_status_obj(db, checktype, int(current_workflow_tag_db_obj.reference_id), int(current_workflow_tag_db_obj.mod_id))
    if not main_status_obj:
        mess = f"Error: main in progress. Could not find main_status_obj for {checktype} in DB"
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=mess)
    if main_status_obj.workflow_tag_id == jobs_types[checktype]['in_progress']:
        return  # already set
    elif main_status_obj.workflow_tag_id == jobs_types[checktype]['failed']:
        return  # already set
    elif main_status_obj.workflow_tag_id == jobs_types[checktype]['needed']:
        main_status_obj.workflow_tag_id = jobs_types[checktype]['in_progress']
    elif main_status_obj.workflow_tag_id == jobs_types[checktype]['complete']:
        mess = "Error: main in complete BUT we are setting a sub task to in progress?"
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=mess)
    else:
        mess = f"Possible Error: main flow status = {main_status_obj.workflow_tag_id} Unknown"
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=mess)
    db.commit()


def sub_task_retry(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, args: list):
    """
    This is called if the workflow transition actions has 'sub_task_retry::XXXXXX' specified.
    i.e. if it has 'sub_task_retry::reference classification'.

    args: [0] type of main flow i.e. 'reference classification' or 'entity extraction'

    """
    from agr_literature_service.api.crud.workflow_tag_crud import (
        get_workflow_tags_from_process
    )
    global jobs_types

    checktype = args[0]
    check_type(checktype)
    main_status_obj = get_current_status_obj(db, checktype, int(current_workflow_tag_db_obj.reference_id), int(current_workflow_tag_db_obj.mod_id))
    if not main_status_obj:
        mess = f"Error: main in progress. Could not find main_status_obj for {checktype} in DB"
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=mess)
    failed_list = get_workflow_tags_from_process(jobs_types[checktype]['failed'])

    any_failed = db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_id == current_workflow_tag_db_obj.reference_id,
        WorkflowTagModel.mod_id == current_workflow_tag_db_obj.mod_id,
        WorkflowTagModel.workflow_tag_id.in_(failed_list)).all()
    if any_failed:
        main_status_obj.workflow_tag_id = jobs_types[checktype]['failed']
    else:
        main_status_obj.workflow_tag_id = jobs_types[checktype]['in_progress']
    db.commit()


def sub_task_complete(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, args: list):
    """
    This is called if the workflow transition actions has 'sub_task_complete::XXXXXX' specified.
    i.e. if it has 'sub_task_in_progress::reference classification'.

    args: [0] type of main flow i.e. 'reference classification' or 'entity extraction'

    """
    # import here else we get a circular import if at the top.
    from agr_literature_service.api.crud.workflow_tag_crud import (
        get_workflow_tags_from_process
    )
    global jobs_types

    checktype = args[0]
    check_type(checktype)
    main_status_obj = get_current_status_obj(db, checktype, current_workflow_tag_db_obj.reference_id, int(current_workflow_tag_db_obj.mod_id))
    check_main_needed = False
    if not main_status_obj:
        mess = f"Error: main in complete. Could not find main_status_obj for {checktype} in DB"
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=mess)

    if main_status_obj.workflow_tag_id == str(jobs_types[checktype]['failed']):
        return  # already set
    elif main_status_obj.workflow_tag_id == jobs_types[checktype]['needed']:
        check_main_needed = True
        main_status_obj.workflow_tag_id = jobs_types[checktype]['in_progress']
    elif main_status_obj.workflow_tag_id == jobs_types[checktype]['in_progress']:
        check_main_needed = True
    elif main_status_obj.workflow_tag_id == jobs_types[checktype]['complete']:
        mess = "Error: main in complete BUT we are setting a sub task to complete?"
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=mess)
    # So we have a completed subtask
    # check if we have any other subtasks are needed or in_progress
    # if not then set main to complete.
    if not check_main_needed:
        return
    not_complete_list = get_workflow_tags_from_process(jobs_types[checktype]['needed'])
    not_complete_list.extend(get_workflow_tags_from_process(jobs_types[checktype]['in_progress']))
    not_complete_list.extend(get_workflow_tags_from_process(jobs_types[checktype]['failed']))

    if not not_complete_list:
        cur = None
    elif len(not_complete_list) == 1:
        cur = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == current_workflow_tag_db_obj.reference_id,
            WorkflowTagModel.mod_id == current_workflow_tag_db_obj.mod_id,
            WorkflowTagModel.workflow_tag_id == not_complete_list[0]).all()
    else:
        cur = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == current_workflow_tag_db_obj.reference_id,
            WorkflowTagModel.mod_id == current_workflow_tag_db_obj.mod_id,
            WorkflowTagModel.workflow_tag_id.in_(not_complete_list)).all()
    # current job successful also we have no other subtasks that are failed, needed
    # or in_progress, so we can set the main one now to complete too.
    # If there are still subtasks that are not complete then we do not change
    # the main task's status.
    if not cur:
        main_status_obj.workflow_tag_id = jobs_types[checktype]['complete']
    db.commit()


def sub_task_failed(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, args: list):
    """
    This is called if the workflow transition actions has 'sub_task_failed::XXXXXX' specified.
    i.e. if it has 'sub_task_failed::reference classification'.

    args: [0] type of main flow i.e. 'reference classification' or 'entity extraction'

    """
    global jobs_types

    checktype = args[0]
    check_type(checktype)
    main_status_obj = get_current_status_obj(db, checktype, current_workflow_tag_db_obj.reference_id, int(current_workflow_tag_db_obj.mod_id))
    if not main_status_obj:
        mess = f"Error: main in failed. Could not find main_status_obj for {checktype} in DB"
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=mess)

    main_status_obj.workflow_tag_id = jobs_types[checktype]['failed']
    db.commit()
