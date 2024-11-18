from agr_literature_service.api.models import (
    WorkflowTagModel
)
from agr_literature_service.api.crud.workflow_tag_crud import (
    get_workflow_tags_from_process
)
from sqlalchemy.orm import Session
from fastapi import HTTPException, status

jobs_types = {'reference classification': {'main': 'ATP:0000165',
                                           'in_progress': 'ATP:0000178',
                                           'complete': 'ATP:0000169',
                                           'failed': 'ATP:0000189',
                                           'needed': 'ATP:0000166'},
              'entity extraction': {'main': 'ATP:00001672',
                                    'in_progress': 'ATP:0000190',
                                    'complete': 'ATP:0000174',
                                    'failed': 'ATP:0000187',
                                    'needed': 'ATP:0000173'}}


def check_type(checktype):
    if checktype not in jobs_types.keys():
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=f"Method sub_task_in_progress with first arg {checktype} not in known list")


def get_current_status_obj(db: Session, job_type: str, reference_id: int) -> WorkflowTagModel:
    global jobs_types
    cur = db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_id == reference_id,
        WorkflowTagModel.workflow_tag_id in (jobs_types[job_type].values())).first()
    return cur


def sub_task_in_progress(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, args: list):
    """
    args: [0] type of main flow i.e. 'reference classification' or 'entity extraction'

    """
    global jobs_types
    checktype = args[0]
    check_type(checktype)
    main_status_obj = get_current_status_obj(db, checktype, current_workflow_tag_db_obj.reference_id)
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


def sub_task_complete(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, args: list):
    checktype = args[0]
    check_type(checktype)
    main_status_obj = get_current_status_obj(db, checktype, current_workflow_tag_db_obj.reference_id)
    check_main_needed = False
    if main_status_obj.workflow_tag_id == jobs_types[check_type]['failed']:
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
    not_complete_list.append(get_workflow_tags_from_process(jobs_types[checktype]['in_progress']))
    cur = db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_id == current_workflow_tag_db_obj.reference_id,
        WorkflowTagModel.workflow_tag_id in not_complete_list).all()
    if not cur:
        main_status_obj.workflow_tag_id = jobs_types[checktype]['complete']


def sub_task_failed(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, args: list):
    checktype = args[0]
    check_type(checktype)
    main_status_obj = get_current_status_obj(db, checktype, current_workflow_tag_db_obj.reference_id)
    main_status_obj.workflow_tag_id = jobs_types[checktype]['failed']
