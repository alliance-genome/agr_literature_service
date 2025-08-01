from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session
from typing import Optional

from agr_literature_service.api import database
from agr_literature_service.api.crud import workflow_tag_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (
    WorkflowTagSchemaShow,
    WorkflowTagSchemaUpdate,
    WorkflowTagSchemaPost,
)
from agr_literature_service.api.schemas.workflow_tag_schemas import WorkflowTransitionSchemaPost
from agr_literature_service.api.user import set_global_user_from_okta
from agr_literature_service.api.crud.ateam_db_helpers import atp_get_name

router = APIRouter(
    prefix="/workflow_tag",
    tags=["Workflow Tag"],
)

get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=int,
)
def create(
    request: WorkflowTagSchemaPost,
    user: OktaUser = db_user,
    db: Session = db_session,
) -> int:
    set_global_user_from_okta(db, user)
    new_id = workflow_tag_crud.create(db, request)
    return new_id


@router.delete(
    "/{reference_workflow_tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def destroy(
    reference_workflow_tag_id: int,
    user: OktaUser = db_user,
    db: Session = db_session,
):
    set_global_user_from_okta(db, user)
    workflow_tag_crud.destroy(db, reference_workflow_tag_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{reference_workflow_tag_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=int,
)
async def patch(
    reference_workflow_tag_id: int,
    request: WorkflowTagSchemaUpdate,
    user: OktaUser = db_user,
    db: Session = db_session,
) -> int:
    set_global_user_from_okta(db, user)
    updates = request.dict(exclude_unset=True)
    # perform the update (this should return the same ID)
    workflow_tag_crud.patch(db, reference_workflow_tag_id, updates)
    # return the integer id so FastAPI can validate it
    return reference_workflow_tag_id


@router.get(
    "/{reference_workflow_tag_id}",
    response_model=WorkflowTagSchemaShow,
    status_code=status.HTTP_200_OK,
)
def show(
    reference_workflow_tag_id: int,
    db: Session = db_session,
) -> WorkflowTagSchemaShow:
    return workflow_tag_crud.show(db, reference_workflow_tag_id)


@router.get(
    "/jobs/{job_string}",
    status_code=status.HTTP_200_OK,
)
def get_jobs(
    job_string: str,
    limit: int = 1000,
    offset: int = 0,
    db: Session = db_session,
    mod_abbreviation: str = None,
    reference: str = None,
    topic: str = None,
):
    return workflow_tag_crud.get_jobs(
        db, job_string, limit, offset,
        mod_abbr=mod_abbreviation,
        reference=reference,
        topic=topic,
    )


@router.post(
    "/job/failed/{reference_workflow_tag_id}",
    status_code=status.HTTP_200_OK,
)
def failed_job(reference_workflow_tag_id: int, db: Session = db_session):
    return workflow_tag_crud.job_change_atp_code(db, reference_workflow_tag_id, 'on_failed')


@router.post(
    "/job/retry/{reference_workflow_tag_id}",
    status_code=status.HTTP_200_OK,
)
def retry_job(reference_workflow_tag_id: int, db: Session = db_session):
    return workflow_tag_crud.job_change_atp_code(db, reference_workflow_tag_id, 'on_retry')


@router.post(
    "/job/success/{reference_workflow_tag_id}",
    status_code=status.HTTP_200_OK,
)
def successful_job(reference_workflow_tag_id: int, db: Session = db_session):
    return workflow_tag_crud.job_change_atp_code(db, reference_workflow_tag_id, 'on_success')


@router.post(
    "/job/started/{reference_workflow_tag_id}",
    status_code=status.HTTP_200_OK,
)
def start_job(reference_workflow_tag_id: int, db: Session = db_session):
    return workflow_tag_crud.job_change_atp_code(db, reference_workflow_tag_id, 'on_start')


@router.get(
    "/{reference_workflow_tag_id}/versions",
    status_code=status.HTTP_200_OK,
)
def show_versions(
    reference_workflow_tag_id: int,
    db: Session = db_session,
):
    return workflow_tag_crud.show_changesets(db, reference_workflow_tag_id)


@router.post(
    "/transition_to_workflow_status",
    status_code=status.HTTP_200_OK,
)
def transition_to_workflow_status(
    request: WorkflowTransitionSchemaPost,
    user: OktaUser = db_user,
    db: Session = db_session,
):
    set_global_user_from_okta(db, user)
    return workflow_tag_crud.transition_to_workflow_status(
        db=db,
        curie_or_reference_id=request.curie_or_reference_id,
        mod_abbreviation=request.mod_abbreviation,
        new_workflow_tag_atp_id=request.new_workflow_tag_atp_id,
        transition_type=request.transition_type,
    )


@router.get(
    "/get_current_workflow_status/{curie_or_reference_id}/{mod_abbreviation}/{workflow_process_atp_id}",
    status_code=status.HTTP_200_OK,
)
def get_current_workflow_status(
    curie_or_reference_id: str,
    mod_abbreviation: str,
    workflow_process_atp_id: str,
    user: OktaUser = db_user,
    db: Session = db_session,
):
    set_global_user_from_okta(db, user)
    return workflow_tag_crud.get_current_workflow_status(
        db=db,
        curie_or_reference_id=curie_or_reference_id,
        mod_abbreviation=mod_abbreviation,
        workflow_process_atp_id=workflow_process_atp_id,
    )


@router.get(
    "/counters/",
    status_code=status.HTTP_200_OK,
)
def counters(
    mod_abbreviation: str = None,
    workflow_process_atp_id: str = None,
    date_option: str = None,
    date_range_start: str = None,
    date_range_end: str = None,
    date_frequency: str = None,
    db: Session = db_session,
):
    return workflow_tag_crud.counters(
        db=db,
        mod_abbreviation=mod_abbreviation,
        workflow_process_atp_id=workflow_process_atp_id,
        date_option=date_option,
        date_range_start=date_range_start,
        date_range_end=date_range_end,
        date_frequency=date_frequency,
    )


@router.get(
    "/by_mod/{mod_abbreviation}",
    status_code=status.HTTP_200_OK,
)
def get_reference_workflow_tags(
    mod_abbreviation: str,
    workflow_tag_id: str,
    startDate: str = None,
    endDate: str = None,
    db: Session = db_session,
):
    return workflow_tag_crud.get_reference_workflow_tags_by_mod(
        db, workflow_tag_id, mod_abbreviation, startDate, endDate
    )


@router.get(
    "/reports/{workflow_tag_id}/{mod_abbreviation}",
    status_code=status.HTTP_200_OK,
)
def get_report_workflow_tags(
    mod_abbreviation: str,
    workflow_tag_id: str,
    db: Session = db_session,
):
    return workflow_tag_crud.report_workflow_tags(db, workflow_tag_id, mod_abbreviation)


@router.get(
    "/workflow_diagram/{mod}",
    status_code=status.HTTP_200_OK,
)
def get_report_workflow_diagram(mod: str, db: Session = db_session):
    return workflow_tag_crud.get_workflow_tag_diagram(mod, db)


@router.get(
    "/get_name/{workflow_tag_id}",
    status_code=status.HTTP_200_OK,
)
def get_name(workflow_tag_id: str):
    return atp_get_name(workflow_tag_id)


@router.get(
    "/subsets/{workflow_name}/{mod_abbreviation}",
    status_code=status.HTTP_200_OK,
)
def get_workflow_tags_subset(
    mod_abbreviation: str,
    workflow_name: str,
    db: Session = db_session,
):
    return workflow_tag_crud.workflow_subset_list(workflow_name, mod_abbreviation, db)


@router.post(
    "/set_priority/{reference_curie}/{mod_abbreviation}/{priority}",
    status_code=status.HTTP_200_OK,
)
def set_priority(
    reference_curie: str,
    mod_abbreviation: str,
    priority: str,
    db: Session = db_session,
):
    return workflow_tag_crud.set_priority(db, reference_curie, mod_abbreviation, priority)


@router.get(
    "/indexing-community/{reference_curie}",
    status_code=status.HTTP_200_OK,
)
@router.get(
    "/indexing-community/{reference_curie}/{mod_abbreviation}",
    status_code=status.HTTP_200_OK,
)
def get_indexing_and_community_workflow_tags(
    reference_curie: str,
    mod_abbreviation: Optional[str] = None,
    db: Session = db_session,
):
    return workflow_tag_crud.get_indexing_and_community_workflow_tags(
        db, reference_curie, mod_abbreviation
    )
