from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import workflow_tag_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (WorkflowTagSchemaShow,
                                                WorkflowTagSchemaUpdate,
                                                WorkflowTagSchemaPost,
                                                ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/workflow_tag",
    tags=['Workflow Tag']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: WorkflowTagSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    return workflow_tag_crud.create(db, request)


@router.get('/reset_workflow_dict',
            status_code=status.HTTP_200_OK)
def reset_parent_children(user: OktaUser = db_user,
                          db: Session = db_session):
    set_global_user_from_okta(db, user)
    return workflow_tag_crud.load_workflow_parent_children()


@router.delete('/{reference_workflow_tag_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(reference_workflow_tag_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    workflow_tag_crud.destroy(db, reference_workflow_tag_id)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{reference_workflow_tag_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(reference_workflow_tag_id: int,
                request: WorkflowTagSchemaUpdate,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_from_okta(db, user)
    patch = request.dict(exclude_unset=True)
    return workflow_tag_crud.patch(db, reference_workflow_tag_id, patch)


@router.get('/{reference_workflow_tag_id}',
            response_model=WorkflowTagSchemaShow,
            status_code=200)
def show(reference_workflow_tag_id: int,
         db: Session = db_session):
    return workflow_tag_crud.show(db, reference_workflow_tag_id)


@router.get('/{reference_workflow_tag_id}/versions',
            status_code=200)
def show_versions(reference_workflow_tag_id: int,
                  db: Session = db_session):
    return workflow_tag_crud.show_changesets(db, reference_workflow_tag_id)
