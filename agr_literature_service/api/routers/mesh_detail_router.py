from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import mesh_detail_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (MeshDetailSchemaPost, MeshDetailSchemaShow,
                                                MeshDetailSchemaUpdate, ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/reference/mesh_detail",
    tags=['Reference']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=int)
def create(request: MeshDetailSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    return mesh_detail_crud.create(db, request)


@router.delete('/{mesh_detail_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(mesh_detail_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    mesh_detail_crud.destroy(db, mesh_detail_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{mesh_detail_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(mesh_detail_id: int,
                request: MeshDetailSchemaUpdate,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_from_okta(db, user)
    patch = request.dict(exclude_unset=True)
    return mesh_detail_crud.patch(db, mesh_detail_id, patch)


@router.get('/{mesh_detail_id}',
            response_model=MeshDetailSchemaShow,
            status_code=200)
def show(mesh_detail_id: int,
         db: Session = db_session):
    return mesh_detail_crud.show(db, mesh_detail_id)


@router.get('/{mesh_detail_id}/versions',
            status_code=200)
def show_versions(mesh_detail_id: int,
                  db: Session = db_session):
    return mesh_detail_crud.show_changesets(db, mesh_detail_id)
