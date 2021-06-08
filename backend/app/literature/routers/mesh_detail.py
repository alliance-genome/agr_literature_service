from typing import List

from sqlalchemy.orm import Session

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User

from literature import database

from literature.user import set_global_user_id

from literature.schemas import MeshDetailSchemaShow
from literature.schemas import MeshDetailSchemaPost
from literature.schemas import MeshDetailSchemaCreate
from literature.schemas import MeshDetailSchemaUpdate

from literature.crud import mesh_detail_crud
from literature.routers.authentication import auth


router = APIRouter(
    prefix="/reference/mesh_detail",
    tags=['Reference']
)


get_db = database.get_db


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=MeshDetailSchemaUpdate,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: MeshDetailSchemaPost,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return mesh_detail_crud.create(db, request)


@router.delete('/{mesh_detail_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(mesh_detail_id: int,
            user: Auth0User = Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    mesh_detail_crud.destroy(db, mesh_detail_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{mesh_detail_id}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=MeshDetailSchemaUpdate,
            dependencies=[Depends(auth.implicit_scheme)])
def update(mesh_detail_id: int,
           request: MeshDetailSchemaUpdate,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return mesh_detail_crud.update(db, mesh_detail_id, request)


@router.get('/{mesh_detail_id}',
            response_model=MeshDetailSchemaUpdate,
            status_code=200)
def show(mesh_detail_id: int,
         db: Session = Depends(get_db)):
    return mesh_detail_crud.show(db, mesh_detail_id)


@router.get('/{mesh_detail_id}/versions',
            status_code=200)
def show(mesh_detail_id: int,
         db: Session = Depends(get_db)):
    return mesh_detail_crud.show_changesets(db, mesh_detail_id)
