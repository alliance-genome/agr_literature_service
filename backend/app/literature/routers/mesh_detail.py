from typing import List

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User

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


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=MeshDetailSchemaUpdate,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: MeshDetailSchemaPost,
           user: Auth0User = Security(auth.get_user)):
    return mesh_detail_crud.create(request)


@router.delete('/{mesh_detail_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(mesh_detail_id: int,
            user: Auth0User = Security(auth.get_user)):
    mesh_detail_crud.destroy(mesh_detail_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{mesh_detail_id}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=MeshDetailSchemaUpdate,
            dependencies=[Depends(auth.implicit_scheme)])
def update(mesh_detail_id: int,
           request: MeshDetailSchemaUpdate,
           user: Auth0User = Security(auth.get_user)):
    return mesh_detail_crud.update(mesh_detail_id, request)


@router.get('/{mesh_detail_id}',
            response_model=MeshDetailSchemaUpdate,
            status_code=200)
def show(mesh_detail_id: int):
    return mesh_detail_crud.show(mesh_detail_id)


@router.get('/{mesh_detail_id}/versions',
            status_code=200)
def show(mesh_detail_id: int):
    return mesh_detail_crud.show_changesets(mesh_detail_id)
