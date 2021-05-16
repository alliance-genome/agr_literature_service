from typing import List

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User

from literature.schemas import FileSchemaShow
from literature.schemas import FileSchemaUpdate
from literature.schemas import FileSchemaCreate

from literature.crud import s3file
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/file",
    tags=['File']
)


@router.post('/{reference_id}',
             status_code=status.HTTP_201_CREATED,
             response_model=FileSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: FileSchemaCreate,
           user: Auth0User = Security(auth.get_user)):
    return s3file.create(request)


@router.delete('/{file_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(file_id: int,
            user: Auth0User = Security(auth.get_user)):
    s3file.destroy(file_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{file_id}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=FileSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)])
def update(file_id: int,
           request: FileSchemaUpdate,
           user: Auth0User = Security(auth.get_user)):
    return s3file.update(file_id, request)


@router.get('/{file_id}',
            status_code=200)
def show(file_id: int):
    return s3file.show(file_id)


@router.get('/{file_id}/versions',
            status_code=200)
def show(file_id: int):
    return s3file.show_changesets(file_id)
