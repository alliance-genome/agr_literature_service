from typing import List

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User

from literature.user import set_global_user_id
from literature.user import get_global_user_id

from literature.schemas import ResourceSchemaShow
from literature.schemas import ResourceSchemaPost
from literature.schemas import ResourceSchemaUpdate

from literature.crud import resource_crud

from literature.routers.authentication import auth


router = APIRouter(
    prefix="/resource",
    tags=['Resource']
)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             dependencies=[Depends(auth.implicit_scheme)],
             response_model=str)
def create(request: ResourceSchemaPost,
           user: Auth0User = Security(auth.get_user)):
    set_global_user_id(user.id)
    return resource_crud.create(request)


@router.delete('/{curie}',
               dependencies=[Depends(auth.implicit_scheme)],
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(curie: str,
            user: Auth0User = Security(auth.get_user)):
    resource_crud.destroy(curie)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{curie}',
            status_code=status.HTTP_202_ACCEPTED,
            dependencies=[Depends(auth.implicit_scheme)],
            response_model=str)
def update(curie: str,
           request: ResourceSchemaUpdate,
           user: Auth0User = Security(auth.get_user)):
    return resource_crud.update(curie, request)


@router.get('/',
            response_model=List[str])
def all():
    return resource_crud.get_all()


@router.get('/{curie}',
            status_code=200,
            response_model=ResourceSchemaShow)
def show(curie: str):
    return resource_crud.show(curie)


@router.get('/{curie}/versions',
            status_code=200)
def show(curie: str):
    return resource_crud.show_changesets(curie)
