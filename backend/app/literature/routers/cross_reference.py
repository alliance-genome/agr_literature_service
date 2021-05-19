from typing import List

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User

from literature.schemas import CrossReferenceSchema
from literature.schemas import CrossReferenceSchemaUpdate
from literature.schemas import CrossReferenceSchemaRelated

from literature.crud import cross_reference
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/cross-reference",
    tags=['Cross Reference']
)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=CrossReferenceSchema,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: CrossReferenceSchema,
           user: Auth0User = Security(auth.get_user)):
    return cross_reference.create(request)


@router.delete('/{curie}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(curie: str,
            user: Auth0User = Security(auth.get_user)):
    cross_reference.destroy(curie)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{curie}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=CrossReferenceSchema,
            dependencies=[Depends(auth.implicit_scheme)])
def update(curie: str,
           request: CrossReferenceSchemaUpdate,
           user: Auth0User = Security(auth.get_user)):
    return cross_reference.update(curie, request)


@router.get('/{curie}',
            response_model=CrossReferenceSchema,
            status_code=200)
def show(curie: str):
    return cross_reference.show(curie)


@router.get('/{curie}/versions',
            status_code=200)
def show(curie):
    return cross_reference.show_changesets(curie)
