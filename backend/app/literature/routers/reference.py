from typing import List

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import HTTPException
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User

from sqlalchemy.orm import Session

from literature import schemas
from literature.crud import reference
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/reference",
    tags=['Reference']
)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=schemas.ReferenceSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: schemas.ReferenceSchemaPost,
           user: Auth0User = Security(auth.get_user),):
    return reference.create(request)


@router.delete('/{curie}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(curie: str,
            user: Auth0User = Security(auth.get_user)):
    reference.destroy(curie)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{curie}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=schemas.ReferenceSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)])
def update(curie: str,
           request: schemas.ReferenceSchemaUpdate,
           user: Auth0User = Security(auth.get_user)):
    return reference.update(curie, request)


@router.get('/',
            response_model=List[schemas.ReferenceSchemaShow])
def all():
    return reference.get_all()


@router.get('/{curie}',
            status_code=200,
            response_model=schemas.ReferenceSchemaShow)
def show(curie: str):
    return reference.show(curie)


@router.get('/{curie}/versions',
            status_code=200)
def show(curie: str):
    return reference.show_changesets(curie)
