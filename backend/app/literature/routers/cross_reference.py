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

from literature.schemas import CrossReferenceSchema
from literature.schemas import CrossReferenceSchemaUpdate
from literature.schemas import CrossReferenceSchemaRelated

from literature.crud import cross_reference_crud
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/cross-reference",
    tags=['Cross Reference']
)


get_db = database.get_db

@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: CrossReferenceSchema,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(user.id)
    return cross_reference_crud.create(db, request)


@router.delete('/{curie}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(curie: str,
            user: Auth0User = Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(user.id)
    cross_reference_crud.destroy(db, curie)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{curie}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=str,
            dependencies=[Depends(auth.implicit_scheme)])
def update(curie: str,
           request: CrossReferenceSchemaUpdate,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(user.id)
    return cross_reference_crud.update(db, curie, request)


@router.get('/{curie}',
            response_model=CrossReferenceSchema,
            status_code=200)
def show(curie: str,
         db: Session = Depends(get_db)):
    return cross_reference_crud.show(db, curie)


@router.get('/{curie}/versions',
            status_code=200)
def show(curie: str,
         db: Session = Depends(get_db)):
    return cross_reference_crud.show_changesets(db, curie)
