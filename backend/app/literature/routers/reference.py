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
from literature import database
from literature.crud import reference
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/reference",
    tags=['Reference']
)

get_db = database.get_db

@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=schemas.ReferenceSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: schemas.ReferenceSchemaPost,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    return reference.create(request, db)


@router.delete('/{curie}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(curie: str,
            db: Session = Depends(get_db),
            user: Auth0User = Security(auth.get_user)):
    reference.destroy(curie, db)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{curie}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=schemas.ReferenceSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)])
def update(curie: str,
           request: schemas.ReferenceSchemaUpdate,
           db: Session = Depends(get_db),
           user: Auth0User = Security(auth.get_user)):
    return reference.update(curie, request, db)


@router.get('/',
            response_model=List[schemas.ReferenceSchemaShow])
def all(db: Session = Depends(get_db)):
    return reference.get_all(db)


@router.get('/{curie}',
            status_code=200,
            response_model=schemas.ReferenceSchemaShow)
def show(curie: str,
         db: Session = Depends(get_db)):
    return reference.show(curie, db)


@router.get('/{curie}/versions',
            status_code=200)
def show(curie: str,
         db: Session = Depends(get_db)):
    return reference.show_changesets(curie, db)
