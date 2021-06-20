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

from literature.schemas import ModReferenceTypeSchemaShow
from literature.schemas import ModReferenceTypeSchemaPost
from literature.schemas import ModReferenceTypeSchemaCreate
from literature.schemas import ModReferenceTypeSchemaUpdate

from literature.crud import mod_reference_type_crud
from literature.routers.authentication import auth


router = APIRouter(
    prefix="/reference/mod_reference_type",
    tags=['Reference']
)


get_db = database.get_db


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=ModReferenceTypeSchemaUpdate,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: ModReferenceTypeSchemaPost,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return mod_reference_type_crud.create(db, request)


@router.delete('/{mod_reference_type_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(mod_reference_type_id: int,
            user: Auth0User = Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    mod_reference_type_crud.destroy(db, mod_reference_type_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{mod_reference_type_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ModReferenceTypeSchemaUpdate,
              dependencies=[Depends(auth.implicit_scheme)])
async def patch(mod_reference_type_id: int,
                request: ModReferenceTypeSchemaUpdate,
                user: Auth0User = Security(auth.get_user),
                db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    patch = request.dict(exclude_unset=True)

    return mod_reference_type_crud.patch(db, mod_reference_type_id, patch)


@router.get('/{mod_reference_type_id}',
            response_model=ModReferenceTypeSchemaUpdate,
            status_code=200)
def show(mod_reference_type_id: int,
         db: Session = Depends(get_db)):
    return mod_reference_type_crud.show(db, mod_reference_type_id)


@router.get('/{mod_reference_type_id}/versions',
            status_code=200)
def show(mod_reference_type_id: int,
         db: Session = Depends(get_db)):
    return mod_reference_type_crud.show_changesets(db, mod_reference_type_id)
