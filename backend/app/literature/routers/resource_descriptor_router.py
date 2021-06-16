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

#from literature.schemas import ResourceDesciptorSchema

from literature.crud import resource_descriptor_crud
from literature.routers.authentication import auth


router = APIRouter(
    prefix="/resource-descriptor",
    tags=['ResourceDescriptor']
)


get_db = database.get_db


@router.get('/',
            status_code=200)
def show(db: Session = Depends(get_db)):
    return resource_descriptor_crud.show(db)


@router.put('/',
            status_code=status.HTTP_202_ACCEPTED,
            dependencies=[Depends(auth.implicit_scheme)])
def update(user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return resource_descriptor_crud.update(db)
