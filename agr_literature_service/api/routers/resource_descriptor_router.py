from fastapi import APIRouter, Depends, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from literature import database
from literature.crud import resource_descriptor_crud
from literature.routers.authentication import auth
from literature.user import set_global_user_id

router = APIRouter(
    prefix="/resource_descriptor",
    tags=['Resource Descriptor']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.get('/',
            status_code=200)
def show(db: Session = db_session):
    return resource_descriptor_crud.show(db)


@router.put('/',
            status_code=status.HTTP_202_ACCEPTED)
def update(user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_id(db, user.id)
    return resource_descriptor_crud.update(db)
