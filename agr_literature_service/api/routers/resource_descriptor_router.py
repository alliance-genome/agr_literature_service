from fastapi import APIRouter, Depends, Security, status
from typing import Dict, Any, Optional

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import resource_descriptor_crud
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(
    prefix="/resource_descriptor",
    tags=['Resource Descriptor']
)


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.get('/',
            status_code=200)
def show(db: Session = db_session,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    set_global_user_from_cognito(db, user)
    return resource_descriptor_crud.show(db)


@router.put('/',
            status_code=status.HTTP_202_ACCEPTED)
def update(user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
           db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return resource_descriptor_crud.update(db)
