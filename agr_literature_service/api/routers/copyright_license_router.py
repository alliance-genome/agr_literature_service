from fastapi import APIRouter, Depends, Security, status
from typing import Dict, Any, Optional

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import copyright_license_crud
from agr_literature_service.api.schemas import CopyrightLicenseSchemaPost
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(
    prefix="/copyright_license",
    tags=['Copyright License']
)


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=int)
def create(request: CopyrightLicenseSchemaPost,
           user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
           db: Session = db_session) -> int:
    set_global_user_from_cognito(db, user)
    new_id = copyright_license_crud.create(db, request)
    return new_id


@router.get('/all',
            status_code=200)
def show_all(db: Session = db_session,
             user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    set_global_user_from_cognito(db, user)
    return copyright_license_crud.show_all(db)
