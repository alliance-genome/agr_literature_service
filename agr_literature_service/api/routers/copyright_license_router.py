from fastapi import APIRouter, Depends, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import copyright_license_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import CopyrightLicenseSchemaPost
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/copyright_license",
    tags=['Copyright License']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=int)
def create(request: CopyrightLicenseSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session) -> int:
    set_global_user_from_okta(db, user)
    new_id = copyright_license_crud.create(db, request)
    return new_id


@router.get('/all',
            status_code=200)
def show_all(db: Session = db_session):
    return copyright_license_crud.show_all(db)
