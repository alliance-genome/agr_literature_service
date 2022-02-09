from fastapi import APIRouter, Depends, Security
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from literature import database
from literature.crud import reference_crud, resource_crud
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/bulk_download",
    tags=['Bulk Downloads'])


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.get('/references/external_ids/',
            status_code=200)
async def show(db: Session = db_session,
               user: OktaUser = db_user):
    return reference_crud.show_all_references_external_ids(db)


@router.get('/resources/external_ids/',
            status_code=200)
async def show_ex_ids(db: Session = db_session,
                      user: OktaUser = db_user):
    return resource_crud.show_all_resources_external_ids(db)
