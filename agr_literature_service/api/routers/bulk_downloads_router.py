from fastapi import APIRouter, Depends, Security
from typing import Dict, Any

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import reference_crud, resource_crud

from agr_cognito_auth import get_cognito_user_swagger

router = APIRouter(
    prefix="/bulk_download",
    tags=['Bulk Downloads'])


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.get('/references/external_ids/',
            status_code=200)
async def show(db: Session = db_session,
               user: Dict[str, Any] = Security(get_cognito_user_swagger)):
    return reference_crud.show_all_references_external_ids(db)


@router.get('/resources/external_ids/',
            status_code=200)
async def show_ex_ids(db: Session = db_session,
                      user: Dict[str, Any] = Security(get_cognito_user_swagger)):
    return resource_crud.show_all_resources_external_ids(db)
