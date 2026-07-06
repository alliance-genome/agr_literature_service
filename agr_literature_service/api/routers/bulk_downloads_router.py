from fastapi import APIRouter, Depends, Security
from fastapi.responses import StreamingResponse
from typing import Dict, Any, Optional

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import reference_crud, resource_crud
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(
    prefix="/bulk_download",
    tags=['Bulk Downloads'])


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.get('/references/external_ids/',
            status_code=200)
def show(db: Session = db_session,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    return StreamingResponse(reference_crud.stream_all_references_external_ids(db),
                             media_type="application/json")


@router.get('/resources/external_ids/',
            status_code=200)
def show_ex_ids(db: Session = db_session,
                user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    return StreamingResponse(resource_crud.stream_all_resources_external_ids(db),
                             media_type="application/json")
