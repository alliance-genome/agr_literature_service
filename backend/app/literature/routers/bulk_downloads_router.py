from sqlalchemy.orm import Session

from botocore.client import BaseClient

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Response
from fastapi import Security

from fastapi_okta import OktaUser

from literature import database

from literature.user import set_global_user_id

from literature.routers.authentication import auth
from literature.deps import s3_auth

from literature.crud import reference_crud
from literature.crud import resource_crud

from sqlalchemy import func


router = APIRouter(
    prefix="/bulk_download",
    tags=['Bulk Downloads'])


get_db = database.get_db


@router.get('/references/external_ids/',
            status_code=200)
async def show(db: Session = Depends(get_db),
               user: OktaUser = Security(auth.get_user)):
    return reference_crud.show_all_references_external_ids(db)


@router.get('/resources/external_ids/',
            status_code=200)
async def show(db: Session = Depends(get_db),
               user: OktaUser = Security(auth.get_user)):
    return resource_crud.show_all_resources_external_ids(db)
