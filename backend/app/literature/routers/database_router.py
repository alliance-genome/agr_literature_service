from typing import List

from sqlalchemy.orm import Session

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User

from fastapi.responses import StreamingResponse

from literature import database
from literature.config import config


from literature.user import set_global_user_id

from literature.crud import db_schema_crud
from literature.routers.authentication import auth


router = APIRouter(
    prefix="/database",
    tags=['Database']
)


get_db = database.get_db

@router.get('/schema/download',
            status_code=200,
            response_class=StreamingResponse)
async def download():
    return StreamingResponse(db_schema_crud.download_image(),
                             media_type="image/png")

@router.get('/configuration',
            status_code=200,
            ) # response_class=str)
def show():
    postgres_config = {'host': config.PSQL_HOST,
                       'port': config.PSQL_PORT,
                       'database_name': config.PSQL_DATABASE}
    return {'postgres': postgres_config}
