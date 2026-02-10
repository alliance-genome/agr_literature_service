from typing import Any, Dict, Optional

from fastapi import APIRouter, Security
from fastapi.responses import StreamingResponse

from agr_literature_service.api import database
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.config import config
from agr_literature_service.api.crud import db_schema_crud

router = APIRouter(
    prefix="/database",
    tags=['Database']
)

get_db = database.get_db


@router.get('/schema/download',
            status_code=200,
            response_class=StreamingResponse)
async def download(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    return StreamingResponse(db_schema_crud.download_image(),
                             media_type="image/png")


@router.get('/configuration',
            status_code=200,
            )
def show_config(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    postgres_config = {'host': config.PSQL_HOST,
                       'port': config.PSQL_PORT,
                       'database_name': config.PSQL_DATABASE}
    return {'postgres': postgres_config}
