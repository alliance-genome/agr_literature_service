from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from literature import database
from literature.config import config
from literature.crud import db_schema_crud

router = APIRouter(
    prefix="/database",
    tags=['Database']
)

get_db = database.get_db


@router.get('/schema/download',
            status_code=200,
            response_class=StreamingResponse)
async def show():
    return StreamingResponse(db_schema_crud.download_image(),
                             media_type="image/png")


@router.get('/configuration',
            status_code=200,
            )
def show_config():
    postgres_config = {'host': config.PSQL_HOST,
                       'port': config.PSQL_PORT,
                       'database_name': config.PSQL_DATABASE}
    return {'postgres': postgres_config}
