import io

from sqlalchemy import MetaData
from sqlalchemy_schemadisplay import create_schema_graph

from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL


def download_image():
    return io.BytesIO(create_schema_graph(metadata=MetaData(SQLALCHEMY_DATABASE_URL)).create_png())
