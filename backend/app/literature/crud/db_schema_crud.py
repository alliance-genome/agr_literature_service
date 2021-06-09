import io

from fastapi import HTTPException
from fastapi import status

from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy.orm import class_mapper
from sqlalchemy import MetaData

from literature.database.config import SQLALCHEMY_DATABASE_URL


def download_image():
    return io.BytesIO(create_schema_graph(metadata=MetaData(SQLALCHEMY_DATABASE_URL)).create_png())

