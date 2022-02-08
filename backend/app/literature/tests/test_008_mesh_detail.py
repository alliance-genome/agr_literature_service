import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.mesh_detail_crud import (create, destroy, patch, show,
                                              show_changesets)
from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.models import Base, MeshDetailModel
from literature.schemas import MeshDetailSchemaPost

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


def test_get_bad_mesh_detail():
    with pytest.raises(HTTPException):
        show(db, 99999)
