import pytest
from literature.crud.reference_comment_and_correction_crud import create, show, patch, destroy, show_changesets
from sqlalchemy import create_engine
from sqlalchemy import MetaData

from literature.models import (
    Base, ReferenceCommentAndCorrectionModel
)
from literature.schemas import ReferenceCommentAndCorrectionSchemaPost
from literature.database.config import SQLALCHEMY_DATABASE_URL
from sqlalchemy.orm import sessionmaker
from fastapi import HTTPException
metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


def test_get_bad_rcc():
    with pytest.raises(HTTPException):
        show(db, 99999)