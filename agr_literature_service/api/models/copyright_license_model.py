from typing import Dict

from sqlalchemy import Column, Integer, String, TEXT
from sqlalchemy.sql.sqltypes import Boolean
# from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning

enable_versioning()


class CopyrightLicenseModel(Base):
    __tablename__ = "copyright_license"
    __bind_key__ = 'lit'
    __table_args__ = {"schema": "lit"}
    __versioned__: Dict = {'schema': 'lit'}

    copyright_license_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    name = Column(
        String(100),
        unique=True,
        nullable=False
    )

    url = Column(
        String(255)
    )

    description = Column(
        TEXT()
    )

    open_access = Column(
        Boolean,
        nullable=True,
        default=True
    )
