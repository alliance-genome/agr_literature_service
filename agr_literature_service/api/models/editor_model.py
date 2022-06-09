"""
editor_model.py
===============
"""


from datetime import datetime
from typing import Dict

import pytz
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from literature.database.base import Base
from literature.database.versioning import enable_versioning


enable_versioning()


class EditorModel(Base):
    __tablename__ = "editor"
    __versioned__: Dict = {}

    editor_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    resource_id = Column(
        Integer,
        ForeignKey("resource.resource_id",
                   ondelete="CASCADE"),
        index=True
    )

    resource = relationship(
        "ResourceModel",
        back_populates="editor"
    )

    orcid = Column(
        String,
        ForeignKey("cross_reference.curie")
    )

    orcid_cross_reference = relationship(
        "CrossReferenceModel",
        lazy="joined",
        back_populates="editor"
    )

    order = Column(
        Integer,
        nullable=True
    )

    name = Column(
        String(),
        unique=False,
        nullable=True
    )

    first_name = Column(
        String(),
        unique=False,
        nullable=True
    )

    last_name = Column(
        String(),
        unique=False,
        nullable=True
    )

    date_updated = Column(
        DateTime,
        nullable=True,
        default=datetime.utcnow
    )

    date_created = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone("UTC"))
    )
