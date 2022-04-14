"""
note_model.py
============
"""


from datetime import datetime
from typing import Dict

import pytz
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from literature.database.base import Base
from literature.database.versioning import enable_versioning


enable_versioning()


class NoteModel(Base):
    __tablename__ = "notes"
    __versioned__: Dict = {}

    note_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("references.reference_id"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="notes"
    )

    resource_id = Column(
        Integer,
        ForeignKey("resources.resource_id"),
        index=True,
    )

    resource = relationship(
        "ResourceModel",
        back_populates="notes"
    )

    note = Column(
        String(),
        unique=False,
        nullable=False
    )

    name = Column(
        String(),
        unique=False,
        nullable=True
    )

    date_created = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone("UTC"))
    )
