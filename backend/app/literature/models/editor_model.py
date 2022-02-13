"""
editor_model.py
===============
"""


from datetime import datetime
from typing import Dict

import pytz
from sqlalchemy import ARRAY, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from literature.database.base import Base


class EditorModel(Base):
    __tablename__ = "editors"
    __versioned__: Dict = {}

    editor_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("references.reference_id",
                   ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="editors"
    )

    resource_id = Column(
        Integer,
        ForeignKey("resources.resource_id",
                   ondelete="CASCADE"),
        index=True
    )

    resource = relationship(
        "ResourceModel",
        back_populates="editors"
    )

    orcid = Column(
        String,
        ForeignKey("cross_references.curie")
    )

    orcid_cross_reference = relationship(
        "CrossReferenceModel",
        lazy="joined",
        back_populates="editors"
    )

    person_id = Column(
        Integer,
        ForeignKey("people.person_id"),
        nullable=True
    )

    person = relationship(
        "PersonModel",
        back_populates="editors",
        single_parent=True,
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

    middle_names = Column(
        ARRAY(String()),
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
