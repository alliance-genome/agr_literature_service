"""
author_model.py
===============
"""

from datetime import datetime
from typing import Dict

import pytz
from sqlalchemy import (ARRAY, Boolean, Column, DateTime, ForeignKey, Integer,
                        String)
from sqlalchemy.orm import relationship

from literature.database.base import Base


class AuthorModel(Base):
    __tablename__ = "authors"
    __versioned__: Dict = {}

    author_id = Column(
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
        back_populates="authors"
    )

    resource_id = Column(
        Integer,
        ForeignKey("resources.resource_id"),
        index=True,
    )

    resource = relationship(
        "ResourceModel",
        back_populates="authors"
    )

    orcid = Column(
        String,
        ForeignKey("cross_references.curie"),
        index=True
    )

    orcid_cross_reference = relationship(
        "CrossReferenceModel",
        lazy="joined",
        back_populates="authors"
    )

    person_id = Column(
        Integer,
        ForeignKey("people.person_id"),
        nullable=True,
        index=True
    )

    person = relationship(
        "PersonModel",
        back_populates="authors",
        single_parent=True,
    )

    first_author = Column(
        Boolean,
        nullable=True,
        unique=False
    )

    order = Column(
        Integer,
        nullable=True
    )

    corresponding_author = Column(
        Boolean(),
        nullable=True
    )

    name = Column(
        String(),
        unique=False,
        nullable=True
    )

    affiliation = Column(
        ARRAY(String),
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

    def __str__(self):
        """
        Overwrite the default output.
        """
        return "{} 1st({})".format(self.name, self.first_author)
