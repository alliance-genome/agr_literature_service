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
from literature.database.versioning import enable_versioning


enable_versioning()


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
        ForeignKey("references.reference_id", ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
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

    affiliations = Column(
        ARRAY(String),
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

    def __str__(self):
        """
        Overwrite the default output.
        """
        return "{} 1st({})".format(self.name, self.first_author)
