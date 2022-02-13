"""
resource_model.py
=================
"""


from datetime import datetime
from typing import Dict

import pytz
from sqlalchemy import ARRAY, Column, DateTime, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Boolean

from literature.database.base import Base


class ResourceModel(Base):
    __versioned__: Dict = {}
    __tablename__ = "resources"

    resource_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    curie = Column(
        String(),
        unique=True,
        index=True,
        nullable=False
    )

    cross_references = relationship(
        "CrossReferenceModel",
        lazy="joined",
        back_populates="resource",
        cascade="all, delete, delete-orphan",
        passive_deletes=True
    )

    references = relationship(
        "ReferenceModel",
        back_populates="resource"
    )

    title = Column(
        String(),
        nullable=True
    )

    notes = relationship(
        "NoteModel",
        lazy="joined",
        back_populates="resource",
    )

    title_synonyms = Column(
        ARRAY(String()),
        unique=False,
        nullable=True
    )

    iso_abbreviation = Column(
        String(),
        unique=False,
        nullable=True
    )

    medline_abbreviation = Column(
        String(),
        unique=False,
        nullable=True
    )

    copyright_date = Column(
        DateTime
    )

    publisher = Column(
        String(),
        unique=False,
        nullable=True
    )

    print_issn = Column(
        String(),
        unique=False,
        nullable=True
    )

    online_issn = Column(
        String(),
        unique=False,
        nullable=True
    )

    authors = relationship(
        "AuthorModel",
        lazy="joined",
        back_populates="resource",
        cascade="all, delete, delete-orphan"
    )

    editors = relationship(
        "EditorModel",
        lazy="joined",
        back_populates="resource",
        cascade="all, delete, delete-orphan"
    )

    volumes = Column(
        ARRAY(String()),
        unique=False,
        nullable=True
    )

    abbreviation_synonyms = Column(
        ARRAY(String()),
        nullable=True
    )

    pages = Column(
        String(),
        unique=False,
        nullable=True
    )

    abstract = Column(
        String(),
        unique=False,
        nullable=True
    )

    summary = Column(
        String(),
        unique=False,
        nullable=True
    )

    date_updated = Column(
        DateTime,
        nullable=True,
    )

    date_created = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone("UTC"))
    )

    open_access = Column(
        Boolean,
        nullable=False,
        default=False
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return "Resource id = {} created {} updated {}: curie='{}' title='{}...'".\
            format(self.resource_id, self.date_created, self.date_updated, self.curie, self.title[:10]
