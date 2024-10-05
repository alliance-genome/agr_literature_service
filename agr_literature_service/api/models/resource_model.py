"""
resource_model.py
=================
"""


from typing import Dict, List, Optional

from sqlalchemy import ARRAY, Column, DateTime, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Boolean

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class ResourceModel(Base, AuditedModel):
    __versioned__: Dict = {}
    __tablename__ = "resource"

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

    cross_reference = relationship(
        "CrossReferenceModel",
        lazy="joined",
        back_populates="resource",
        cascade="all, delete, delete-orphan",
        passive_deletes=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="resource"
    )

    title = Column(
        String(),
        nullable=True
    )

    title_synonyms: Column = Column(
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

    editor = relationship(
        "EditorModel",
        lazy="joined",
        back_populates=None,
        cascade="all, delete, delete-orphan"
    )

    volumes: Column = Column(
        ARRAY(String()),
        unique=False,
        nullable=True
    )

    abbreviation_synonyms: Column = Column(
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

    open_access = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default='false'
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return "Resource id = {} created {} updated {}: curie='{}' title='{}...'".\
            format(self.resource_id, self.date_created, self.date_updated, self.curie, self.title[:10])
