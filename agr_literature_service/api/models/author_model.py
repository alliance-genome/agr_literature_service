"""
author_model.py
===============
"""

from typing import Dict

from sqlalchemy import (ARRAY, Boolean, Column, ForeignKey, Integer,
                        String)
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class AuthorModel(Base, AuditedModel):
    __tablename__ = "author"
    __versioned__: Dict = {}

    author_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="author"
    )

    orcid = Column(
        String(),
        index=True,
        nullable=True
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

    corresponding_author: Column = Column(
        Boolean(),
        nullable=True
    )

    name = Column(
        String(),
        unique=False,
        nullable=True
    )

    affiliations: Column = Column(
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

    first_initial = Column(
        String(),
        unique=False,
        nullable=True
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return "{} 1st({})".format(self.name, self.first_author)
