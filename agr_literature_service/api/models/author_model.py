"""
author_model.py
===============
"""

from typing import Dict

from sqlalchemy import (ARRAY, Boolean, CheckConstraint, Column, ForeignKey,
                        Index, Integer, String)
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
        index=True,
        nullable=False
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

    author_order = Column(
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

    person_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="RESTRICT", name="author_person_id_fkey"),
        index=True,
        nullable=True
    )

    person = relationship("PersonModel")

    __table_args__ = (
        CheckConstraint(
            "person_id IS NOT NULL OR author_order IS NOT NULL",
            name="ck_author_person_or_order",
        ),
        CheckConstraint(
            "author_order IS NOT NULL OR ("
            "name IS NULL AND first_name IS NULL AND last_name IS NULL "
            "AND first_initial IS NULL AND orcid IS NULL AND affiliations IS NULL "
            "AND COALESCE(first_author, false) = false "
            "AND COALESCE(corresponding_author, false) = false)",
            name="ck_person_only_link_only",
        ),
        Index("uq_author_ref_person", "reference_id", "person_id", unique=True),
        Index("uq_author_ref_order", "reference_id", "author_order", unique=True),
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return "{} 1st({})".format(self.name, self.first_author)
