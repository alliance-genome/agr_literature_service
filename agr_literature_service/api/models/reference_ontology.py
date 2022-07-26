"""
reference_ontology_model.py
==================
"""


from datetime import datetime
from typing import Dict

import pytz
from sqlalchemy import (Column, DateTime, ForeignKey, Integer,
                        String)
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning


enable_versioning()


class ReferenceOntologyModel(Base):
    __tablename__ = "reference_ontology"
    __versioned__: Dict = {}

    reference_ontology_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

# reference id - internal reference id
    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id"),
        index=True,
        nullable=False
    )

    reference = relationship(
        "ReferenceModel",
        foreign_keys="ReferenceOntologyModel.reference_id",
        back_populates="ontology"
    )

# ontology node term-id - string from A api.
    ontology_id = Column(
        String(),
        unique=False,
        nullable=False
    )

# mod - from mod table (null means all).  Curators will be explicit and know that null means all, would never want to be vague and separate vague null from explicit all.
    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id"),
        nullable=True
    )

    mod = relationship(
        "ModModel",
        foreign_keys="ModModel.mod_id",
        back_populates="ontology"
    )

# date created - timestamp
# date updated - timestamp
    date_created = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone("UTC"))
    )

    date_updated = Column(
        DateTime,
        nullable=True,
        default=datetime.utcnow
    )

# created by - id from users table
# updated by - id from users table
    created_by = Column(
        String,
        ForeignKey("users.id"),
        nullable=False
    )

    updated_by = Column(
        String,
        ForeignKey("users.id"),
        nullable=True
    )
