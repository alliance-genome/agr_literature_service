"""
mod_model.py
===============
"""


from typing import Dict
from sqlalchemy import ARRAY, Column, Integer, String
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class ModModel(Base, AuditedModel):
    __tablename__ = "mod"
    __versioned__: Dict = {}

    mod_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    abbreviation = Column(
        String(10),
        unique=True,
        nullable=False
    )

    short_name = Column(
        String(10),
        unique=True,
        nullable=False
    )

    full_name = Column(
        String(100),
        unique=True,
        nullable=False
    )

    taxon_ids: Column = Column(
        ARRAY(String()),
        unique=False,
        nullable=True
    )

    referencetypes = relationship(
        "ModReferencetypeAssociationModel",
        back_populates="mod"
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return f"{self.abbreviation} {self.full_name} {self.taxon_ids}"
