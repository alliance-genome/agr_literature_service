"""
mod_taxon_model.py
==================
"""

from typing import Dict
from sqlalchemy import (Column, Integer, String, ForeignKey)
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class ModTaxonModel(AuditedModel, Base):
    __tablename__ = "mod_taxon"
    __versioned__: Dict = {}

    mod_taxon_id = Column(
        Integer(),
        primary_key=True,
        autoincrement=True
    )

    mod_id = Column(
        Integer(),
        ForeignKey("mod.mod_id"),
        index=True,
        unique=False,
        nullable=False
    )

    # taxon_id i.e. ;'NCBITaxon:559292'
    taxon = Column(
        String(),
        unique=True,
        nullable=False
    )
