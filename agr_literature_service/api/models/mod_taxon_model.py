"""
mod_taxon_model.py
==================
"""
# from datetime import datetime
# import pytz

from typing import Dict
# from sqlalchemy import (Column, Integer, String, ForeignKey, DateTime)
from sqlalchemy import (Column, Integer, ForeignKey)
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel


class ModTaxonModel(AuditedModel, Base):
    __tablename__ = "mod_taxon"
    __versioned__: Dict = {}

    mod_taxon_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id"),
        unique=False,
        nullable=False
    )

    # taxon_id
    taxon = Column(
        Integer(),
        unique=True,
        nullable=False
    )
