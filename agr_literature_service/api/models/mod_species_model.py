"""
mod_species_model.py
==================
"""

from typing import Dict
from sqlalchemy import (Column, Integer)
from agr_literature_service.api.database.base import Base


class ModSpeciesModel(Base):
    __tablename__ = "mod_species"
    __versioned__: Dict = {}

    mod_species_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    mod_id = Column(
        Integer,
        unique=False,
        nullable=False
    )

    # taxon_id
    taxon = Column(
        Integer(),
        unique=True,
        nullable=False
    )
