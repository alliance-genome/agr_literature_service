"""
mod_model.py
===============
"""


from typing import Dict

from sqlalchemy import Column, Integer, String

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel


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
