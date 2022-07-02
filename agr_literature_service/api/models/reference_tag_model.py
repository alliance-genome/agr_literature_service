"""
reference_tag_model.py
==================
"""

from typing import Dict

from sqlalchemy import (Column, ForeignKey, Integer, String)
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning


enable_versioning()


class ReferenceTagModel(Base):
    __tablename__ = "reference_tag"
    __versioned__: Dict = {}

    reference_tag_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id",
                   ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="tag"
    )

    source = Column(
        String(),
        unique=False,
        nullable=False,
        default='Curator'
    )

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    mod = relationship(
        "ModModel",
        lazy="joined"
    )

    # Maybe get list from DB?
    # Else a yaml file.
    # "WB primary"
    tag_type = Column(
        String(),
        unique=False,
        nullable=False
    )

    value = Column(
        String(),
        nullable=False
    )
