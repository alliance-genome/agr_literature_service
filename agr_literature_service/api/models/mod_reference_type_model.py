"""
mod_reference_type_model.py
===========================
"""


from typing import Dict

from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning


enable_versioning()


class ModReferenceTypeModel(Base):
    __tablename__ = "mod_reference_type"
    __versioned__: Dict = {}
    __table_args__ = (UniqueConstraint('reference_id', 'reference_type', 'source', name='uniq_mrt'),)

    mod_reference_type_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id",
                   ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="mod_reference_type"
    )

    reference_type = Column(
        String(),
        unique=False,
        nullable=False
    )

    source = Column(
        String(),
        unique=False,
        nullable=True
    )
