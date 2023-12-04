"""
reference_relation_model.py
==========================================
"""


from typing import Dict

from sqlalchemy import Column, Enum, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.schemas import ReferenceRelationType
from agr_literature_service.api.database.versioning import enable_versioning


enable_versioning()


class ReferenceRelationModel(Base):
    __tablename__ = "reference_relation"
    __versioned__: Dict = {}
    __table_args__ = (UniqueConstraint('reference_id_from', 'reference_id_to', 'reference_relation_type', name='rccm_uniq'),)

    reference_relation_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id_from = Column(
        Integer,
        ForeignKey("reference.reference_id"),
        index=True,
        nullable=False
    )

    reference_from = relationship(
        "ReferenceModel",
        foreign_keys="ReferenceRelationModel.reference_id_from",
        back_populates="reference_relation_out"
    )

    reference_id_to = Column(
        Integer,
        ForeignKey("reference.reference_id"),
        index=True,
        nullable=False
    )

    reference_to = relationship(
        "ReferenceModel",
        foreign_keys="ReferenceRelationModel.reference_id_to",
        back_populates="reference_relation_in"
    )

    reference_relations_type = Column(
        Enum(ReferenceRelationType),
        unique=False,
        nullable=False
    )
