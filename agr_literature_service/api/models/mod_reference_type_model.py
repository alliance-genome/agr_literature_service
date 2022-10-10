"""
mod_reference_type_model.py
===========================
"""


from typing import Dict

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class ReferenceTypeModel(Base):
    __tablename__ = "referencetype"
    __versioned__: Dict = {}

    referencetype_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    label = Column(
        String(),
        unique=True,
        nullable=False
    )


class ModReferenceTypeAssociationModel(Base):
    __tablename__ = "mod_referencetype"
    __versioned__: Dict = {}

    mod_referencetype_id = Column(
        Integer,
        autoincrement=True,
        primary_key=True
    )

    mod_id = Column(
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        nullable=False
    )

    mod = relationship("ModModel")

    referencetype_id = Column(
        ForeignKey("referencetype.referencetype_id", ondelete="CASCADE"),
        nullable=False
    )

    referencetype = relationship("ReferenceTypeModel")

    display_order = Column(
        Integer,
        nullable=False
    )


class ReferenceModReferenceTypeAssociationModel(Base, AuditedModel):
    __tablename__ = "reference_mod_referencetype"
    __versioned__: Dict = {}

    reference_mod_referencetype_id = Column(
        Integer,
        autoincrement=True,
        primary_key=True
    )

    reference_id = Column(
        ForeignKey("reference.reference_id")
    )

    mod_referencetype_id = Column(
        ForeignKey("mod_referencetype.mod_referencetype_id")
    )

    mod_referencetype = relationship("ModReferenceTypeAssociationModel")
