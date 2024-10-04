"""
mod_reference_type_model.py
===========================
"""


from typing import Dict

from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class ReferencetypeModel(Base):
    __tablename__ = "referencetype"
    __bind_key__ = 'lit'
    __table_args__ = {"schema": "lit"}
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


class ModReferencetypeAssociationModel(Base):
    __tablename__ = "mod_referencetype"
    __bind_key__ = 'lit'
    __table_args__ = {"schema": "lit"}
    __versioned__: Dict = {}
    __table_args__ = (UniqueConstraint('mod_id', 'referencetype_id', name='uniq_mrt_new'),)

    mod_referencetype_id = Column(
        Integer,
        autoincrement=True,
        primary_key=True
    )

    mod_id = Column(
        ForeignKey("lit.mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    mod = relationship("ModModel")

    referencetype_id = Column(
        Integer,
        ForeignKey("lit.referencetype.referencetype_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    referencetype = relationship("ReferencetypeModel", foreign_keys="ModReferencetypeAssociationModel.referencetype_id")

    display_order = Column(
        Integer,
        nullable=False
    )


class ReferenceModReferencetypeAssociationModel(Base, AuditedModel):
    __tablename__ = "reference_mod_referencetype"
    __bind_key__ = 'lit'
    __table_args__ = {"schema": "lit"}
    __versioned__: Dict = {}
    __table_args__ = (UniqueConstraint('reference_id', 'mod_referencetype_id', name='uniq_rmrt'),)

    reference_mod_referencetype_id = Column(
        Integer,
        autoincrement=True,
        primary_key=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("lit.reference.reference_id"),
        index=True,
    )

    mod_referencetype_id = Column(
        Integer,
        ForeignKey("mod_referencetype.mod_referencetype_id"), index=True,
    )

    mod_referencetype = relationship("ModReferencetypeAssociationModel",
                                     foreign_keys="ReferenceModReferencetypeAssociationModel.mod_referencetype_id",
                                     primaryjoin="ReferenceModReferencetypeAssociationModel.mod_referencetype_id == ModReferencetypeAssociationModel.mod_referencetype_id")
