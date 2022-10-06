"""
mod_reference_type_model.py
===========================
"""


from typing import Dict

from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint, Table
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


class NewReferenceTypeModel(Base):
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


class NewModReferenceTypeAssociationModel(Base):
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

    referencetype_id = Column(
        ForeignKey("referencetype.referencetype_id", ondelete="CASCADE"),
        nullable=False
    )

    display_order = Column(
        Integer,
        nullable=True
    )


class NewReferenceModReferenceTypeAssociationModel(Base):
    __tablename__ = "reference_mod_referencetype"
    __versioned__: Dict = {}

    reference_id = Column(
        ForeignKey("reference.reference_id"),
        primary_key=True
    )

    mod_referencetype_id = Column(
        ForeignKey("mod_referencetype.mod_referencetype_id"),
        primary_key=True
    )
