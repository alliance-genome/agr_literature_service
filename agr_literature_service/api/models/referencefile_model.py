"""
referencefile_model.py
========================
"""


from typing import Dict

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Index
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.database.versioning import enable_versioning


enable_versioning()


class ReferencefileModel(Base, AuditedModel):
    __tablename__ = "referencefile"
    __versioned__: Dict = {}
    __table_args__ = (
        Index('idx_md5sum', 'md5sum', unique=False),
        Index('idx_reference_id_display_name', 'reference_id', 'display_name', 'file_extension', unique=True),
        Index('idx_md5sum_reference_id', 'md5sum', 'reference_id', unique=True),
    )

    referencefile_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    display_name = Column(
        String(),
        nullable=False,
        index=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="referencefiles"
    )

    file_class = Column(
        String(),
        nullable=False,
        index=True
    )

    file_publication_status = Column(
        String(),
        nullable=False,
        index=True
    )

    file_extension = Column(
        String(),
        nullable=False,
        index=True
    )

    pdf_type = Column(
        String(),
        nullable=True,
        index=True
    )

    md5sum = Column(
        String(),
        nullable=False,
        index=True
    )

    is_annotation = Column(
        Boolean,
        unique=False,
        default=False
    )

    referencefile_mods = relationship(
        "ReferencefileModAssociationModel",
        back_populates="referencefile",
        cascade="all, delete, delete-orphan"
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return f"Referencefile: reference_id={self.reference_id} display_name={self.display_name} " \
               f"file_class={self.file_class} md5sum={self.md5sum}"


class ReferencefileModAssociationModel(Base, AuditedModel):
    __tablename__ = "referencefile_mod"
    __versioned__: Dict = {}

    referencefile_mod_id = Column(
        Integer,
        autoincrement=True,
        primary_key=True
    )

    mod_id: Column = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=True
    )

    mod = relationship("ModModel")

    referencefile_id: Column = Column(
        Integer,
        ForeignKey("referencefile.referencefile_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    referencefile = relationship("ReferencefileModel")

    __table_args__ = (
        Index('idx_referencefile_mod_not_null',
              'referencefile_id', 'mod_id',
              unique=True,
              postgresql_where=(mod_id.isnot(None))),
        Index('idx_referencefile_mod_null',
              'referencefile_id',
              unique=True,
              postgresql_where=(mod_id.is_(None))),
    )
