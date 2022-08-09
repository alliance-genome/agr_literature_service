"""
reference_comment_and_correction_model.py
==========================================
"""


from typing import Dict

from sqlalchemy import Column, Enum, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.schemas import ReferenceCommentAndCorrectionType
from agr_literature_service.api.database.versioning import enable_versioning


enable_versioning()


class ReferenceCommentAndCorrectionModel(Base):
    __tablename__ = "reference_comments_and_corrections"
    __versioned__: Dict = {}
    __table_args__ = (UniqueConstraint('reference_id_from', 'reference_id_to', 'reference_comment_and_correction_type', name='rccm_uniq'),)

    reference_comment_and_correction_id = Column(
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
        foreign_keys="ReferenceCommentAndCorrectionModel.reference_id_from",
        back_populates="comment_and_corrections_out"
    )

    reference_id_to = Column(
        Integer,
        ForeignKey("reference.reference_id"),
        index=True,
        nullable=False
    )

    reference_to = relationship(
        "ReferenceModel",
        foreign_keys="ReferenceCommentAndCorrectionModel.reference_id_to",
        back_populates="comment_and_corrections_in"
    )

    reference_comment_and_correction_type = Column(
        Enum(ReferenceCommentAndCorrectionType),
        unique=False,
        nullable=False
    )
