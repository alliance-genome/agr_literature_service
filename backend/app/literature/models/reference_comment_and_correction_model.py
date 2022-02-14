from typing import Dict

from sqlalchemy import Column, Enum, ForeignKey, Integer
from sqlalchemy.orm import relationship

from literature.database.base import Base
from literature.schemas import ReferenceCommentAndCorrectionType


class ReferenceCommentAndCorrectionModel(Base):
    __tablename__ = 'reference_comments_and_corrections'
    __versioned__: Dict = {}

    reference_comment_and_correction_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id_from = Column(
        Integer,
        ForeignKey('references.reference_id'),
        index=True,
        nullable=False
    )

    reference_from = relationship(
        'ReferenceModel',
        foreign_keys='ReferenceCommentAndCorrectionModel.reference_id_from',
        back_populates="comment_and_corrections_out"
    )

    reference_id_to = Column(
        Integer,
        ForeignKey('references.reference_id'),
        index=True,
        nullable=False
    )

    reference_to = relationship(
        'ReferenceModel',
        foreign_keys='ReferenceCommentAndCorrectionModel.reference_id_to',
        back_populates="comment_and_corrections_in"
    )

    reference_comment_and_correction_type = Column(
        Enum(ReferenceCommentAndCorrectionType),
        unique=False,
        nullable=False
    )
