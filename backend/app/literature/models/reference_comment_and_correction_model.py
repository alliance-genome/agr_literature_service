from datetime import datetime
import pytz

from sqlalchemy import Enum
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy import DateTime
from sqlalchemy import ARRAY

from sqlalchemy.orm import relationship

from literature.database.base import Base

from literature.schemas import ReferenceCommentAndCorrectionType


class ReferenceCommentAndCorrectionModel(Base):
    __tablename__ = 'reference_comments_and_corrections'
    __versioned__ = {}

    reference_comment_and_correction_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_from_id = Column(
        Integer,
        ForeignKey('references.reference_id'),
        index=True,
        nullable=False
    )

    reference_from = relationship(
        'ReferenceModel',
        foreign_keys='ReferenceCommentAndCorrectionModel.reference_from_id',
        back_populates="comment_and_corrections_out"
    )

    reference_to_id = Column(
        Integer,
        ForeignKey('references.reference_id'),
        index=True,
        nullable=False
    )

    reference_to = relationship(
        'ReferenceModel',
        foreign_keys='ReferenceCommentAndCorrectionModel.reference_to_id',
        back_populates="comment_and_corrections_in"
    )

    reference_comment_and_correction_type = Column(
         Enum(ReferenceCommentAndCorrectionType),
         unique=False,
         nullable=False
    )
