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

from literature.schemas import CommentReferenceType


class CommentReferenceModel(Base):
    __tablename__ = 'comment_references'
    __versioned__ = {}

    comment_reference_id = Column(
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
        foreign_keys='CommentReferenceModel.reference_from_id',
        back_populates="comment_references_out"
    )

    reference_to_id = Column(
        Integer,
        ForeignKey('references.reference_id'),
        index=True,
        nullable=False
    )

    reference_to = relationship(
        'ReferenceModel',
        foreign_keys='CommentReferenceModel.reference_to_id',
        back_populates="comment_references_in"
    )

    comment_type = Column(
         Enum(CommentReferenceType),
         unique=False,
         nullable=False
    )
