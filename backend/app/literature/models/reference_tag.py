from datetime import datetime
import pytz

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy import DateTime
from sqlalchemy import ARRAY
from sqlalchemy import Enum

from sqlalchemy.orm import relationship

from literature.database.base import Base

from literature.schemas import TagName
from literature.schemas import TagSource

class ReferenceTag(Base):
    __tablename__ = 'reference_tags'
    __versioned__ = {}

    reference_tag_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
         Integer,
         ForeignKey('references.reference_id',
                    ondelete='CASCADE')
    )

    reference = relationship(
        'Reference',
        back_populates="tags"
    )


    tag_name = Column(
        Enum(TagName),
        unique=False,
        nullable=False
    )

    tag_source = Column(
        Enum(TagSource),
        unique=False,
        nullable=False
    )
