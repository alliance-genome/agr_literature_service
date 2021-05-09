from datetime import datetime
import pytz

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy import DateTime
from sqlalchemy import ARRAY

from sqlalchemy.orm import relationship

from literature.database.base import Base


class Author(Base):
    __tablename__ = 'authors'
    __versioned__ = {}

    author_id = Column(
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
        back_populates="authors"
    )

    resource_id = Column(
        Integer,
        ForeignKey('resources.resource_id',
                   ondelete='CASCADE')
    )

    resource = relationship(
        'Resource',
        back_populates="authors"
    )

    order = Column(
        Integer,
        nullable=True
    )

    correspondingAuthor = Column(
        Boolean(),
        nullable=True
    )

    name = Column(
        String(),
        unique=False,
        nullable=True
    )

    firstName = Column(
        String(),
        unique=False,
        nullable=True
    )

    middleNames = Column(
       ARRAY(String()),
       nullable=True
    )

    lastName = Column(
        String(),
        unique=False,
        nullable=True
    )

    dateUpdated = Column(
        DateTime,
        nullable=True,
        default=datetime.utcnow
    )

    dateCreated = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone('UTC'))
    )
