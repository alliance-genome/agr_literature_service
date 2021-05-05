from datetime import datetime
import pytz

from typing import TYPE_CHECKING

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import DateTime
from sqlalchemy import ARRAY

from sqlalchemy.orm import relationship
#from sqlalchemy_continuum import make_versioned

from literature.database import Base

if TYPE_CHECKING:
    from .user import User  # noqa: F401

#from references.schemas.allianceCategory import AllianceCategory

from enum import Enum

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

    order = Column(
        Integer,
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

 #   middleNames = relationship('ResourceMiddleName' , backref='resourceAuthor', lazy=True)
    #crossreferences

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
