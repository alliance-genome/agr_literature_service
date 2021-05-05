from datetime import datetime
import pytz

from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
#from sqlalchemy_continuum import make_versioned

from literature.database.main import Base

if TYPE_CHECKING:
    from .user import User  # noqa: F401

#from references.schemas.allianceCategory import AllianceCategory

from enum import Enum

class Editor(Base):
    __tablename__ = 'editors'
 #   __versioned__ = {}

    editor_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    resource_id = Column(
        Integer,
        ForeignKey('resources.resource_id')
    )

    resource = relationship(
        'Resource',
        uselist=False,
        back_populates="editors"
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
