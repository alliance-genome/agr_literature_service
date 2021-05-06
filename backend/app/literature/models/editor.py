from datetime import datetime
import pytz

from typing import TYPE_CHECKING

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy import DateTime
from sqlalchemy import ARRAY

from sqlalchemy.orm import relationship

from literature.database.main import Base

if TYPE_CHECKING:
    from .user import User  # noqa: F401

#from references.schemas.allianceCategory import AllianceCategory

from enum import Enum

class Editor(Base):
    __tablename__ = 'editors'
    __versioned__ = {}

    editor_id = Column(
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
        back_populates="editors"
    )

    resource_id = Column(
        Integer,
        ForeignKey('resources.resource_id',
                   ondelete='CASCADE')
    )

    resource = relationship(
        'Resource',
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
