from datetime import datetime

from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
#from sqlalchemy_continuum import make_versioned

from literature.database.main import Base

if TYPE_CHECKING:
    from .user import User  # noqa: F401

#from references.schemas.allianceCategory import AllianceCategory

from enum import Enum

class User(Base):
    __tablename__ = 'users'
#    __versioned__ = {}

    id = Column(
        Integer,
        primary_key=True,
        index=True
    )
