from datetime import datetime
import pytz

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import DateTime
from sqlalchemy import ARRAY

from sqlalchemy.orm import relationship

from literature.database.base import Base


class Resource(Base):
    __versioned__ = {}
    __tablename__ = 'resources'

    resource_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    curie = Column(
        String(),
        unique=True,
        index=True,
        nullable=False
    )

    cross_references = relationship(
        'CrossReference',
        lazy='joined',
        back_populates='resource',
        cascade="all, delete, delete-orphan"
    )

    references = relationship(
        "Reference",
        back_populates="resource"
    )

    title = Column(
        String(),
        nullable=True
    )

    titleSynonyms = Column(
        ARRAY(String()),
        unique=False,
        nullable=True
    )

    isoAbbreviation = Column(
        String(),
        unique=True,
        nullable=True
    )

    medlineAbbreviation = Column(
        String(),
        unique=False,
        nullable=True
    )

    copyrightDate = Column(
        DateTime
    )

    publisher = Column(
        String(),
        unique=False,
        nullable=True
    )

    printISSN = Column(
        String(),
        unique=False,
        nullable=True
    )

    onlineISSN = Column(
        String(),
        unique=False,
        nullable=True
    )

    authors = relationship(
        'Author',
        lazy='joined',
        back_populates='resource',
        cascade="all, delete, delete-orphan"
    )

    editors = relationship(
        'Editor',
        lazy='joined',
        back_populates='resource',
        cascade="all, delete, delete-orphan"
    )

    volumes = Column(
        ARRAY(String()),
        unique=False,
        nullable=True
    )

    abbreviationSynonyms = Column(
        ARRAY(String()),
        nullable=True
    )

    pages = Column(
        String(),
        unique=False,
        nullable=True
    )

    abstract = Column(
        String(),
        unique=False,
        nullable=True
    )

    summary = Column(
        String(),
        unique=False,
        nullable=True
    )

    dateUpdated = Column(
        DateTime,
        nullable=True,
    )

    dateCreated = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone('UTC'))
    )

