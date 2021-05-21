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

    title_synonyms = Column(
        ARRAY(String()),
        unique=False,
        nullable=True
    )

    iso_abbreviation = Column(
        String(),
        unique=True,
        nullable=True
    )

    medline_abbreviation = Column(
        String(),
        unique=False,
        nullable=True
    )

    copyright_date = Column(
        DateTime
    )

    publisher = Column(
        String(),
        unique=False,
        nullable=True
    )

    print_issn = Column(
        String(),
        unique=False,
        nullable=True
    )

    online_issn = Column(
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

    abbreviation_Synonyms = Column(
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

    date_updated = Column(
        DateTime,
        nullable=True,
    )

    date_created = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone('UTC'))
    )

