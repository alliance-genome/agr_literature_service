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


class Person(Base):
    __tablename__ = 'people'
    __versioned__ = {}

    person_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    references = relationship(
        'Reference',
        secondary = 'person_reference_link'
    )

    editors = relationship(
        "Editor",
        back_populates="person"
    )

    authors = relationship(
        "Author",
        back_populates="person"
    )

    orcids = relationship(
        'CrossReference',
        lazy='joined',
        secondary = 'person_orcid_cross_reference_link'
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

    affiliation = Column(
        ARRAY(String),
        unique=False,
        nullable=True
    )

    first_name = Column(
        String(),
        unique=False,
        nullable=True
    )

    middle_names = Column(
       ARRAY(String()),
       nullable=True
    )

    last_name = Column(
        String(),
        unique=False,
        nullable=True
    )

    date_updated = Column(
        DateTime,
        nullable=True,
        default=datetime.utcnow
    )

    date_created = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone('UTC'))
    )
