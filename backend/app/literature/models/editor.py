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
                    ondelete='CASCADE'),
         index=True
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

    orcid_id = Column(
        String,
        ForeignKey('cross_references.curie')
    )

    orcid_cross_reference = relationship(
        'CrossReference',
        lazy="joined",
        back_populates="editors"
    )

    person_id = Column(
        Integer,
        ForeignKey('people.person_id'),
        nullable=True
    )

    person = relationship(
        'Person',
        back_populates="editors",
        single_parent=True,
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
