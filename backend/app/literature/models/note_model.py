from datetime import datetime
import pytz

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import DateTime

from sqlalchemy.orm import relationship

from literature.database.base import Base


class NoteModel(Base):
    __tablename__ = 'notes'
    __versioned__ = {}

    note_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey('references.reference_id'),
        index=True
    )

    reference = relationship(
        'ReferenceModel',
        back_populates="notes"
    )

    resource_id = Column(
        Integer,
        ForeignKey('resources.resource_id'),
        index=True,
    )

    resource = relationship(
        'ResourceModel',
        back_populates="notes"
    )

    note = Column(
        String(),
        unique=False,
        nullable=False
    )

    name = Column(
        String(),
        unique=False,
        nullable=True
    )

    date_created = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone('UTC'))
    )
